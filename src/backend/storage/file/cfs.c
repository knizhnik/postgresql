/*-------------------------------------------------------------------------
 *
 * cfs.c
 *	  Compressed file system
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/cfs.c
 *
 * NOTES:
 *
 * This file implemets compression of file pages.
 * Updated compressed pages are always appended to the end of file segment.
 * Garbage collector is used to shrink files when them become tool large.
 * GC is spawned as one or more background workers. Them recursively traverse all tablespace directories, 
 * find out *.map files are if logical size of the file is twice larger than physical size of the file 
 * performs compactification. Locking implemented using atomic operations is used to eliminate race 
 * conditions.
 */

#include "postgres.h"

#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "port/atomics.h"
#include "pgstat.h"
#include "portability/mem.h"
#include "storage/fd.h"
#include "storage/cfs.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/resowner_private.h"
#include "postmaster/bgworker.h"

int cfs_gc_workers;
int cfs_gc_threshold;
int cfs_gc_period;
int cfs_gc_delay;


typedef struct
{
	pg_atomic_flag gc_started;
	int            n_workers;
	int            max_iterations;
} CfsState;


static bool cfs_read_file(int fd, void* data, uint32 size);
static bool cfs_write_file(int fd, void const* data, uint32 size);
static void cfs_start_background_gc(void);

static CfsState* cfs_state;
static bool      cfs_stop;


#if CFS_COMPRESSOR == SNAPPY_COMPRESSOR

#include <snappy-c.h>

size_t cfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return snappy_compress(src, src_size, dst, &dst_size) == SNAPPY_OK ? dst_size : 0;
}

size_t cfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return snappy_uncompress(src, src_size, dst, &dst_size) == SNAPPY_OK ? dst_size : 0;
}

char const* cfs_algorithm()
{
	return "snappy";
}

#elif CFS_COMPRESSOR == LCFSE_COMPRESSOR

#include <lcfse.h>

size_t cfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	char* scratch_buf = palloc(lcfse_encode_scratch_size());
    size_t rc = lcfse_encode_buffer(dst, dst_size, src, src_size, scratch_buf);
	pfree(scratch_buf);
	return rc;
}

size_t cfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	char* scratch_buf = palloc(lcfse_encode_scratch_size());
    size_t rc = lcfse_decode_buffer(dst, dst_size, src, src_size, scratch_buf);
	pfree(scratch_buf);
	return rc;
}

char const* cfs_algorithm()
{
	return "lcfse";
}

#elif CFS_COMPRESSOR == LZ4_COMPRESSOR

#include <lz4.h>

size_t cfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return LZ4_compress(src, dst, src_size);
}

size_t cfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return LZ4_decompress_safe(src, dst, src_size, dst_size);
}

char const* cfs_algorithm()
{
	return "lz4";
}

#elif CFS_COMPRESSOR == ZLIB_COMPRESSOR

#include <zlib.h>

size_t cfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    uLongf compressed_size = dst_size;
    int rc = compress2(dst, &compressed_size, src, src_size, Z_BEST_SPEED);
	return rc == Z_OK ? compressed_size : rc;
}

size_t cfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    uLongf dest_len = dst_size;
    int rc = uncompress(dst, &dest_len, src, src_size);
	return rc == Z_OK ? dest_len : rc;
}

char const* cfs_algorithm()
{
	return "zlib";
}

#else

#include <common/pg_lzcompress.h>

size_t cfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	return pglz_compress(src, src_size, dst, PGLZ_strategy_always);
}

size_t cfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	return pglz_decompress(src, src_size, dst, dst_size);
}

char const* cfs_algorithm()
{
	return "pglz";
}

#endif



void cfs_initialize()
{
	cfs_state = (CfsState*)ShmemAlloc(sizeof(CfsState));
	pg_atomic_init_flag(&cfs_state->gc_started);
	elog(LOG, "Start CFS version %s compression algorithm %s", 
		 CFS_VERSION, cfs_algorithm());
}

int cfs_msync(FileMap* map)
{
	return msync(map, sizeof(FileMap), MS_SYNC);
}

FileMap* cfs_mmap(int md)
{
	return (FileMap*)mmap(NULL, sizeof(FileMap), PROT_WRITE | PROT_READ, MAP_SHARED, md, 0);
}

int cfs_munmap(FileMap* map)
{
	return munmap(map, sizeof(FileMap));
}

void cfs_lock_file(FileMap* map, char const* file_path)
{
	long delay = CFS_LOCK_MIN_TIMEOUT;
	while (true) { 
		uint64 count = pg_atomic_fetch_add_u32(&map->lock, 1);
		if (count < CFS_GC_LOCK) {
			break;
		} 
		if (InRecovery) { 
			/* Uhhh... looks like last GC was interrupted.
			 * Try to recover file
			 */
			char* map_bck_path = psprintf("%s.map.bck", file_path);
			char* file_bck_path = psprintf("%s.bck", file_path);
			if (access(file_bck_path, R_OK) != 0) {
				/* There is no backup file: new map should be constructed */					
				int md2 = open(map_bck_path, O_RDWR|PG_BINARY, 0);
				if (md2 >= 0) { 
					/* Recover map */
					if (!cfs_read_file(md2, map, sizeof(FileMap))) { 
						elog(LOG, "Failed to read file %s: %m", map_bck_path);
					}
					close(md2);
				} 
			} else { 
				/* Presence of backup file means that we still have unchanged data and map files.
				 * Just remove backup files, grab lock and continue processing
				 */
				unlink(file_bck_path);
				unlink(map_bck_path);
			}
			pfree(file_bck_path);
			pfree(map_bck_path);
			break;
		}
		pg_atomic_fetch_sub_u32(&map->lock, 1);
		pg_usleep(delay);
		if (delay < CFS_LOCK_MAX_TIMEOUT) { 
			delay *= 2;
		}
	}
	if (IsUnderPostmaster && cfs_gc_workers != 0 && pg_atomic_test_set_flag(&cfs_state->gc_started))
	{
		cfs_start_background_gc();
	}
}

/*
 * Protects file from GC
 */
void cfs_unlock_file(FileMap* map)
{
	pg_atomic_fetch_sub_u32(&map->lock, 1);
}

/*
 * Get position for storing updated page
 */
uint32 cfs_alloc_page(FileMap* map, uint32 oldSize, uint32 newSize)
{
	pg_atomic_fetch_add_u32(&map->usedSize, newSize - oldSize);
	return pg_atomic_fetch_add_u32(&map->physSize, newSize);
}

/*
 * Update logical file size
 */
void cfs_extend(FileMap* map, uint32 newSize)
{
	uint32 oldSize = pg_atomic_read_u32(&map->virtSize);
	while (newSize > oldSize && !pg_atomic_compare_exchange_u32(&map->virtSize, &oldSize, newSize));
}

/*
 * Safe read of file
 */
static bool cfs_read_file(int fd, void* data, uint32 size)
{
	uint32 offs = 0;
	do { 
		int rc = (int)read(fd, (char*)data + offs, size - offs);
		if (rc <= 0) { 
			if (errno != EINTR) { 
				return false;
			}
		} else { 
			offs += rc;
		}
	} while (offs < size);
	
	return true;
}

/*
 * Safe write of file
 */
static bool cfs_write_file(int fd, void const* data, uint32 size)
{
	uint32 offs = 0;
	do { 
		int rc = (int)write(fd, (char const*)data + offs, size - offs);
		if (rc <= 0) { 
			if (errno != EINTR) { 
				return false;
			}
		} else { 
			offs += rc;
		}
	} while (offs < size);
	
	return true;
}

/*
 * Sort pages by offset to improve access locality
 */
static int cfs_cmp_page_offs(void const* p1, void const* p2) 
{
	uint32 o1 = CFS_INODE_OFFS(**(inode_t**)p1);
	uint32 o2 = CFS_INODE_OFFS(**(inode_t**)p2);
	return o1 < o2 ? -1 : o1 == o2 ? 0 : 1;
}

/*
 * Perform garbage collection (if required) of file
 * @param map_path path to file map file (*.map). 
 */
static bool cfs_gc_file(char* map_path)
{
	int md = open(map_path, O_RDWR|PG_BINARY, 0);
	FileMap* map;
	uint32 physSize;
	uint32 usedSize;
	uint32 virtSize;
	int suf = strlen(map_path)-4;
	int fd = -1, fd2 = -1, md2 = -1;
	bool succeed = true;

	if (md < 0) { 
		elog(LOG, "Failed to open map file %s: %m", map_path);
		return false;
	}
	map = cfs_mmap(md);
	if (map == MAP_FAILED) {
		elog(LOG, "Failed to map file %s: %m", map_path);
		close(md);
		return false;
	}
	usedSize = pg_atomic_read_u32(&map->usedSize);
	physSize = pg_atomic_read_u32(&map->physSize);
	virtSize = pg_atomic_read_u32(&map->virtSize);
		
	if ((physSize - usedSize)*100 > physSize*cfs_gc_threshold) /* do we need to perform defragmentation? */
	{ 
		long delay = CFS_LOCK_MIN_TIMEOUT;		
		char* file_path = (char*)palloc(suf+1);
		char* map_bck_path = (char*)palloc(suf+10);
		char* file_bck_path = (char*)palloc(suf+5);
		FileMap* newMap = (FileMap*)palloc0(sizeof(FileMap));
		uint32 newSize = 0;
		inode_t** inodes = (inode_t**)palloc(RELSEG_SIZE*sizeof(inode_t*));
		bool remove_backups = true;
		int n_pages = virtSize / BLCKSZ;
		TimestampTz startTime, endTime;
		long secs;
		int usecs;
		int i;
		
		startTime = GetCurrentTimestamp();

		memcpy(file_path, map_path, suf);
		file_path[suf] = '\0';
		strcat(strcpy(map_bck_path, map_path), ".bck");
		strcat(strcpy(file_bck_path, file_path), ".bck");

		while (true) { 
			uint32 access_count = 0;
			if (pg_atomic_compare_exchange_u32(&map->lock, &access_count, CFS_GC_LOCK)) {				
				break;
			}
			if (access_count >= CFS_GC_LOCK) { 
				/* Uhhh... looks like last GC was interrupted.
				 * Try to recover file
				 */
				if (access(file_bck_path, R_OK) != 0) {
					/* There is no backup file: new map should be constructed */					
					md2 = open(map_bck_path, O_RDWR|PG_BINARY, 0);
					if (md2 >= 0) { 
						/* Recover map */
						if (!cfs_read_file(md2, newMap, sizeof(FileMap))) { 
							elog(LOG, "Failed to read file %s: %m", map_bck_path);
							goto Cleanup;
						}
						close(md2);
						md2 = -1;
						newSize = pg_atomic_read_u32(&newMap->usedSize);
						remove_backups = false;
						goto ReplaceMap;
					}
				} else { 
					/* Presence of backup file means that we still have unchanged data and map files.
					 * Just remove backup files, grab lock and continue processing
					 */
					unlink(file_bck_path);
					unlink(map_bck_path);
					break;
				}
			}
			pg_usleep(delay);
			if (delay < CFS_LOCK_MAX_TIMEOUT) { 
				delay *= 2;
			}
		}				 			
		md2 = open(map_bck_path, O_CREAT|O_RDWR|PG_BINARY|O_TRUNC, 0600);
		if (md2 < 0) { 
			goto Cleanup;
		}
		for (i = 0; i < n_pages; i++) { 
			newMap->inodes[i] = map->inodes[i];
		    inodes[i] = &newMap->inodes[i];
		}
		/* sort inodes by offset to improve read locality */
		qsort(inodes, n_pages, sizeof(inode_t*), cfs_cmp_page_offs);
		
		fd = open(file_path, O_RDWR|PG_BINARY, 0);
		if (fd < 0) { 
			goto Cleanup;
		}
		
		fd2 = open(file_bck_path, O_CREAT|O_RDWR|PG_BINARY|O_TRUNC, 0600);
		if (fd2 < 0) { 
			goto Cleanup;
		}
		
		for (i = 0; i < n_pages; i++) { 
			int size = CFS_INODE_SIZE(*inodes[i]);
			if (size != 0) { 
				char block[BLCKSZ];
				off_t rc PG_USED_FOR_ASSERTS_ONLY;
				uint32 offs = CFS_INODE_OFFS(*inodes[i]);
				Assert(size <= BLCKSZ);	
				rc = lseek(fd, offs, SEEK_SET);
				Assert(rc == offs);
				
				if (!cfs_read_file(fd, block, size)) { 
					elog(LOG, "Failed to read file %s: %m", file_path);
					goto Cleanup;
				}
				
				if (!cfs_write_file(fd2, block, size)) { 
					elog(LOG, "Failed to write file %s: %m", file_bck_path);
					goto Cleanup;
				}
				offs = newSize;
				newSize += size;
				*inodes[i] = CFS_INODE(size, offs);
			}
		}
		pg_atomic_write_u32(&map->usedSize, newSize);

		if (close(fd) < 0) { 
			elog(LOG, "Failed to close file %s: %m", file_path);
			goto Cleanup;
		}
		fd = -1;

		/* Persist copy of data file */
		if (pg_fsync(fd2) < 0) { 
			elog(LOG, "Failed to sync file %s: %m", file_bck_path);
			goto Cleanup;
		}
		if (close(fd2) < 0) { 
			elog(LOG, "Failed to close file %s: %m", file_bck_path);
			goto Cleanup;
		}
		fd2 = -1;

		/* Persist copy of map file */
		if (!cfs_write_file(md2, &newMap, sizeof(newMap))) { 
			elog(LOG, "Failed to write file %s: %m", map_bck_path);
			goto Cleanup;
		}
		if (pg_fsync(md2) < 0) { 
			elog(LOG, "Failed to sync file %s: %m", map_bck_path);
			goto Cleanup;
		}
		if (close(md2) < 0) { 
			elog(LOG, "Failed to close file %s: %m", map_bck_path);
			goto Cleanup;
		}
		md2 = -1;

		/* Persist map with CFS_GC_LOCK set: in case of crash we will know that map may be changed by GC */
		if (cfs_msync(map) < 0) {
			elog(LOG, "Failed to sync map %s: %m", map_path);
			goto Cleanup;
		}
		if (pg_fsync(md) < 0) { 
			elog(LOG, "Failed to sync file %s: %m", map_path);
			goto Cleanup;
		}
		
		/* 
		 * Now all information necessary for recovery is stored.
		 * We are ready to replace existed file with defragmented one.
		 * Use rename and rely on file system to provide atomicity of this operation.
		 */
		remove_backups = false;
		if (rename(file_bck_path, file_path) < 0) { 
			elog(LOG, "Failed to rename file %s: %m", file_path);
			goto Cleanup;
		}
	  ReplaceMap:
		/* At this moment defragmented file version is stored. We can perfrom in-place update of map.
		 * If crash happens at this point, map can be recovered from backup file */
		memcpy(map->inodes, newMap->inodes, n_pages * sizeof(inode_t));
		pg_atomic_write_u32(&map->usedSize, newSize);
		pg_atomic_write_u32(&map->physSize, newSize);
		map->generation += 1; /* force all backends to reopen the file */
		
		/* Before removing backup files and releasing locks we need to flush updated map file */
		if (cfs_msync(map) < 0) {
			elog(LOG, "Failed to sync map %s: %m", map_path);
			goto Cleanup;
		}
		if (pg_fsync(md) < 0) { 
			elog(LOG, "Failed to sync file %s: %m", map_path);
		  Cleanup:
			if (fd >= 0) close(fd);
			if (fd2 >= 0) close(fd2);
			if (md2 >= 0) close(md2);
			if (remove_backups) { 
				unlink(file_bck_path);
				unlink(map_bck_path);		
				remove_backups = false;
			}	
			succeed = false;
		} else { 
			remove_backups = true; /* now backups are not need any more */
		}
		pg_atomic_fetch_sub_u32(&map->lock, CFS_GC_LOCK); /* release lock */

		/* remove map backup file */
		if (remove_backups && unlink(map_bck_path)) {
			elog(LOG, "Failed to unlink file %s: %m", map_bck_path);
			succeed = false;
		}
		
		endTime = GetCurrentTimestamp();
		TimestampDifference(startTime, endTime, &secs, &usecs);

		elog(LOG, "%d: defragment file %s: old size %d, new size %d, logical size %d, used %d, compression ratio %f, time %ld usec",
			 MyProcPid, file_path, physSize, newSize, virtSize, usedSize, (double)virtSize/newSize,
			 secs*USECS_PER_SEC + usecs);

		pfree(file_path);
		pfree(file_bck_path);
		pfree(map_bck_path);
		pfree(inodes);
		pfree(newMap);
		
		if (cfs_gc_delay != 0) { 
			int rc = WaitLatch(MyLatch,
							   WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   cfs_gc_delay /* ms */ );
			if (rc & WL_POSTMASTER_DEATH) {
				exit(1);
			}
		}
	} else if (cfs_state->max_iterations == 1) { 
		elog(LOG, "%d: file %.*s: physical size %d, logical size %d, used %d, compression ratio %f",
			 MyProcPid, suf, map_path, physSize, virtSize, usedSize, (double)virtSize/physSize);
	}
	
	if (cfs_munmap(map) < 0) { 
		elog(LOG, "Failed to unmap file %s: %m", map_path);
		succeed = false;
	}
	if (close(md) < 0) { 
		elog(LOG, "Failed to close file %s: %m", map_path);
		succeed = false;
	}
	return succeed;
}

static bool cfs_gc_directory(int worker_id, char const* path)
{
	DIR* dir = AllocateDir(path);
	bool success = true;

	if (dir != NULL) { 
		struct dirent* entry;
		char file_path[MAXPGPATH];
		int len;

		while ((entry = ReadDir(dir, path)) != NULL && !cfs_stop)
		{
			if (strcmp(entry->d_name, ".") == 0 ||
				strcmp(entry->d_name, "..") == 0)
			{
				continue;
			}
			len = snprintf(file_path, sizeof(file_path), "%s/%s", path, entry->d_name);
			if (len > 4 && 
				strcmp(file_path + len - 4, ".map") == 0) 
			{ 
				if (entry->d_ino % cfs_state->n_workers == worker_id && !cfs_gc_file(file_path))
				{ 
					success = false;
					break;
				}
			} else { 
				if (!cfs_gc_directory(worker_id, file_path)) 
				{ 
					success = false;
					break;
				}
			}
		}
		FreeDir(dir);
	}
	return success;
}

static void cfs_cancel(int sig)
{
	cfs_stop = true;
}
	  
static bool cfs_scan_tablespace(int worker_id)
{
	return cfs_gc_directory(worker_id, "pg_tblspc");
}


static void cfs_bgworker_main(Datum arg)
{
	int worker_id = DatumGetInt32(arg);
    sigset_t sset;

	signal(SIGINT, cfs_cancel);
    signal(SIGQUIT, cfs_cancel);
    signal(SIGTERM, cfs_cancel);
    sigfillset(&sset);
    sigprocmask(SIG_UNBLOCK, &sset, NULL);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

	elog(INFO, "Start CFS garbage collector %d", MyProcPid);

	while (cfs_scan_tablespace(worker_id) && !cfs_stop && --cfs_state->max_iterations >= 0) { 
		int rc = WaitLatch(MyLatch,
						   WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   cfs_gc_period /* ms */ );
		if (rc & WL_POSTMASTER_DEATH) {
			exit(1);
		}
	}
}

void cfs_start_background_gc()
{
	int i;
	cfs_state->max_iterations = INT_MAX;
	cfs_state->n_workers = cfs_gc_workers;

	for (i = 0; i < cfs_gc_workers; i++) {
		BackgroundWorker worker;	
		BackgroundWorkerHandle* handle;
		MemSet(&worker, 0, sizeof(worker));
		sprintf(worker.bgw_name, "cfs-worker-%d", i);
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = 1;
		worker.bgw_main = cfs_bgworker_main;
		worker.bgw_main_arg = Int32GetDatum(i);
		if (!RegisterDynamicBackgroundWorker(&worker, &handle)) { 
			break;
		}
	}
	elog(LOG, "Start %d background CFS background workers", i);
}

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cfs_start_gc);
PG_FUNCTION_INFO_V1(cfs_version);

Datum cfs_start_gc(PG_FUNCTION_ARGS)
{
	int i = 0;

	if (cfs_gc_workers == 0 && pg_atomic_test_set_flag(&cfs_state->gc_started)) 
	{
		int j;
		BackgroundWorkerHandle** handles;

		cfs_stop = true; /* do just one iteration */	   

		cfs_state->max_iterations = 1;
		cfs_state->n_workers = PG_GETARG_INT32(0);
		handles = (BackgroundWorkerHandle**)palloc(cfs_state->n_workers*sizeof(BackgroundWorkerHandle*));		

		for (i = 0; i < cfs_state->n_workers; i++) {
			BackgroundWorker worker;
			MemSet(&worker, 0, sizeof(worker));
			sprintf(worker.bgw_name, "cfs-worker-%d", i);
			worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
			worker.bgw_start_time = BgWorkerStart_ConsistentState;
			worker.bgw_restart_time = 1;
			worker.bgw_main = cfs_bgworker_main;
			worker.bgw_main_arg = Int32GetDatum(i);
			if (!RegisterDynamicBackgroundWorker(&worker, &handles[i])) { 
				break;
			}
		}
		for (j = 0; j < i; j++) {
			WaitForBackgroundWorkerShutdown(handles[j]);
		}
		pfree(handles);
		pg_atomic_clear_flag(&cfs_state->gc_started);
	}
	PG_RETURN_INT32(i);
}

Datum cfs_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_CSTRING(psprintf("%s-%s", CFS_VERSION, cfs_algorithm()));
}
