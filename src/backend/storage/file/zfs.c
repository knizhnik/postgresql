/*-------------------------------------------------------------------------
 *
 * zfs.c
 *	  Compressed file system
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/zfs.c
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
#include "storage/zfs.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/resowner_private.h"
#include "postmaster/bgworker.h"

int zfs_gc_workers;
int zfs_gc_threshold;
int zfs_gc_timeout;


typedef struct
{
	pg_atomic_flag gc_started;
	int            n_workers;
	int            max_iterations;
} ZfsState;


static bool zfs_read_file(int fd, void* data, uint32 size);
static bool zfs_write_file(int fd, void const* data, uint32 size);
static void zfs_start_background_gc(void);

static ZfsState* zfs_state;
static bool      zfs_stop;


#if ZFS_COMPRESSOR == SNAPPY_COMPRESSOR

#include <snappy-c.h>

size_t zfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return snappy_compress(src, src_size, dst, &dst_size) == SNAPPY_OK ? dst_size : 0;
}

size_t zfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return snappy_uncompress(src, src_size, dst, &dst_size) == SNAPPY_OK ? dst_size : 0;
}

char const* zfs_algorithm()
{
	return "snappy";
}

#elif ZFS_COMPRESSOR == LZFSE_COMPRESSOR

#include <lzfse.h>

size_t zfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	char* scratch_buf = palloc(lzfse_encode_scratch_size());
    size_t rc = lzfse_encode_buffer(dst, dst_size, src, src_size, scratch_buf);
	pfree(scratch_buf);
	return rc;
}

size_t zfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	char* scratch_buf = palloc(lzfse_encode_scratch_size());
    size_t rc = lzfse_decode_buffer(dst, dst_size, src, src_size, scratch_buf);
	pfree(scratch_buf);
	return rc;
}

char const* zfs_algorithm()
{
	return "lzfse";
}

#elif ZFS_COMPRESSOR == LZ4_COMPRESSOR

#include <lz4.h>

size_t zfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return LZ4_compress(src, dst, src_size);
}

size_t zfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    return LZ4_decompress_safe(src, dst, src_size, dst_size);
}

char const* zfs_algorithm()
{
	return "lz4";
}

#elif ZFS_COMPRESSOR == ZLIB_COMPRESSOR

#include <zlib.h>

size_t zfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    uLongf compressed_size = dst_size;
    return compress2(dst, &compressed_size, src, src_size, Z_BEST_SPEED) == Z_OK ? compressed_size : 0;
}

size_t zfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
    uLongf dest_len = dst_size;
    return uncompress(dst, &dest_len, src, src_size) == Z_OK ? dest_len : 0;
}

char const* zfs_algorithm()
{
	return "zlib";
}

#else

#include <common/pg_lzcompress.h>

size_t zfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	return pglz_compress(src, src_size, dst, PGLZ_strategy_always);
}

size_t zfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size)
{
	return pglz_decompress(src, src_size, dst, dst_size);
}

char const* zfs_algorithm()
{
	return "pglz";
}

#endif



void zfs_initialize()
{
	zfs_state = (ZfsState*)ShmemAlloc(sizeof(ZfsState));
	pg_atomic_init_flag(&zfs_state->gc_started);
}

int zfs_msync(FileMap* map)
{
	return msync(map, sizeof(FileMap), MS_SYNC);
}

FileMap* zfs_mmap(int md)
{
	return (FileMap*)mmap(NULL, sizeof(FileMap), PROT_WRITE | PROT_READ, MAP_SHARED, md, 0);
}

int zfs_munmap(FileMap* map)
{
	return munmap(map, sizeof(FileMap));
}

void zfs_lock_file(FileMap* map, char const* file_path)
{
	long delay = ZFS_LOCK_MIN_TIMEOUT;
	while (true) { 
		uint64 count = pg_atomic_fetch_add_u32(&map->lock, 1);
		if (count < ZFS_GC_LOCK) {
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
					if (!zfs_read_file(md2, map, sizeof(FileMap))) { 
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
		if (delay < ZFS_LOCK_MAX_TIMEOUT) { 
			delay *= 2;
		}
	}
	if (IsUnderPostmaster && zfs_gc_workers != 0 && pg_atomic_test_set_flag(&zfs_state->gc_started))
	{
		zfs_start_background_gc();
	}
}

/*
 * Protects file from GC
 */
void zfs_unlock_file(FileMap* map)
{
	pg_atomic_fetch_sub_u32(&map->lock, 1);
}

/*
 * Get position for storing uodated page
 */
uint32 zfs_alloc_page(FileMap* map, uint32 oldSize, uint32 newSize)
{
	pg_atomic_fetch_add_u32(&map->usedSize, newSize - oldSize);
	return pg_atomic_fetch_add_u32(&map->physSize, newSize);
}

/*
 * Update logical file size
 */
void zfs_extend(FileMap* map, uint32 newSize)
{
	uint32 oldSize = pg_atomic_read_u32(&map->virtSize);
	while (newSize > oldSize && !pg_atomic_compare_exchange_u32(&map->virtSize, &oldSize, newSize));
}

/*
 * Safe read of file
 */
static bool zfs_read_file(int fd, void* data, uint32 size)
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
static bool zfs_write_file(int fd, void const* data, uint32 size)
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
static int zfs_cmp_page_offs(void const* p1, void const* p2) 
{
	FileMapEntry* e1 = *(FileMapEntry**)p1;
	FileMapEntry* e2 = *(FileMapEntry**)p2;
	return e1->offs < e2->offs ? -1 : e1->offs == e2->offs ? 0 : 1;
}

/*
 * Perform garbage collection (if required) of file
 * @param map_path path to file map file (*.map). 
 */
static bool zfs_gc_file(char* map_path)
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
	map = zfs_mmap(md);
	if (map == MAP_FAILED) {
		elog(LOG, "Failed to map file %s: %m", map_path);
		close(md);
		return false;
	}
	usedSize = pg_atomic_read_u32(&map->usedSize);
	physSize = pg_atomic_read_u32(&map->physSize);
	virtSize = pg_atomic_read_u32(&map->virtSize);
		
	if ((physSize - usedSize)*100 > physSize*zfs_gc_threshold) /* do we need to perform defragmentation? */
	{ 
		long delay = ZFS_LOCK_MIN_TIMEOUT;		
		char* file_path = (char*)palloc(suf+1);
		char* map_bck_path = (char*)palloc(suf+10);
		char* file_bck_path = (char*)palloc(suf+5);
		FileMap* newMap = (FileMap*)palloc0(sizeof(FileMap));
		uint32 newSize = 0;
		FileMapEntry** entries = (FileMapEntry**)palloc(RELSEG_SIZE*sizeof(FileMapEntry*));
		bool remove_backups = true;
		int n_pages;
		int i;

		memcpy(file_path, map_path, suf);
		file_path[suf] = '\0';
		strcat(strcpy(map_bck_path, map_path), ".bck");
		strcat(strcpy(file_bck_path, file_path), ".bck");

		while (true) { 
			uint32 access_count = 0;
			if (pg_atomic_compare_exchange_u32(&map->lock, &access_count, ZFS_GC_LOCK)) {				
				break;
			}
			if (access_count >= ZFS_GC_LOCK) { 
				/* Uhhh... looks like last GC was interrupted.
				 * Try to recover file
				 */
				if (access(file_bck_path, R_OK) != 0) {
					/* There is no backup file: new map should be constructed */					
					md2 = open(map_bck_path, O_RDWR|PG_BINARY, 0);
					if (md2 >= 0) { 
						/* Recover map */
						if (!zfs_read_file(md2, newMap, sizeof(FileMap))) { 
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
			if (delay < ZFS_LOCK_MAX_TIMEOUT) { 
				delay *= 2;
			}
		}				 			
		md2 = open(map_bck_path, O_CREAT|O_RDWR|PG_BINARY|O_TRUNC, 0600);
		if (md2 < 0) { 
			goto Cleanup;
		}
		n_pages = virtSize / BLCKSZ;
		for (i = 0; i < n_pages; i++) { 
			newMap->entries[i] = map->entries[i];
			entries[i] = &newMap->entries[i];
		}
		/* sort entries by offset to improve read locality */
		qsort(entries, n_pages, sizeof(FileMapEntry*), zfs_cmp_page_offs);
		
		fd = open(file_path, O_RDWR|PG_BINARY, 0);
		if (fd < 0) { 
			goto Cleanup;
		}
		
		fd2 = open(file_bck_path, O_CREAT|O_RDWR|PG_BINARY|O_TRUNC, 0600);
		if (fd2 < 0) { 
			goto Cleanup;
		}
		
		for (i = 0; i < n_pages; i++) { 
			if (entries[i]->size != 0) { 
				char block[BLCKSZ];
				int size = entries[i]->size;
				off_t rc PG_USED_FOR_ASSERTS_ONLY;
				Assert(size <= BLCKSZ);	
				rc = lseek(fd, entries[i]->offs, SEEK_SET);
				Assert(rc == entries[i]->offs);
				
				if (!zfs_read_file(fd, block, size)) { 
					elog(LOG, "Failed to read file %s: %m", file_path);
					goto Cleanup;
				}
				
				if (!zfs_write_file(fd2, block, size)) { 
					elog(LOG, "Failed to write file %s: %m", file_bck_path);
					goto Cleanup;
				}
				entries[i]->offs = newSize;
				newSize += entries[i]->size;
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
		if (!zfs_write_file(md2, &newMap, sizeof(newMap))) { 
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

		/* Persist map with ZFS_GC_LOCK set: in case of crash we will know that map may be changed by GC */
		if (zfs_msync(map) < 0) {
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
		memcpy(map->entries, newMap->entries, n_pages * sizeof(FileMapEntry));
		pg_atomic_write_u32(&map->usedSize, newSize);
		pg_atomic_write_u32(&map->physSize, newSize);
		map->generation += 1; /* force all backends to reopen the file */
		
		/* Before removing backup files and releasing locks we need to flush updated map file */
		if (zfs_msync(map) < 0) {
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
		pg_atomic_fetch_sub_u32(&map->lock, ZFS_GC_LOCK); /* release lock */

		/* remove map backup file */
		if (remove_backups && unlink(map_bck_path)) {
			elog(LOG, "Failed to unlink file %s: %m", map_bck_path);
			succeed = false;
		}
		
		elog(LOG, "%d: defragment file %s: old size %d, new size %d, logical size %d, used %d, compression ratio %f",
			 MyProcPid, file_path, physSize, newSize, virtSize, usedSize, (double)virtSize/newSize);

		pfree(file_path);
		pfree(file_bck_path);
		pfree(map_bck_path);
		pfree(entries);
		pfree(newMap);
	} else if (zfs_state->max_iterations == 1) { 
		elog(LOG, "%d: file %.*s: physical size %d, logical size %d, used %d, compression ratio %f",
			 MyProcPid, suf, map_path, physSize, virtSize, usedSize, (double)virtSize/physSize);
	}
	
	if (zfs_munmap(map) < 0) { 
		elog(LOG, "Failed to unmap file %s: %m", map_path);
		succeed = false;
	}
	if (close(md) < 0) { 
		elog(LOG, "Failed to close file %s: %m", map_path);
		succeed = false;
	}
	return succeed;
}

static bool zfs_gc_directory(int worker_id, char const* path)
{
	DIR* dir = AllocateDir(path);
	bool success = true;

	if (dir != NULL) { 
		struct dirent* entry;
		char file_path[MAXPGPATH];
		int len;

		while ((entry = ReadDir(dir, path)) != NULL && !zfs_stop)
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
				if (entry->d_ino % zfs_state->n_workers == worker_id && !zfs_gc_file(file_path))
				{ 
					success = false;
					break;
				}
			} else { 
				if (!zfs_gc_directory(worker_id, file_path)) 
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

static void zfs_cancel(int sig)
{
	zfs_stop = true;
}
	  
static bool zfs_scan_tablespace(int worker_id)
{
	return zfs_gc_directory(worker_id, "pg_tblspc");
}


static void zfs_bgworker_main(Datum arg)
{
	int worker_id = DatumGetInt32(arg);
    sigset_t sset;

	signal(SIGINT, zfs_cancel);
    signal(SIGQUIT, zfs_cancel);
    signal(SIGTERM, zfs_cancel);
    sigfillset(&sset);
    sigprocmask(SIG_UNBLOCK, &sset, NULL);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

	while (zfs_scan_tablespace(worker_id) && !zfs_stop && --zfs_state->max_iterations >= 0) { 
		pg_usleep(zfs_gc_timeout*USECS_PER_SEC);
	}
}

void zfs_start_background_gc()
{
	int i;
	zfs_state->max_iterations = INT_MAX;
	zfs_state->n_workers = zfs_gc_workers;

	for (i = 0; i < zfs_gc_workers; i++) {
		BackgroundWorker worker;	
		BackgroundWorkerHandle* handle;
		MemSet(&worker, 0, sizeof(worker));
		sprintf(worker.bgw_name, "zfs-worker-%d", i);
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = 1;
		worker.bgw_main = zfs_bgworker_main;
		worker.bgw_main_arg = Int32GetDatum(i);
		if (!RegisterDynamicBackgroundWorker(&worker, &handle)) { 
			break;
		}
	}
	elog(LOG, "Start %d background ZFS background workers", i);
}

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(zfs_start_gc);

Datum zfs_start_gc(PG_FUNCTION_ARGS)
{
	int i = 0;

	if (zfs_gc_workers == 0 && pg_atomic_test_set_flag(&zfs_state->gc_started)) 
	{
		int j;
		BackgroundWorkerHandle** handles;

		zfs_stop = true; /* do just one iteration */	   

		zfs_state->max_iterations = 1;
		zfs_state->n_workers = PG_GETARG_INT32(0);
		handles = (BackgroundWorkerHandle**)palloc(zfs_state->n_workers*sizeof(BackgroundWorkerHandle*));		

		for (i = 0; i < zfs_state->n_workers; i++) {
			BackgroundWorker worker;
			MemSet(&worker, 0, sizeof(worker));
			sprintf(worker.bgw_name, "zfs-worker-%d", i);
			worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
			worker.bgw_start_time = BgWorkerStart_ConsistentState;
			worker.bgw_restart_time = 1;
			worker.bgw_main = zfs_bgworker_main;
			worker.bgw_main_arg = Int32GetDatum(i);
			if (!RegisterDynamicBackgroundWorker(&worker, &handles[i])) { 
				break;
			}
		}
		for (j = 0; j < i; j++) {
			WaitForBackgroundWorkerShutdown(handles[j]);
		}
		pfree(handles);
		pg_atomic_clear_flag(&zfs_state->gc_started);
	}
	PG_RETURN_INT32(i);
}

