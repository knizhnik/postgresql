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

#endif


static bool zfs_stop;

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

void zfs_lock_file(FileMap* map)
{
	long delay = ZFS_LOCK_MIN_TIMEOUT;
	while (true) { 
		uint64 count = pg_atomic_fetch_add_u32(&map->lock, 1);
		if (count < ZFS_GC_LOCK) {
			break;
		} 
		pg_atomic_fetch_sub_u32(&map->lock, 1);
		pg_usleep(delay);
		if (delay < ZFS_LOCK_MAX_TIMEOUT) { 
			delay *= 2;
		}
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
	uint32 realSize;
	uint32 usedSize;
	size_t suf = strlen(map_path)-4;
	int fd = -1, fd2 = -1, md2 = -1;
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
	realSize = pg_atomic_read_u32(&map->physSize);

	if ((realSize - usedSize)*100 > realSize*zfs_gc_threshold) 
	{ 
		long delay = ZFS_LOCK_MIN_TIMEOUT;		
		char* file_path = (char*)palloc(suf+1);
		char* map_bck_path = (char*)palloc(suf+10);
		char* file_bck_path = (char*)palloc(suf+5);
		FileMap newMap;
		uint32 usedSize = 0;

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
						if (!zfs_read_file(md2, &newMap, sizeof(newMap))) { 
							elog(LOG, "Failed to read file %s: %m", map_bck_path);
							goto Cleanup;
						}
						close(md2);
						md2 = -1;
						usedSize = pg_atomic_read_u32(&newMap.usedSize);
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
		if ((realSize - usedSize)*100 > realSize*zfs_gc_threshold) /* recheck condition in critical section */
		{
			FileMapEntry* entries[RELSEG_SIZE];
			int i;
			
			md2 = open(map_bck_path, O_CREAT|O_RDWR|PG_BINARY|O_TRUNC, 0600);
			if (md2 < 0) { 
				goto Cleanup;
			}
			for (i = 0; i < RELSEG_SIZE; i++) { 
				newMap.entries[i] = map->entries[i];
				entries[i] = &newMap.entries[i];
			}
			qsort(entries, RELSEG_SIZE, sizeof(FileMapEntry*), zfs_cmp_page_offs);

			fd = open(file_path, O_RDWR|PG_BINARY, 0);
			if (fd < 0) { 
				goto Cleanup;
			}

			fd2 = open(file_bck_path, O_CREAT|O_RDWR|PG_BINARY|O_TRUNC, 0600);
			if (fd2 < 0) { 
				goto Cleanup;
			}
			
			for (i = 0; i < RELSEG_SIZE; i++) { 
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
					entries[i]->offs = usedSize;
					usedSize += entries[i]->size;
				}
			}
			pg_atomic_write_u32(&map->usedSize, usedSize);

			if (!zfs_write_file(md2, &newMap, sizeof(newMap))) { 
				elog(LOG, "Failed to write file %s: %m", map_bck_path);
				goto Cleanup;
			}
			if (close(fd) < 0) { 
				elog(LOG, "Failed to close file %s: %m", file_path);
				goto Cleanup;
			}
			fd = -1;
			if (pg_fsync(fd2) < 0) { 
				elog(LOG, "Failed to sync file %s: %m", file_bck_path);
				goto Cleanup;
			}
			if (close(fd2) < 0) { 
				elog(LOG, "Failed to close file %s: %m", file_bck_path);
				goto Cleanup;
			}
			fd2 = -1;
			if (pg_fsync(md2) < 0) { 
				elog(LOG, "Failed to sync file %s: %m", map_bck_path);
				goto Cleanup;
			}
			if (close(md2) < 0) { 
				elog(LOG, "Failed to close file %s: %m", map_bck_path);
				goto Cleanup;
			}
			md2 = -1;
			if (rename(file_bck_path, file_path) < 0) { 
				elog(LOG, "Failed to rename file %s: %m", file_path);
				goto Cleanup;
			}
		  ReplaceMap:
			/* At this moment packed file version is stored */
			memcpy(map->entries, newMap.entries, pg_atomic_read_u32(&map->virtSize) / BLCKSZ * sizeof(FileMapEntry));
			pg_atomic_write_u32(&map->usedSize, usedSize);
			pg_atomic_write_u32(&map->physSize, usedSize);
			map->generation += 1;

			if (zfs_msync(map) < 0) {
				elog(LOG, "Failed to sync map %s: %m", map_path);
				zfs_munmap(map);
				close(md);
				return false;
			}
			if (pg_fsync(md) < 0) { 
				elog(LOG, "Failed to sync file %s: %m", map_path);
				zfs_munmap(map);				
				close(md);
				return false;
			}
			pg_atomic_fetch_sub_u32(&map->lock, ZFS_GC_LOCK);

			if (unlink(map_bck_path)) {
				elog(LOG, "Failed to unlink file %s: %m", map_bck_path);
				zfs_munmap(map);				
				close(md);
				return false;
			}
		} else { 
			/* Finally release lock */
			pg_atomic_fetch_sub_u32(&map->lock, ZFS_GC_LOCK);
		}

		pfree(file_path);
		pfree(file_bck_path);
		pfree(map_bck_path);
		
		if (zfs_munmap(map) < 0) { 
			elog(LOG, "Failed to unmap file %s: %m", map_path);
			close(md);
			return false;
		}
		if (close(md) < 0) { 
			elog(LOG, "Failed to close file %s: %m", map_path);
			return false;
		}
		return true;

	  Cleanup:
		pg_atomic_fetch_sub_u32(&map->lock, ZFS_GC_LOCK);
		zfs_munmap(map);
		close(md);
		if (fd >= 0) close(fd);
		if (fd2 >= 0) close(fd2);
		if (md2 >= 0) close(md2);
		unlink(file_bck_path);
		unlink(map_bck_path);
		pfree(file_path);
		pfree(file_bck_path);
		pfree(map_bck_path);
		return false;
	}
	zfs_munmap(map);
	close(md);
	return true;
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
			if (len > 4  && strcmp(file_path + len - 4, ".map") == 0) { 
				if (entry->d_ino % zfs_gc_workers == worker_id && !zfs_gc_file(file_path)) { 
					success = false;
					break;
				}
			} else { 
				if (!zfs_gc_directory(worker_id, file_path)) { 
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

	while (zfs_scan_tablespace(worker_id) && !zfs_stop) { 
		pg_usleep(zfs_gc_timeout*USECS_PER_SEC);
	}
}

void zfs_initialize()
{
	int i;
	for (i = 0; i < zfs_gc_workers; i++) {
		BackgroundWorker worker;	
		BackgroundWorkerHandle* handle;
		sprintf(worker.bgw_name, "zfs-worker-%d", i);
		worker.bgw_flags = 0;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = 1;
		worker.bgw_main = zfs_bgworker_main;
		worker.bgw_main_arg = Int32GetDatum(i);
		if (!RegisterDynamicBackgroundWorker(&worker, &handle)) { 
			break;
		}
	}
	if (i != zfs_gc_workers) {
		elog(LOG, "Start only %d of %d requested ZFS background workers", 
			 i, zfs_gc_workers);
	}
}

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(zfs_start_gc);

Datum zfs_start_gc(PG_FUNCTION_ARGS)
{
	int i = 0;

	if (zfs_gc_workers == 0) {
		int j;
		BackgroundWorkerHandle** handles;

		zfs_gc_workers = PG_GETARG_INT32(0);
		zfs_stop = true; /* do just one iteration */	   

		handles = (BackgroundWorkerHandle**)palloc(zfs_gc_workers*sizeof(BackgroundWorkerHandle*));

		for (i = 0; i < zfs_gc_workers; i++) {
			BackgroundWorker worker;
			sprintf(worker.bgw_name, "zfs-worker-%d", i);
			worker.bgw_flags = 0;
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
		zfs_gc_workers = 0;
	}
	PG_RETURN_INT32(i);
}

