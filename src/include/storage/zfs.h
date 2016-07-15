#ifndef __ZFS_H__
#define __ZFS_H__

#include "port/atomics.h"

#define ZFS_GC_LOCK  0x10000000

#define ZFS_LOCK_MIN_TIMEOUT 100    /* microseconds */
#define ZFS_LOCK_MAX_TIMEOUT 10000  /* microseconds */

#define ZFS_MAX_COMPRESSED_SIZE(size) ((size)*2)
#define ZFS_MIN_COMPRESSED_SIZE(size) ((size)*2/3)

#define LZ_COMPRESSOR     1
#define ZLIB_COMPRESSOR   2
#define LZ4_COMPRESSOR    3
#define SNAPPY_COMPRESSOR 4
#define LZFSE_COMPRESSOR  5

#ifndef ZFS_COMPRESSOR 
#define ZFS_COMPRESSOR ZLIB_COMPRESSOR
#endif

size_t zfs_compress(void* dst, size_t dst_size, void const* src, size_t src_size);
size_t zfs_decompress(void* dst, size_t dst_size, void const* src, size_t src_size);
char const* zfs_algorithm(void);

typedef struct 
{
	uint32 offs;
	uint32 size;
} FileMapEntry;

typedef struct 
{
	pg_atomic_uint32 physSize;
	pg_atomic_uint32 virtSize;
	pg_atomic_uint32 usedSize;
	pg_atomic_uint32 lock;
	uint64           generation;
	FileMapEntry     entries[RELSEG_SIZE];
} FileMap;

void     zfs_lock_file(FileMap* map, char const* path);
void     zfs_unlock_file(FileMap* map);
uint32   zfs_alloc_page(FileMap* map, uint32 oldSize, uint32 newSize);
void     zfs_extend(FileMap* map, uint32 pos);

int      zfs_msync(FileMap* map);
FileMap* zfs_mmap(int md);
int      zfs_munmap(FileMap* map);
void     zfs_initialize(void);

extern int zfs_gc_workers;
extern int zfs_gc_timeout;
extern int zfs_gc_threshold;

#endif


