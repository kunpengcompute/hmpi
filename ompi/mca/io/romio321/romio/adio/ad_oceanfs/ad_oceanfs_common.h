#ifndef AD_OCEANFS_COMMON_H_INCLUDED
#define AD_OCEANFS_COMMON_H_INCLUDED
#include <stdint.h>
#include "ad_oceanfs.h"

typedef struct {
    uint64_t group_id;
} MPI_CONTEXT_T;

typedef struct {
    char *oceanfs_filename;
    MPI_CONTEXT_T *context;
} ADIOI_OCEANFS_fs;

void ADIOI_OCEANFS_Init(int rank, int *error_code);

#endif /* AD_OCEANFS_COMMON_H_INCLUDED */
