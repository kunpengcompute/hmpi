#include "ad_oceanfs_pub.h"

static int g_gOffsetAggmethod1Size = 3;
static int g_gOffsetAggmethod0Size = 2;

static int g_gIndexEndOffset = 1;
static int g_gIndexCountSize = 2;

void AdPubOffsetAggmethod1(MPI_Comm comm, int nprocs, int myrank, ADIO_Offset startOffset, ADIO_Offset endOffset,
    ADIO_Offset myCountSize, ADIO_Offset *dstStartOffset, ADIO_Offset *dstEndOffset, ADIO_Offset *dstCountSizes)
{
    ADIO_Offset *oceanfs_offsets0 = (ADIO_Offset *)ADIOI_Malloc(g_gOffsetAggmethod1Size * nprocs * sizeof(ADIO_Offset));
    ADIO_Offset *oceanfs_offsets = (ADIO_Offset *)ADIOI_Malloc(g_gOffsetAggmethod1Size * nprocs * sizeof(ADIO_Offset));
    int i;
    for (i = 0; i < g_gOffsetAggmethod1Size * nprocs; i++) {
        oceanfs_offsets0[i] = 0;
    }
    oceanfs_offsets0[myrank * g_gOffsetAggmethod1Size] = startOffset;
    oceanfs_offsets0[myrank * g_gOffsetAggmethod1Size + g_gIndexEndOffset] = endOffset;
    oceanfs_offsets0[myrank * g_gOffsetAggmethod1Size + g_gIndexCountSize] = myCountSize;
    MPI_Allreduce(oceanfs_offsets0, oceanfs_offsets, nprocs * g_gOffsetAggmethod1Size, ADIO_OFFSET, MPI_MAX, comm);
    for (i = 0; i < nprocs; i++) {
        dstStartOffset[i] = oceanfs_offsets[i * g_gOffsetAggmethod1Size];
        dstEndOffset[i] = oceanfs_offsets[i * g_gOffsetAggmethod1Size + g_gIndexEndOffset];
        dstCountSizes[i] = oceanfs_offsets[i * g_gOffsetAggmethod1Size + g_gIndexCountSize];
    }
    ADIOI_Free(oceanfs_offsets0);
    ADIOI_Free(oceanfs_offsets);
}

void AdPubOffsetAggmethod0(MPI_Comm comm, int nprocs, int myrank, ADIO_Offset startOffset, ADIO_Offset endOffset,
    ADIO_Offset *dstStartOffset, ADIO_Offset *dstEndOffset)
{
    ADIO_Offset *oceanfs_offsets0 = (ADIO_Offset *)ADIOI_Malloc(g_gOffsetAggmethod0Size * nprocs * sizeof(ADIO_Offset));
    ADIO_Offset *oceanfs_offsets = (ADIO_Offset *)ADIOI_Malloc(g_gOffsetAggmethod0Size * nprocs * sizeof(ADIO_Offset));
    int i;
    for (i = 0; i < g_gOffsetAggmethod0Size * nprocs; i++) {
        oceanfs_offsets0[i] = 0;
    }
    oceanfs_offsets0[myrank * g_gOffsetAggmethod0Size] = startOffset;
    oceanfs_offsets0[myrank * g_gOffsetAggmethod0Size + g_gIndexEndOffset] = endOffset;
    MPI_Allreduce(oceanfs_offsets0, oceanfs_offsets, nprocs * g_gOffsetAggmethod0Size, ADIO_OFFSET, MPI_MAX, comm);
    for (i = 0; i < nprocs; i++) {
        dstStartOffset[i] = oceanfs_offsets[i * g_gOffsetAggmethod0Size];
        dstEndOffset[i] = oceanfs_offsets[i * g_gOffsetAggmethod0Size + g_gIndexEndOffset];
    }
    ADIOI_Free(oceanfs_offsets0);
    ADIOI_Free(oceanfs_offsets);
}

void CheckOffsetAndLen(void** recvBufForOffsets, void** recvBufForLens)
{
    /* If no recv buffer was allocated in the loop above, make it NULL */
    if (*recvBufForOffsets == (void *)0xFFFFFFFFFFFFFFFF) {
        *recvBufForOffsets = NULL;
    }
    if (*recvBufForLens == (void *)0xFFFFFFFFFFFFFFFF) {
        *recvBufForLens = NULL;
    }
}

void AllocAccess(ADIOI_Access *my_req, int nprocs, int *my_req_per_proc, int* count_procs_ptr)
{
    int i;
    *count_procs_ptr = 0;
    for (i = 0; i < nprocs; i++) {
        if (my_req_per_proc[i]) {
            my_req[i].offsets = (ADIO_Offset *)ADIOI_Malloc(my_req_per_proc[i] * sizeof(ADIO_Offset));
            my_req[i].lens = (ADIO_Offset *)ADIOI_Malloc(my_req_per_proc[i] * sizeof(ADIO_Offset));
            (*count_procs_ptr)++;
        }
        my_req[i].count = 0; /* will be incremented where needed later */
    }
}

void AllocOtherReq(int nprocs, int *others_req_per_proc, void** recvBufForOffsets,
    void** recvBufForLens, ADIOI_Access *others_req, int* others_req_procs_ptr)
{
    int i;
    *others_req_procs_ptr = 0;

    for (i = 0; i < nprocs; i++) {
        if (others_req_per_proc[i]) {
            others_req[i].count = others_req_per_proc[i];

            others_req[i].offsets = (ADIO_Offset *)ADIOI_Malloc(others_req_per_proc[i] * sizeof(ADIO_Offset));
            others_req[i].lens = (ADIO_Offset *)ADIOI_Malloc(others_req_per_proc[i] * sizeof(ADIO_Offset));

            if ((MPIU_Upint)others_req[i].offsets < (MPIU_Upint)*recvBufForOffsets) {
                *recvBufForOffsets = others_req[i].offsets;
            }
            if ((MPIU_Upint)others_req[i].lens < (MPIU_Upint)*recvBufForLens) {
                *recvBufForLens = others_req[i].lens;
            }

            others_req[i].mem_ptrs = (MPI_Aint *)ADIOI_Malloc(others_req_per_proc[i] * sizeof(MPI_Aint));

            (*others_req_procs_ptr)++;
        } else {
            others_req[i].count = 0;
            others_req[i].offsets = NULL;
            others_req[i].lens = NULL;
        }
    }

    CheckOffsetAndLen(recvBufForOffsets, recvBufForLens);
}

void FreeAccess(ADIOI_Access *acc, int nprocs)
{
    int i;
    for (i = 0; i < nprocs; i++) {
        if (acc[i].count) {
            ADIOI_Free(acc[i].offsets);
            ADIOI_Free(acc[i].lens);
        }
    }
    ADIOI_Free(acc);
}

void FreeAccessAll(ADIOI_Access *acc, int nprocs)
{
    int i;
    for (i = 0; i < nprocs; i++) {
        if (acc[i].count) {
            ADIOI_Free(acc[i].offsets);
            ADIOI_Free(acc[i].lens);
            ADIOI_Free(acc[i].mem_ptrs);
        }
    }
    ADIOI_Free(acc);
}

int CalcCount(int* array, int nprocs)
{
    int i;
    int cnt = 0;
    for (i = 0; i < nprocs; i++) {
        if (array[i]) {
            cnt++;
        }
    }

    return cnt;
}

void CalcLoc(ADIOI_Access *others_req, int nprocs, ADIO_Offset* st_loc, ADIO_Offset* end_loc)
{
    int i, j;
    for (i = 0; i < nprocs; i++) {
        if (others_req[i].count) {
            *st_loc = others_req[i].offsets[0];
            *end_loc = others_req[i].offsets[0];
            break;
        }
    }

    for (i = 0; i < nprocs; i++) {
        for (j = 0; j < others_req[i].count; j++) {
            *st_loc = ADIOI_MIN(*st_loc, others_req[i].offsets[j]);
            *end_loc = ADIOI_MAX(*end_loc, (others_req[i].offsets[j] + others_req[i].lens[j] - 1));
        }
    }
}

void SetNtimes(ADIOI_Iread_and_exch_vars *vars, ADIO_Offset st_loc, ADIO_Offset end_loc, int coll_bufsize)
{
    /* calculate ntimes, the number of times this process must perform I/O
     * operations in order to complete all the requests it has received.
     * the need for multiple I/O operations comes from the restriction that
     * we only use coll_bufsize bytes of memory for internal buffering.
     */
    if ((st_loc == -1 && end_loc == -1) || coll_bufsize == 0) {
        /* this process does no I/O. */
        vars->ntimes = 0;
    } else {
        /* ntimes=ceiling_div(end_loc - st_loc + 1, coll_bufsize) */
        vars->ntimes = (int)((end_loc - st_loc + coll_bufsize) / coll_bufsize);
    }
}

void SetNtimesLocal(int *ntimes, ADIO_Offset st_loc, ADIO_Offset end_loc, int coll_bufsize)
{
    if ((st_loc == -1 && end_loc == -1) || coll_bufsize == 0) {
         /* this process does no I/O. */
        *ntimes = 0;
    } else {
        /* ntimes=ceiling_div(end_loc - st_loc + 1, coll_bufsize) */
        *ntimes = (int)((end_loc - st_loc + coll_bufsize) / coll_bufsize);
    }
}

ADIOI_Flatlist_node* OCEANFS_Flatten_and_find(MPI_Datatype type)
{
    ADIOI_Flatlist_node* flat_file = ADIOI_Flatlist;
    while (flat_file && flat_file->type != type) {
        flat_file = flat_file->next;
    }

    return flat_file;
}

