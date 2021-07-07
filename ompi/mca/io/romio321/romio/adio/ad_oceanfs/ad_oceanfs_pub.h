#ifndef AD_OCEANFS_PUB_H
#define AD_OCEANFS_PUB_H

#include "adio.h"
#include "adio_extern.h"

#define OCEANFS_Int int
#define OCEANFS_Delete_flattened(type)  ADIOI_Delete_flattened(type)
ADIOI_Flatlist_node* OCEANFS_Flatten_and_find(MPI_Datatype type);
#define OCEANFS_UNREFERENCED_ARG(arg) ADIOI_UNREFERENCED_ARG(arg)

#ifdef AGGREGATION_PROFILE
#define OCEANFS_Log_event(id) MPE_Log_event((id), 0, NULL)
#else
#define OCEANFS_Log_event(id)
#endif

#define MUL_2 (2)
#define MUL_100 (100)

#define FreeAdioiTwo(a, b) \
    do { \
        ADIOI_Free(a); \
        ADIOI_Free(b); \
    } while (0)

#define FreeAdioiThree(a, b, c) \
    do { \
        ADIOI_Free(a); \
        ADIOI_Free(b); \
        ADIOI_Free(c); \
    } while (0)

#define FreeAdioiFour(a, b, c, d) \
    do { \
        ADIOI_Free(a); \
        ADIOI_Free(b); \
        ADIOI_Free(c); \
        ADIOI_Free(d); \
    } while (0)

#define FreeAdioiFive(a, b, c, d, e) \
    do { \
        ADIOI_Free(a); \
        ADIOI_Free(b); \
        ADIOI_Free(c); \
        ADIOI_Free(d); \
        ADIOI_Free(e); \
    } while (0)

#define AD_COLL_BUF_INCR                                                                                 \
{                                                                                                        \
    while (buf_incr) {                                                                                   \
        size_in_buf = ADIOI_MIN(buf_incr, flat_buf_sz);                                                  \
        user_buf_idx += size_in_buf;                                                                     \
        flat_buf_sz -= size_in_buf;                                                                      \
        if (!flat_buf_sz) {                                                                              \
            if (flat_buf_idx < (flat_buf->count - 1))                                                    \
                flat_buf_idx++;                                                                          \
            else {                                                                                       \
                flat_buf_idx = 0;                                                                        \
                n_buftypes++;                                                                            \
            }                                                                                            \
            user_buf_idx =                                                                               \
                flat_buf->indices[flat_buf_idx] + (ADIO_Offset)n_buftypes * (ADIO_Offset)buftype_extent; \
            flat_buf_sz = flat_buf->blocklens[flat_buf_idx];                                             \
        }                                                                                                \
        buf_incr -= size_in_buf;                                                                         \
    }                                                                                                    \
}

enum MPE_Log_event_ID {
    MPE_Log_ID_7    = 7,
    MPE_Log_ID_8    = 8,
    MPE_Log_ID_13   = 13,
    MPE_Log_ID_14   = 14,
    MPE_Log_ID_5004 = 5004,
    MPE_Log_ID_5005 = 5005,
    MPE_Log_ID_5012 = 5012,
    MPE_Log_ID_5013 = 5013,
    MPE_Log_ID_5024 = 5024,
    MPE_Log_ID_5025 = 5025,
    MPE_Log_ID_5026 = 5026,
    MPE_Log_ID_5027 = 5027,
    MPE_Log_ID_5032 = 5032,
    MPE_Log_ID_5033 = 5033
};

enum GET_Oceanfsmpio_Read_Aggmethod_VALUE {
    OCEANFS_1 = 1,
    OCEANFS_2 = 2
};

/* ADIOI_GEN_IreadStridedColl */
struct ADIOI_GEN_IreadStridedColl_vars {
    /* requests */
    MPI_Request req_offset[2]; /* ADIOI_IRC_STATE_GEN_IREADSTRIDEDCOLL */
    MPI_Request req_ind_io;    /* ADIOI_IRC_STATE_GEN_IREADSTRIDEDCOLL_INDIO */

    /* parameters */
    ADIO_File fd;
    void *buf;
    int count;
    MPI_Datatype datatype;
    int file_ptr_type;
    ADIO_Offset offset;

    /* stack variables */
    ADIOI_Access *my_req;
    /* array of nprocs structures, one for each other process in
       whose file domain this process's request lies */

    ADIOI_Access *others_req;
    /* array of nprocs structures, one for each other process
       whose request lies in this process's file domain. */

    int nprocs;
    int nprocs_for_coll;
    int myrank;
    int contig_access_count;
    int interleave_count;
    int buftype_is_contig;
    int *count_my_req_per_proc;
    int count_my_req_procs;
    int count_others_req_procs;
    ADIO_Offset start_offset;
    ADIO_Offset end_offset;
    ADIO_Offset orig_fp;
    ADIO_Offset fd_size;
    ADIO_Offset min_st_offset;
    ADIO_Offset *offset_list;
    ADIO_Offset *st_offsets;
    ADIO_Offset *fd_start;
    ADIO_Offset *fd_end;
    ADIO_Offset *end_offsets;
    ADIO_Offset *len_list;
    int *buf_idx;
};

/* ADIOI_Iread_and_exch */
struct ADIOI_Iread_and_exch_vars {
    /* requests */
    MPI_Request req1; /* ADIOI_IRC_STATE_IREAD_AND_EXCH */
    MPI_Request req2; /* ADIOI_IRC_STATE_IREAD_AND_EXCH_L1_BEGIN */

    /* parameters */
    ADIO_File fd;
    void *buf;
    MPI_Datatype datatype;
    int nprocs;
    int myrank;
    ADIOI_Access *others_req;
    ADIO_Offset *offset_list;
    ADIO_Offset *len_list;
    int contig_access_count;
    ADIO_Offset min_st_offset;
    ADIO_Offset fd_size;
    ADIO_Offset *fd_start;
    ADIO_Offset *fd_end;
    int *buf_idx;

    /* stack variables */
    int m;
    int ntimes;
    int max_ntimes;
    int buftype_is_contig;
    ADIO_Offset st_loc;
    ADIO_Offset end_loc;
    ADIO_Offset off;
    ADIO_Offset done;
    char *read_buf;
    int *curr_offlen_ptr;
    int *count;
    int *send_size;
    int *recv_size;
    int *partial_send;
    int *recd_from_proc;
    int *start_pos;
    /* Not convinced end_loc-st_loc couldn't be > int, so make these offsets */
    ADIO_Offset size;
    ADIO_Offset real_size;
    ADIO_Offset for_curr_iter;
    ADIO_Offset for_next_iter;
    ADIOI_Flatlist_node *flat_buf;
    MPI_Aint buftype_extent;
    int coll_bufsize;

    /* next function to be called */
    void (*next_fn)(ADIOI_NBC_Request *, int *);
};

/* ADIOI_R_Iexchange_data */
struct ADIOI_R_Iexchange_data_vars {
    /* requests */
    MPI_Request req1;  /* ADIOI_IRC_STATE_R_IEXCHANGE_DATA */
    MPI_Request *req2; /* ADIOI_IRC_STATE_R_IEXCHANGE_DATA_RECV & FILL */

    /* parameters */
    ADIO_File fd;
    void *buf;
    ADIOI_Flatlist_node *flat_buf;
    ADIO_Offset *offset_list;
    ADIO_Offset *len_list;
    int *send_size;
    int *recv_size;
    int *count;
    int *start_pos;
    int *partial_send;
    int *recd_from_proc;
    int nprocs;
    int myrank;
    int buftype_is_contig;
    int contig_access_count;
    ADIO_Offset min_st_offset;
    ADIO_Offset fd_size;
    ADIO_Offset *fd_start;
    ADIO_Offset *fd_end;
    ADIOI_Access *others_req;
    int iter;
    MPI_Aint buftype_extent;
    int *buf_idx;

    /* stack variables */
    int nprocs_recv;
    int nprocs_send;
    char **recv_buf;

    /* next function to be called */
    void (*next_fn)(ADIOI_NBC_Request *, int *);
};

void AdPubOffsetAggmethod1(MPI_Comm comm, int nprocs, int myrank, ADIO_Offset startOffset, ADIO_Offset endOffset,
    ADIO_Offset myCountSize, ADIO_Offset *dstStartOffset, ADIO_Offset *dstEndOffset, ADIO_Offset *dstCountSizes);
void AdPubOffsetAggmethod0(MPI_Comm comm, int nprocs, int myrank, ADIO_Offset startOffset, ADIO_Offset endOffset,
    ADIO_Offset *dstStartOffset, ADIO_Offset *dstEndOffset);

void CheckOffsetAndLen(void** recvBufForOffsets, void** recvBufForLens);
void AllocAccess(ADIOI_Access *my_req, int nprocs, int *my_req_per_proc, int* count_procs_ptr);
void AllocOtherReq(int nprocs, int *others_req_per_proc, void** recvBufForOffsets,
    void** recvBufForLens, ADIOI_Access *others_req, int* others_req_procs_ptr);
void FreeAccess(ADIOI_Access *acc, int nprocs);
void FreeAccessAll(ADIOI_Access *acc, int nprocs);

int CalcCount(int* array, int nprocs);
void CalcLoc(ADIOI_Access *others_req, int nprocs, ADIO_Offset* st_loc, ADIO_Offset* end_loc);
void SetNtimes(ADIOI_Iread_and_exch_vars *vars, ADIO_Offset st_loc, ADIO_Offset end_loc, int coll_bufsize);
void SetNtimesLocal(int *ntimes, ADIO_Offset st_loc, ADIO_Offset end_loc, int coll_bufsize);

#endif /* AD_OCEANFS_PUB_H */
