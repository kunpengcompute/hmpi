#ifndef AD_OCEANFS_LLT_H
#define AD_OCEANFS_LLT_H

#include "ad_oceanfs.h"
#include "ad_oceanfs_pub.h"
#include "mpi_fs_intf.h"
#include "adio.h"

void llt_pub(int* total, int* success);
void llt_viewio_1(int* total, int* success);
void llt_viewio_2(int* total, int* success);
void llt_fcntl(int* total, int* success);
void llt_group_report(int* total, int* success);
void llt_aggrs(int* total, int* success);
void llt_aggrs_1(int* total, int* success);
void llt_aggrs_2(int* total, int* success);
void llt_aggrs_3(int* total, int* success);
void llt_aggrs_4(int* total, int* success);
void llt_resize(int* total, int* success);
void llt_fs(int* total, int* success);
void llt_file(int* total, int* success);
void llt_wrcoll(int* total, int* success);
void llt_view(int* total, int* success);
void llt_open(int* total, int* success);

void ad_viewio_SetIovec(struct iovec *iov, void  *iov_base, size_t iov_len);
void ad_viewio_ClacFlatBuf(struct iovec *iov, void *buf, ADIO_Offset size_in_buftype, ADIOI_Flatlist_node *flat_buf,
    int* i, int* k, ADIO_Offset j_bufextent);
int ADIOI_OCEANFS_fill_iovec(ADIO_File fd, struct iovec *iov, ADIO_Offset *offset_inbuftypes, void *buf,
    MPI_Datatype buftype, ADIOI_Flatlist_node *flat_buf, int *buftype_idx, int *block_idx, int len, int xfered_len);
int ADIOI_OCEANFS_StridedViewIO(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int io_flag, int *error_code);
ADIO_Offset ad_viewio_update_offset_in_file(ADIO_File fd, ADIO_Offset update_bytes, ADIO_Offset curr_offset);

void ad_aggrs_PreprocBlksize(ADIO_File fd);

void ad_aggrs_CalcOffset(int nprocs, int* scounts, int* sdispls, int* rcounts, int* rdispls, 
    int* count_my_req_per_proc, int* count_others_req_per_proc, ADIOI_Access *my_req,
    ADIOI_Access *others_req, void* sendBufForOffsets, void* recvBufForOffsets);
void ad_aggrs_CalcLen(int nprocs, int* scounts, int* sdispls, int* rcounts, int* rdispls, 
    int* count_my_req_per_proc, int* count_others_req_per_proc, ADIOI_Access *my_req,
    ADIOI_Access *others_req, void* sendBufForLens, void* recvBufForLens);

void ad_wrcoll_CheckHole(ADIO_Offset *srt_off, ADIO_Offset off, int *srt_len, int sum, int size, int* hole);
void ad_wrcoll_InitVec(int nprocs, int *send_buf_idx, int *curr_to_proc, int *done_to_proc, int *sent_to_proc);
void ad_wrcoll_CopyProcVec(int nprocs, int *send_size, int *sent_to_proc, int *curr_to_proc);

void ad_view_Init(u32* block_count, u32** blocklens, off_t** blockoffs, off_t* ub_off, int filetype_is_contig,
    MPI_Count filetype_size, ADIO_File fd);
int ad_open_GetMode(ADIO_File fd);
void ad_open_OpenCheck(ADIO_File fd, int *error_code);

#endif /* AD_OCEANFS_LLT_H */
