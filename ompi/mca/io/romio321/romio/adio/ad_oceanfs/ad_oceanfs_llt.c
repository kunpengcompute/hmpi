#include "ad_oceanfs_llt.h"
#include "ad_oceanfs_group_tuning.h"
#include "ad_oceanfs_aggrs.h"
#include "ad_oceanfs_file.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "ad_env.h"

#define LLT_TMP_FILE_NAME_MAX_LEN   128

static void LltResult(const char* name, int len, int total, int success)
{
    char fileName[LLT_TMP_FILE_NAME_MAX_LEN];
    if (snprintf(fileName, sizeof(fileName), "./%s_tmp_resulte.xml", name) < 0) {
        ROMIO_LOG(AD_LOG_LEVEL_ERR, "snprintf error!");
    }
    FILE* fpXml = fopen(fileName, "w+");
    if (fpXml == NULL) {
        return;
    }
    fprintf(fpXml, " <testsuite name=\"%s\" tests=\"%d\" failures=\"0\" disabled=\"0\" errors=\"%d\" time=\"2\">",
        name, total, (total - success));
    fprintf(fpXml, "</testsuite>\n");
    fclose(fpXml);

    //
    if (snprintf(fileName, sizeof(fileName), "./%s_test_case_count.dat", name) < 0) {
        ROMIO_LOG(AD_LOG_LEVEL_ERR, "snprintf error!");
    }
    fpXml = fopen(fileName, "w+");
    if (fpXml == NULL) {
        return;
    }
    fprintf(fpXml, "%d %d", total, (total - success));
    fclose(fpXml);
}

void llt_pub(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 4;
    int local_suc = 0;
    
    ADIOI_Iread_and_exch_vars vars;
    SetNtimes(&vars, -1, -1, 0);
    if (vars.ntimes == 0) {
        local_suc++;
    }

    static ADIO_Offset st_loc = 10;
    static ADIO_Offset end_loc = 20;
    static int coll_bufsize = 5;
    static int ret = 3;
    SetNtimes(&vars, st_loc, end_loc, coll_bufsize);
    if (vars.ntimes == ret) {
        local_suc++;
    }

    static int nprocs1 = 2;
    int myrank = 1;
    ADIO_Offset startOffset = 1;
    ADIO_Offset endOffset = 1;
    ADIO_Offset myCountSize = 1;
    ADIO_Offset dstStartOffset[] = {0, 1};
    ADIO_Offset dstEndOffset[] = {0, 1};
    ADIO_Offset dstCountSizes[] = {0, 1};
    AdPubOffsetAggmethod1(MPI_COMM_WORLD, nprocs1, myrank, startOffset, endOffset, myCountSize, dstStartOffset,
        dstEndOffset, dstCountSizes);
    if (dstEndOffset[0] == 0) {
        local_suc++;
    }

    static int nprocs0 = 3;
    AdPubOffsetAggmethod0(MPI_COMM_WORLD, nprocs0, myrank, startOffset, endOffset, dstStartOffset, dstEndOffset);
    local_suc++;

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_pub";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_viewio_1(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;
    struct iovec iov;
    ad_viewio_SetIovec(&iov, NULL, 1);

    if (iov.iov_base == NULL && iov.iov_len == 1) {
        local_suc++;
    }
    
    int i = 0;
    int k = 0;
    struct iovec iovs[2];
    int buf[100] = {0};
    ADIO_Offset size_in_buftype = 1;
    ADIO_Offset jBufextent = 1;
    ADIO_Offset blocklen[] = {1};
    ADIO_Offset indices[] = {1};
    ADIOI_Flatlist_node flat_buf;
    static int count = 2;
    flat_buf.count = count;
    flat_buf.blocklens = blocklen;
    flat_buf.indices = indices;

    ad_viewio_ClacFlatBuf(iovs, buf, size_in_buftype, &flat_buf, &i, &k, jBufextent);
    if (k) {
        local_suc++;
    }
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_viewio_1";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_viewio_2(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;
    struct iovec iovs[2];
    int buf[100] = {0};
    ADIO_Offset blocklen[] = {1};
    ADIO_Offset indices[] = {1};
    ADIOI_Flatlist_node flat_buf;
    flat_buf.count = 1;
    flat_buf.blocklens = blocklen;
    flat_buf.indices = indices;

    MPI_Datatype buftype = MPI_LONG_DOUBLE_INT;
    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    int block_idx = 0;
    int buftype_idx = 1;
    static ADIO_Offset offset_inbuftypes = 1;
    static int len = 33;

    memset(iovs, 0, sizeof(iovs));

    ADIOI_OCEANFS_fill_iovec(fd, iovs, &offset_inbuftypes, buf, buftype, &flat_buf, &buftype_idx, &block_idx, len, 1);
    local_suc++;

    buftype = MPI_BYTE;
    ADIOI_OCEANFS_fill_iovec(fd, iovs, &offset_inbuftypes, buf, buftype, &flat_buf, &buftype_idx, &block_idx, len, 1);
    local_suc++;

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_viewio_2";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_fcntl(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 4;
    int local_suc = 0;
    
    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    ADIO_Fcntl_t fcntl_struct;
    int errorCode;

    // default
    ADIOI_OCEANFS_Fcntl(fd, 0, &fcntl_struct, &errorCode);

    if (errorCode != MPI_SUCCESS) {
        local_suc++;
    }

    // ADIO_FCNTL_GET_FSIZE
    fd->filename = "/llt/llt_fcntl";
    ADIOI_OCEANFS_Fcntl(fd, ADIO_FCNTL_GET_FSIZE, &fcntl_struct, &errorCode);
    if (errorCode != MPI_SUCCESS) {
        local_suc++;
    }

    // ADIO_FCNTL_SET_DISKSPACE
    fd->access_mode = 0;
    fd->split_coll_count = 1;
    fd->async_count = 1;
    ADIOI_OCEANFS_Fcntl(fd, ADIO_FCNTL_SET_DISKSPACE, &fcntl_struct, &errorCode);

    if (errorCode == MPI_ERR_IO) {
        local_suc++;
    }

    // ADIO_FCNTL_SET_ATOMICITY
    fcntl_struct.atomicity = 0;
    ADIOI_OCEANFS_Fcntl(fd, ADIO_FCNTL_SET_ATOMICITY, &fcntl_struct, &errorCode);

    if (errorCode == MPI_SUCCESS && fd->atomicity == 0) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_fcntl";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_group_report(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 1;
    int local_suc = 0;

    set_nasmpio_timing(1);
    ad_oceanfs_group_report(NULL, 1);
    local_suc++;

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_group_report";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_aggrs(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;

    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    static ADIO_Offset off = 10;
    static ADIO_Offset min_off = 2;
    static ADIO_Offset len = 1;
    static ADIO_Offset fd_size = 4;
    ADIO_Offset fd_end[] = {0, 0, 0, 10, 9, 12};
    ADIO_Offset fd_start[] = {2, 2, 2, 2, 2, 2};
    int ranklist[] = {1, 1, 1, 1, 1, 1};
    
    ADIOI_Hints hints;
    fd->hints = &hints;
    fd->hints->cb_nodes = fd_size; 
    fd->hints->striping_unit = 1;
    fd->hints->ranklist = ranklist;
    int rank = ADIOI_OCEANFS_Calc_aggregator(fd, off, min_off, &len, fd_size, fd_start, fd_end);
    if (rank == 1) {
        local_suc++;
    }

    int nprocs = 1;
    ADIOI_Access my_req[1];
    int count_my_req_per_proc[] = {1};
    int count_my_req_procs;
    AllocAccess(my_req, nprocs, count_my_req_per_proc, &count_my_req_procs);
    if (count_my_req_procs == 1) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_aggrs";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_aggrs_1(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;

    struct ADIOI_FileD localFile1;
    ADIO_File fd = &localFile1;
    int fsPtrValue = 1;
    ADIO_Offset tmpValue1 = 1;
    ADIO_Offset tmpValue2 = 1;
    static int nprocsForColl = 3;
    static int fdSizeRes = 18;
    static int nprocs = 2;
    fd->blksize = nprocsForColl;
    ADIO_Offset st_offsets[] = {40, 9};
    ADIO_Offset end_offsets[] = {1, 58};
    ADIO_Offset *tmp = (ADIO_Offset *)ADIOI_Malloc(nprocsForColl * sizeof(ADIO_Offset));
    ADIO_Offset *tmp1 = (ADIO_Offset *)ADIOI_Malloc(nprocsForColl * sizeof(ADIO_Offset));
    ADIO_Offset **fdStartPtr = &tmp;
    ADIO_Offset **fdEndPtr = &tmp1;
    ADIO_Offset *min_st_offset_ptr = &tmpValue1;
    ADIO_Offset *fd_size_ptr = &tmpValue2;
    ADIOI_OCEANFS_Calc_file_domains(fd, st_offsets, end_offsets, nprocs, nprocsForColl,
        min_st_offset_ptr, fdStartPtr, fdEndPtr, fd_size_ptr, (void *)(&fsPtrValue));
    if (*fd_size_ptr == fdSizeRes) {
        local_suc++;
    }

    fd->blksize = 0;
    ad_aggrs_PreprocBlksize(fd);
    local_suc++;
    
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_aggrs_1";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_aggrs_2(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;
    int count_others_req_per_proc[] = {0};
    ADIOI_Access others_req[1];
    int count_others_req_procs_ptr = 0;
    void* recvBufForOffsets = (void *)0xFFFFFFFFFFFFFFFF;
    void* recvBufForLens = (void *)0xFFFFFFFFFFFFFFFF;

    AllocOtherReq(1, count_others_req_per_proc, &recvBufForOffsets, 
        &recvBufForLens, others_req, &count_others_req_procs_ptr);

    if (others_req[0].offsets == NULL && count_others_req_procs_ptr == 0) {
        local_suc++;
    }

    count_others_req_per_proc[0] = 1;
    AllocOtherReq(1, count_others_req_per_proc, &recvBufForOffsets, 
        &recvBufForLens, others_req, &count_others_req_procs_ptr);

    if (count_others_req_procs_ptr == 1) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_aggrs_2";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_aggrs_3(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 4;
    int local_suc = 0;

    int scounts[1];
    int sdispls[1];
    int rcounts[1];
    int rdispls[1];

    int count_my_req_per_proc[] = {0};
    int count_others_req_per_proc[] = {0};
    ad_aggrs_CalcOffset(1, scounts, sdispls, rcounts, rdispls, 
        count_my_req_per_proc, count_others_req_per_proc, NULL, NULL, NULL, NULL);

    if (sdispls[0] == 0 && rdispls[0] == 0) {
        local_suc++;
    }

    ad_aggrs_CalcLen(1, scounts, sdispls, rcounts, rdispls, 
        count_my_req_per_proc, count_others_req_per_proc, NULL, NULL, NULL, NULL);

    if (sdispls[0] == 0 && rdispls[0] == 0) {
        local_suc++;
    }

    ADIOI_Access my_req[1];
    ADIOI_Access others_req[1];
    void* sendBufForOffsets = (void*)0xFFFF;
    void* recvBufForOffsets = (void*)0xFFFF;
    my_req[0].offsets = (ADIO_Offset *)0xFFFFFF;
    others_req[0].offsets = (ADIO_Offset *)0xFFFFFF;
    count_my_req_per_proc[0] = 1;
    count_others_req_per_proc[0] = 1;
    ad_aggrs_CalcOffset(1, scounts, sdispls, rcounts, rdispls, 
        count_my_req_per_proc, count_others_req_per_proc, my_req,
        others_req, sendBufForOffsets, recvBufForOffsets);

    if (sdispls[0] > 0 && rdispls[0] > 0) {
        local_suc++;
    }

    my_req[0].lens = (ADIO_Offset *)0xFFFFFF;
    others_req[0].lens = (ADIO_Offset *)0xFFFFFF;
    ad_aggrs_CalcLen(1, scounts, sdispls, rcounts, rdispls, 
        count_my_req_per_proc, count_others_req_per_proc, my_req,
        others_req, sendBufForOffsets, recvBufForOffsets);

    if (sdispls[0] > 0 && rdispls[0] > 0) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_aggrs_3";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_aggrs_4(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 1;
    int local_suc = 0;

    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    static ADIO_Offset fd_size = 4;
    ADIO_Offset fd_end[] = {0, 0, 0, 12, 12, 12, 12, 12, 12};
    ADIO_Offset fd_start[] = {2, 2, 2, 2, 2, 2};
    ADIO_Offset offset_list[] = {10};
    ADIO_Offset len_list[] = {2};
    ADIO_Offset min_st_offset = 2;
    int ranklist[] = {1, 1, 1, 1, 1, 1};
    ADIOI_Hints hints;
    fd->hints = &hints;
    fd->hints->cb_nodes = fd_size; 
    fd->hints->striping_unit = 1;
    fd->hints->ranklist = ranklist;
    int contig_access_count = 1;
    int nprocs = 2;
    ADIOI_Access *tmp = (ADIOI_Access *)ADIOI_Malloc(nprocs * sizeof(ADIOI_Access));
    int *tmp1 = (int *)ADIOI_Malloc(nprocs * sizeof(int));
    OCEANFS_Int *tmp_idx = (OCEANFS_Int *)ADIOI_Malloc(nprocs * sizeof(OCEANFS_Int));
    OCEANFS_Int **buf_idx = &tmp_idx;
    int **count_my_req_per_proc = &tmp1;
    int count_my_req_procs;
    ADIOI_Access **my_req = &tmp;
    ADIOI_OCEANFS_Calc_my_req(fd, offset_list, len_list, contig_access_count, min_st_offset, fd_start,
        fd_end, fd_size, nprocs, &count_my_req_procs, count_my_req_per_proc, my_req, buf_idx);
    local_suc++;

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");
    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_aggrs_4";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_resize(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 3;
    int local_suc = 0;
    
    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    int errorCode = MPI_SUCCESS;

    ADIOI_OCEANFS_Resize(NULL, 0, NULL);
    local_suc++;

    ADIOI_OCEANFS_Resize(NULL, 0, &errorCode);
    if (errorCode == MPI_ERR_FILE) {
        local_suc++;
    }

    fd->split_coll_count = 1;
    ADIOI_OCEANFS_Resize(fd, 0, &errorCode);
    if (errorCode == MPI_ERR_IO) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_resize";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_fs(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 3;
    int local_suc = 0;

    mpi_fs_ftruncate(0, 0);
    local_suc++;
    mpi_fs_lseek(0, 0, 0);
    local_suc++;
    mpi_fs_view_read(0, 0, NULL, 0);
    local_suc++;

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_fs";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_file(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;

    static char* g_head_row[] = { 
        "contig-r", "contig-w", "strided-r", "strided-w",
        "strided-coll-r", "strided-coll-w", "oceanfs-r",
        "oceanfs-w", "oceanfs-call-r", "oceanfs-call-w"
        };
    static int g_row_cnt = 10;
    static int g_head_row_size = NASMPIO_CIO_T_FUN_MAX;
    static char* g_head_col[] = { "avg", "max" };
    static int g_col_cnt = 2;
    static int g_col_len = 18;
    static int g_head_col_size = 2;

    TAdOceanfsFile *nas_file = ad_oceanfs_file_init("/tmp/1.txt", FILE_CREATE_FORCE,
        g_row_cnt, g_col_cnt, g_col_len,
        g_head_row, g_head_row_size, g_head_col, g_head_col_size);
    ad_oceanfs_file_set_int(NULL, 0, 0, 0);
    local_suc++;
    if (nas_file) {
        ad_oceanfs_file_set_int(nas_file, 0, 0, 1);
        local_suc++;
    }
    ad_oceanfs_file_destroy(nas_file);

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_file";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_wrcoll(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 4;
    int local_suc = 0;

    ADIO_Offset srt_off[] = {1, 3};
    int hole;
    ad_wrcoll_CheckHole(srt_off, 0, NULL, 0, 0, &hole);

    if (hole == 1) {
        local_suc++;
    }

    static int sum = 2;
    int srt_len[] = {1, 1};
    ad_wrcoll_CheckHole(srt_off, 1, srt_len, sum, 0, &hole);

    if (hole == 1) {
        local_suc++;
    }

    int send_buf_idx[1];
    int curr_to_proc[1];
    int done_to_proc[1];
    int sent_to_proc[1] = {0};
    ad_wrcoll_InitVec(1, send_buf_idx, curr_to_proc, done_to_proc, sent_to_proc);

    if (send_buf_idx[0] == 0 && curr_to_proc[0] == 0) {
        local_suc++;
    }

    int send_size[] = {1};
    curr_to_proc[0] = 1;
    ad_wrcoll_CopyProcVec(1, send_size, sent_to_proc, curr_to_proc);

    if (sent_to_proc[0] == 1) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_wrcoll";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_view(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 1;
    int local_suc = 0;

    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    int filetype_is_contig;
    u32 block_count;
    u32 *blocklens = NULL;
    off_t *blockoffs = NULL;
    off_t ub_off;
    MPI_Count filetype_size;
    MPI_Datatype filetype = MPI_BYTE;
    MPI_Type_size_x(filetype, &filetype_size);
    ADIOI_Datatype_iscontig(filetype, &filetype_is_contig);
    ad_view_Init(&block_count, &blocklens, &blockoffs, &ub_off, filetype_is_contig, filetype_size, fd);
    local_suc++;
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_view";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}

void llt_open(int* total, int* success)
{
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT Start");
    static int local_cnt = 2;
    int local_suc = 0;

    struct ADIOI_FileD localFile;
    ADIO_File fd = &localFile;
    fd->access_mode = 0;
    fd->access_mode |= ADIO_WRONLY;
    fd->access_mode |= ADIO_RDWR;
    fd->access_mode |= ADIO_EXCL;
    fd->access_mode |= ADIO_APPEND;
    int amode = ad_open_GetMode(fd);
    if ((amode & O_WRONLY) && (amode & O_RDWR) && (amode & O_EXCL) && (amode & O_APPEND)) {
        local_suc++;
    }

    int errorCode = 0;
    fd->fd_sys = 0;
    fd->access_mode = ADIO_APPEND;
    fd->filename = "/llt/opentest";

    ADIOI_OCEANFS_fs *nas_fs = (ADIOI_OCEANFS_fs *)ADIOI_Malloc(sizeof(ADIOI_OCEANFS_fs));
    nas_fs->context = (MPI_CONTEXT_T *)ADIOI_Malloc(sizeof(MPI_CONTEXT_T));
    nas_fs->nas_filename = NULL;
    fd->fs_ptr = nas_fs;

    ad_open_OpenCheck(fd, &errorCode);

    if (errorCode == MPI_ERR_IO) {
        local_suc++;
    }

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT End");

    (*total) += local_cnt;
    (*success) += local_suc;

    static char name[] = "llt_open";
    LltResult(name, sizeof(name), local_cnt, local_suc);
}
