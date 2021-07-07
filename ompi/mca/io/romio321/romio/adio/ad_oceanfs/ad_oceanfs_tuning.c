#include "ad_oceanfs_tuning.h"
#include "ad_oceanfs_file.h"
#include "ad_env.h"
#include "mpi.h"

static double g_prof_sum[OCEANFSMPIO_CIO_T_FUN_MAX] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
static int g_prof_cnt[OCEANFSMPIO_CIO_T_FUN_MAX] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
static double g_prof_max[OCEANFSMPIO_CIO_T_FUN_MAX] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

double ad_oceanfs_timing_get_time()
{
    return MPI_Wtime();
}

static void update_time(int fun, double val)
{
    static const int fun_divisor = 2;
    if (fun >= OCEANFSMPIO_CIO_R_CONTIG && fun < OCEANFSMPIO_CIO_T_FUN_MAX) {
        g_prof_sum[fun] += val;
        g_prof_cnt[fun] += 1;
    
        if (fun % fun_divisor == 0) {
            g_prof_sum[OCEANFSMPIO_CIO_R_TOTAL_CNT]++;
            g_prof_max[OCEANFSMPIO_CIO_R_TOTAL_CNT] = g_prof_sum[OCEANFSMPIO_CIO_R_TOTAL_CNT];
        } else {
            g_prof_sum[OCEANFSMPIO_CIO_W_TOTAL_CNT]++;
            g_prof_max[OCEANFSMPIO_CIO_W_TOTAL_CNT] = g_prof_sum[OCEANFSMPIO_CIO_W_TOTAL_CNT];
        }

        if (val > g_prof_max[fun] + 1e-10) {
            g_prof_max[fun] = val;
        }
    }
}

void ad_oceanfs_timing_report(ADIO_File fd, int fun, double start_val)
{
    if (!get_oceanfsmpio_timing() || fd->comm == MPI_COMM_NULL) {
        return;
    }

    double val = ad_oceanfs_timing_get_time() - start_val;
    update_time(fun, val);

    double local_avg[OCEANFSMPIO_CIO_T_FUN_MAX];
    int i;
    for (i = OCEANFSMPIO_CIO_R_CONTIG; i < OCEANFSMPIO_CIO_R_TOTAL_CNT; i++) {
        local_avg[i] = ((g_prof_cnt[i] == 0) ? 0 : (g_prof_sum[i] / g_prof_cnt[i]));
    }
    local_avg[OCEANFSMPIO_CIO_R_TOTAL_CNT] = g_prof_sum[OCEANFSMPIO_CIO_R_TOTAL_CNT];
    local_avg[OCEANFSMPIO_CIO_W_TOTAL_CNT] = g_prof_sum[OCEANFSMPIO_CIO_W_TOTAL_CNT];

    static char* g_head_row[] = { 
        "contig-r", "contig-w", "strided-r", "strided-w",
        "strided-coll-r", "strided-coll-w", "oceanfs-r",
        "oceanfs-w", "oceanfs-call-r", "oceanfs-call-w"
        };
    static int g_row_cnt = 10;
    static int g_head_row_size = OCEANFSMPIO_CIO_T_FUN_MAX;
    static char* g_head_col[] = { "avg", "max" };
    static int g_col_cnt = 2;
    static int g_col_len = 18;
    static int g_col_avg_no = 0;
    static int g_col_max_no = 1;
    static int g_head_col_size = 2;
    
    static int g_dir_len = 128;
    char pname[g_dir_len];
    snprintf(pname, sizeof(pname), "/mpi_state/%d", getpid());
    
    TAdOceanfsFile *oceanfs_file = ad_oceanfs_file_init(pname, FILE_CREATE_INTIME, g_row_cnt, g_col_cnt, g_col_len,
        g_head_row, g_head_row_size, g_head_col, g_head_col_size);

    if (oceanfs_file == NULL) {
        return;
    }

    for (i = OCEANFSMPIO_CIO_R_CONTIG; i < OCEANFSMPIO_CIO_T_FUN_MAX; i++) {
        ad_oceanfs_file_set_double(oceanfs_file, i, g_col_avg_no, local_avg[i]);
        ad_oceanfs_file_set_double(oceanfs_file, i, g_col_max_no, g_prof_max[i]);
    }

    ad_oceanfs_file_destroy(oceanfs_file);
}
