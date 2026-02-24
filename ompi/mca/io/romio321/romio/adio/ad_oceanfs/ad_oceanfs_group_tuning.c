#include "ad_oceanfs_group_tuning.h"
#include <stdint.h>
#include "ad_oceanfs_file.h"
#include "ad_env.h"
#include "mpi.h"


#define GROUP_TUNING_SIZE 4

static int g_row_cnt = GROUP_TUNING_SIZE;
static uint64_t g_group[GROUP_TUNING_SIZE] = {0, 0, 0, 0};
static uint64_t g_cnt[GROUP_TUNING_SIZE] = {0, 0, 0, 0};
static int g_index = 0;
static int g_col_cnt = 2;
static int g_col_len = 25;
static char* g_head_row[] = {"1", "2", "3", "4"};
static int g_head_row_size = 4;
static char* g_head_col[] = {"GroupId", "Count"};
static int g_head_col_size = 2;

void ad_oceanfs_group_report(ADIO_File fd, uint64_t group_id)
{
    if (get_nasmpio_timing() == 0 || group_id <= 0) {
        return;
    }
    ROMIO_LOG(AD_LOG_LEVEL_ALL, "name:%s,group:%llu", fd->filename, (unsigned long long)group_id);

    static int g_dir_len = 128;
    char pname[g_dir_len];
    if (snprintf(pname, sizeof(pname), "/mpi_state/group%d", getpid()) < 0) {
        ROMIO_LOG(AD_LOG_LEVEL_ERR, "snprintf error!");
    }

    TAdOceanfsFile *nas_file = ad_oceanfs_file_init(pname, FILE_CREATE_INTIME, g_row_cnt, g_col_cnt, g_col_len,
        g_head_row, g_head_row_size, g_head_col, g_head_col_size);

    if (nas_file == NULL) {
        return;
    }

    /* 新创建的文件，重新计算index
     * 其他进程有删掉此文件，本进程会多次创建
     */
    if (nas_file->new) {
        g_index = 0;
    }

    int find = 0;
    int j;
    for (j = 0; j < g_index; j++) {
        if (g_group[j] == group_id) {
            g_cnt[j]++;
            ad_oceanfs_file_set_llu(nas_file, j, 1, g_cnt[j]);
            find = 1;
            break;
        }
    }

    if (find == 0 && g_index < g_row_cnt) {
        g_group[g_index] = group_id;
        g_cnt[g_index] = 1;
        ad_oceanfs_file_set_llu(nas_file, g_index, 0, g_group[g_index]);
        ad_oceanfs_file_set_llu(nas_file, g_index, 1, 1);
        g_index++;
    }

    ad_oceanfs_file_destroy(nas_file);
}
