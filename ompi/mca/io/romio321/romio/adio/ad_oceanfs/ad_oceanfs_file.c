#include "ad_oceanfs_file.h"
#include <unistd.h>

#define FILE_VALUE_MAX_LEN  128
#define ONE_DIR_LEN         256

static int file_create(char *file_name, int create, int row, int col, int col_len)
{
    if (create != FILE_CREATE_FORCE) {
        FILE *fpRead = fopen(file_name, "rb");
        if (fpRead) {
            fclose(fpRead);
            return 0;
        } else if (create == FILE_CREATE_NOT) {
            return 0;
        }
    }

    FILE *fpCreate = fopen(file_name, "w+t");

    if (fpCreate == NULL) {
        return 0;
    }

    int i, j, k;
    for (i = 0; i < row; i++) {
        for (j = 0; j < col; j++) {
            fprintf(fpCreate, "0");
            for (k = 1; k < col_len; k++) {
                fprintf(fpCreate, " ");
            }
        }
        fprintf(fpCreate, "\n");
    }

    fclose(fpCreate);
    return 1;
}

static int is_start(char c)
{
    return c != '.' && c != '/';
}

static int save_str(char* dst, int dst_size, char* head, char* end)
{
    int src_len = end - head;
    if (src_len >= dst_size) {
        return -1;
    }

    int i;
    for (i = 0; i < src_len; i++) {
        dst[i] = head[i];
    }
    dst[src_len] = 0;
    return 0;
}

static int create_dir(char *file_name)
{
    static const int rwx = 0777;
    char* pc_end = file_name;
    int i;
    for (i = strlen(file_name) - 1; i >= 0; i--) {
        if (file_name[i] == '/') {
            pc_end = file_name + i;
            break;
        }
    }

    char pc_dir[ONE_DIR_LEN];
    if (save_str(pc_dir, sizeof(pc_dir), file_name, pc_end) < 0) {
        return -1;
    }

    if (access(pc_dir, F_OK) >= 0) {
        return 0;
    }

    char* pc_head = NULL;
    for (pc_head = file_name; pc_head < pc_end; pc_head++) {
        if (is_start(*pc_head)) {
            break;
        }
    }

    char* pc = NULL;
    for (pc = pc_head; pc <= pc_end; pc++) {
        if (*pc == '/') {
            if (save_str(pc_dir, sizeof(pc_dir), file_name, pc) < 0) {
                return -1;
            }

            if (access(pc_dir, F_OK) >= 0) {
                continue;
            }

            if (mkdir(pc_dir, rwx) < 0) {
                return -1;
            }
        }
    }

    return 0;
}

TAdOceanfsFile *ad_oceanfs_file_init(char *file_name, int create, int row, int col, int col_len, char **row_head,
    int row_head_size, char **col_head, int col_head_size)
{
    create_dir(file_name);
    
    int new = file_create(file_name, create, row + 1, col + 1, col_len);

    FILE *fp = fopen(file_name, "r+");
    if (fp == NULL) {
        return NULL;
    }

    TAdOceanfsFile *ret = (TAdOceanfsFile *)malloc(sizeof(TAdOceanfsFile));
    if (ret == NULL) {
        return NULL;
    }

    ret->fp = fp;
    ret->row = row;
    ret->col = col;
    ret->col_len = col_len;
    ret->row_len = ret->col_len * (ret->col + 1) + 1;
    ret->new = new;

    int index;
    int i;
    for (i = 0; i < col; i++) {
        if (i >= col_head_size) {
            index = i % col_head_size;
        } else {
            index = i;
        }
        ad_oceanfs_file_set(ret, -1, i, col_head[index]);
    }
    for (i = 0; i < row; i++) {
        if (i >= col_head_size && row_head_size != 0) {
            index = i % row_head_size;
        } else {
            index = i;
        }
        ad_oceanfs_file_set(ret, i, -1, row_head[index]);
    }

    return ret;
}

int ad_oceanfs_file_set(TAdOceanfsFile *oceanfs_file, int row, int col, char *val)
{
    int local_r = row + 1;
    int local_c = col + 1;

    if (oceanfs_file == NULL || local_r < 0 || local_r > oceanfs_file->row || local_c < 0 || local_c > oceanfs_file->col) {
        return -1;
    }

    int ret = fseek(oceanfs_file->fp, oceanfs_file->row_len * local_r + oceanfs_file->col_len * local_c, SEEK_SET);
    if (ret < 0) {
        return ret;
    }

    fprintf(oceanfs_file->fp, "%s", val);

    return 0;
}

int ad_oceanfs_file_set_double(TAdOceanfsFile *oceanfs_file, int row, int col, double val)
{
    if (oceanfs_file == NULL) {
        return -1;
    }

    char ps_format[FILE_VALUE_MAX_LEN];
    snprintf(ps_format, sizeof(ps_format), "%c-%d.6lf", '%', oceanfs_file->col_len);
    char ps_val[FILE_VALUE_MAX_LEN];
    snprintf(ps_val, sizeof(ps_val), ps_format, val);
    ad_oceanfs_file_set(oceanfs_file, row, col, ps_val);
    return 0;
}

int ad_oceanfs_file_set_llu(TAdOceanfsFile *oceanfs_file, int row, int col, unsigned long long val)
{
    if (oceanfs_file == NULL) {
        return -1;
    }

    char ps[FILE_VALUE_MAX_LEN];
    snprintf(ps, sizeof(ps), "%llu", val);
    ad_oceanfs_file_set(oceanfs_file, row, col, ps);
    return 0;
}

int ad_oceanfs_file_set_int(TAdOceanfsFile *oceanfs_file, int row, int col, int val)
{
    if (oceanfs_file == NULL) {
        return -1;
    }

    char ps[FILE_VALUE_MAX_LEN];
    snprintf(ps, sizeof(ps), "%d", val);
    ad_oceanfs_file_set(oceanfs_file, row, col, ps);
    return 0;
}

void ad_oceanfs_file_destroy(TAdOceanfsFile *oceanfs_file)
{
    if (oceanfs_file == NULL) {
        return;
    }

    if (oceanfs_file->fp) {
        fclose(oceanfs_file->fp);
        oceanfs_file->fp = NULL;
    }

    free(oceanfs_file);
    oceanfs_file = NULL;
}
