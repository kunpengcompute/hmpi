#ifndef AD_OCEANFS_FILE_H_
#define AD_OCEANFS_FILE_H_

#include "adio.h"

typedef struct {
    FILE *fp;    // 文件句柄
    int row;     // 行数
    int col;     // 列数
    int col_len; // 列宽度
    int row_len; // 行总宽度
    int new;     // 是否是新创建的文件
} TAdOceanfsFile;

enum {
    FILE_CREATE_NOT = 0,
    FILE_CREATE_INTIME,
    FILE_CREATE_FORCE
};

TAdOceanfsFile *ad_oceanfs_file_init(char *file_name, int create, int row, int col, int col_len, char **row_head,
    int row_head_size, char **col_head, int col_head_size);
int ad_oceanfs_file_set(TAdOceanfsFile *nas_file, int row, int col, char *val);
int ad_oceanfs_file_set_double(TAdOceanfsFile *nas_file, int row, int col, double val);
int ad_oceanfs_file_set_llu(TAdOceanfsFile *nas_file, int row, int col, unsigned long long val);
int ad_oceanfs_file_set_int(TAdOceanfsFile *nas_file, int row, int col, int val);
void ad_oceanfs_file_destroy(TAdOceanfsFile *nas_file);

#endif /* AD_OCEANFS_FILE_H_ */
