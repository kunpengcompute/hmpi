#ifndef AD_OCEANFS_FILE_H_
#define AD_OCEANFS_FILE_H_

#include "adio.h"

typedef struct {
    FILE *fp;
    int row;
    int col;
    int col_len;
    int row_len;
    int new;
} TAdOceanfsFile;

enum {
    FILE_CREATE_NOT = 0,
    FILE_CREATE_INTIME,
    FILE_CREATE_FORCE
};

TAdOceanfsFile *ad_oceanfs_file_init(char *file_name, int create, int row, int col, int col_len, char **row_head,
    int row_head_size, char **col_head, int col_head_size);
int ad_oceanfs_file_set(TAdOceanfsFile *oceanfs_file, int row, int col, char *val);
int ad_oceanfs_file_set_double(TAdOceanfsFile *oceanfs_file, int row, int col, double val);
int ad_oceanfs_file_set_llu(TAdOceanfsFile *oceanfs_file, int row, int col, unsigned long long val);
int ad_oceanfs_file_set_int(TAdOceanfsFile *oceanfs_file, int row, int col, int val);
void ad_oceanfs_file_destroy(TAdOceanfsFile *oceanfs_file);

#endif /* AD_OCEANFS_FILE_H_ */
