#ifndef AD_OCEANFS_TUNING_H_
#define AD_OCEANFS_TUNING_H_

#include "adio.h"

/* timing fields */
enum {
    NASMPIO_CIO_R_CONTIG = 0,
    NASMPIO_CIO_W_CONTIG,
    NASMPIO_CIO_R_STRIDED,
    NASMPIO_CIO_W_STRIDED,
    NASMPIO_CIO_R_STRIDED_COLL,
    NASMPIO_CIO_W_STRIDED_COLL,
    NASMPIO_CIO_R_NAS,
    NASMPIO_CIO_W_NAS,
    NASMPIO_CIO_R_TOTAL_CNT,
    NASMPIO_CIO_W_TOTAL_CNT,
    NASMPIO_CIO_T_FUN_MAX
};

double ad_oceanfs_timing_get_time();
void ad_oceanfs_timing_report(ADIO_File fd, int fun, double start_val);

#endif /* AD_OCEANFS_TUNING_H_ */
