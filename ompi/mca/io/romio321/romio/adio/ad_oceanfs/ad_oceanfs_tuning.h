#ifndef AD_OCEANFS_TUNING_H_
#define AD_OCEANFS_TUNING_H_

#include "adio.h"

/* timing fields */
enum {
    OCEANFSMPIO_CIO_R_CONTIG = 0,
    OCEANFSMPIO_CIO_W_CONTIG,
    OCEANFSMPIO_CIO_R_STRIDED,
    OCEANFSMPIO_CIO_W_STRIDED,
    OCEANFSMPIO_CIO_R_STRIDED_COLL,
    OCEANFSMPIO_CIO_W_STRIDED_COLL,
    OCEANFSMPIO_CIO_R_OCEANFS,
    OCEANFSMPIO_CIO_W_OCEANFS,
    OCEANFSMPIO_CIO_R_TOTAL_CNT,
    OCEANFSMPIO_CIO_W_TOTAL_CNT,
    OCEANFSMPIO_CIO_T_FUN_MAX
};

double ad_oceanfs_timing_get_time();
void ad_oceanfs_timing_report(ADIO_File fd, int fun, double start_val);

#endif /* AD_OCEANFS_TUNING_H_ */
