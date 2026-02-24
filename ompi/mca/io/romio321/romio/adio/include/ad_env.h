#ifndef AD_ENV_H_
#define AD_ENV_H_

#include "adio.h"

void ad_oceanfs_get_env_vars();

/* corresponds to environment variables to select optimizations and timing level */
int get_nasmpio_timing();
void set_nasmpio_timing(int val);
int get_nasmpio_timing_cw_level();
int get_nasmpio_comm();
int get_nasmpio_tunegather();
int get_nasmpio_tuneblocking();
int get_nasmpio_pthreadio();
int get_nasmpio_p2pcontig();
int get_nasmpio_write_aggmethod();
int get_nasmpio_read_aggmethod();
int get_nasmpio_onesided_no_rmw();
void set_nasmpio_onesided_no_rmw(int val);
int get_nasmpio_onesided_always_rmw();
void set_nasmpio_onesided_always_rmw(int val);
int get_nasmpio_onesided_inform_rmw();
int get_group_lock_enable();
int get_log_level();
int get_oceanfs_max_size();
int get_oceanfs_io_size_max();
int get_nasmpio_oceanfs();

#endif  /* AD_ENV_H_ */
