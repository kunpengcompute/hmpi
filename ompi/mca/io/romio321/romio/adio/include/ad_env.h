#ifndef AD_ENV_H_
#define AD_ENV_H_

#include "adio.h"

void ad_oceanfs_get_env_vars();

/* corresponds to environment variables to select optimizations and timing level */
int get_oceanfsmpio_timing();
void set_oceanfsmpio_timing(int val);
int get_oceanfsmpio_timing_cw_level();
int get_oceanfsmpio_comm();
int get_oceanfsmpio_tunegather();
int get_oceanfsmpio_tuneblocking();
int get_oceanfsmpio_pthreadio();
int get_oceanfsmpio_p2pcontig();
int get_oceanfsmpio_write_aggmethod();
int get_oceanfsmpio_read_aggmethod();
int get_oceanfsmpio_onesided_no_rmw();
void set_oceanfsmpio_onesided_no_rmw(int val);
int get_oceanfsmpio_onesided_always_rmw();
void set_oceanfsmpio_onesided_always_rmw(int val);
int get_oceanfsmpio_onesided_inform_rmw();
int get_group_lock_enable();
int get_log_level();

#endif  /* AD_ENV_H_ */
