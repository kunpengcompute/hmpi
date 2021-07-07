## -*- Mode: Makefile; -*-
## vim: set ft=automake :
##
## (C) 2017 by DataDirect Networks
##     See COPYRIGHT in top-level directory.
##

if BUILD_AD_OCEANFS

noinst_HEADERS += adio/ad_oceanfs/ad_oceanfs.h          \
    adio/ad_oceanfs/ad_oceanfs_common.h                 \
    adio/ad_oceanfs/ad_oceanfs_file.h                   \
    adio/ad_oceanfs/ad_oceanfs_pub.h                    \
    adio/ad_oceanfs/ad_oceanfs_tuning.h                 \
    adio/ad_oceanfs/ad_oceanfs_group_tuning.h           \
    adio/ad_oceanfs/ad_oceanfs_aggrs.h                  \
    adio/ad_oceanfs/mpi_fs_intf.h
romio_other_sources +=                                  \
    adio/ad_oceanfs/ad_oceanfs.c                        \
    adio/ad_oceanfs/ad_oceanfs_close.c                  \
    adio/ad_oceanfs/ad_oceanfs_common.c                 \
    adio/ad_oceanfs/ad_oceanfs_fcntl.c                  \
    adio/ad_oceanfs/ad_oceanfs_io.c                     \
    adio/ad_oceanfs/ad_oceanfs_open.c                   \
    adio/ad_oceanfs/ad_oceanfs_file.c                   \
    adio/ad_oceanfs/ad_oceanfs_pub.c                    \
    adio/ad_oceanfs/ad_oceanfs_tuning.c                 \
    adio/ad_oceanfs/ad_oceanfs_group_tuning.c           \
    adio/ad_oceanfs/ad_oceanfs_aggrs.c                  \
    adio/ad_oceanfs/ad_oceanfs_rdstr.c                  \
    adio/ad_oceanfs/ad_oceanfs_rdcoll.c                 \
    adio/ad_oceanfs/ad_oceanfs_wrcoll.c                 \
    adio/ad_oceanfs/ad_oceanfs_resize.c                 \
    adio/ad_oceanfs/ad_oceanfs_hints.c                  \
    adio/ad_oceanfs/ad_oceanfs_view.c                   \
    adio/ad_oceanfs/ad_oceanfs_viewio.c                 \
    adio/ad_oceanfs/mpi_fs_intf.c

endif BUILD_AD_OCEANFS
