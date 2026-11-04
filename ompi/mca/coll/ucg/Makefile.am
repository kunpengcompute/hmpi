#
# Copyright (c) 2022-2022 Huawei Technologies Co., Ltd.
#                         All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

AM_CPPFLAGS = $(coll_ucg_CPPFLAGS) -DCOLL_UCG_HOME=\"$(coll_ucg_HOME)\" $(coll_ucg_extra_CPPFLAGS)

#dist_ompidata_DATA = help-coll-ucg.txt
coll_ucg_sources = \
        coll_ucg.h \
        coll_ucg_debug.h \
        coll_ucg_request.h \
        coll_ucg_dt.h \
        coll_ucg_component.c \
        coll_ucg_module.c \
        coll_ucg_request.c \
        coll_ucg_dt.c \
        coll_ucg_allreduce.c \
        coll_ucg_barrier.c \
        coll_ucg_bcast.c \
        coll_ucg_alltoallv.c \
        coll_ucg_scatter.c \
        coll_ucg_scatterv.c \
        coll_ucg_gather.c \
        coll_ucg_gatherv.c \
        coll_ucg_allgatherv.c \
        coll_ucg_reduce_scatter.c \
        coll_ucg_reduce_scatter_block.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_ompi_coll_ucg_DSO
component_noinst =
component_install = mca_coll_ucg.la
else
component_noinst = libmca_coll_ucg.la
component_install =
endif

mcacomponentdir = $(ompilibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_coll_ucg_la_SOURCES = $(coll_ucg_sources)
mca_coll_ucg_la_LIBADD = $(coll_ucg_LIBS)
mca_coll_ucg_la_LDFLAGS = -module -avoid-version $(coll_ucg_LDFLAGS)

noinst_LTLIBRARIES = $(component_noinst)
libmca_coll_ucg_la_SOURCES =$(coll_ucg_sources)
libmca_coll_ucg_la_LIBADD = $(coll_ucg_LIBS)
libmca_coll_ucg_la_LDFLAGS = -module -avoid-version $(coll_ucg_LDFLAGS)
