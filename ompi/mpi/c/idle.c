/*
 * Copyright (c) 2021      Huawei Technologies Co., Ltd. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"

#include MCA_timer_IMPLEMENTATION_HEADER
#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Register_idle_progress = PMPI_Register_idle_progress
#pragma weak MPI_Deregister_idle_progress = PMPI_Deregister_idle_progress
#endif
#define MPI_Register_idle_progress PMPI_Register_idle_progress
#define MPI_Deregister_idle_progress PMPI_Deregister_idle_progress
#endif

int MPI_Register_idle_progress(MPI_Idle_progress_function idle_progress_fn,
                               void *arg, unsigned *idle_index)
{
    return opal_progress_register_idle(idle_progress_fn, arg, idle_index);
}

int MPI_Deregister_idle_progress(unsigned idle_index)
{
    return opal_progress_unregister_idle(idle_index);
}
