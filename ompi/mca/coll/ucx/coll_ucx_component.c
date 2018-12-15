/*
 * Copyright (c) 2011 Mellanox Technologies. All rights reserved.
 * Copyright (c) 2015 Los Alamos National Security, LLC. All rights reserved.
 * Copyright (c) 2019 Huawei Technologies Co., Ltd. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "coll_ucx.h"

static int mca_coll_ucx_component_open(void);
static int mca_coll_ucx_component_close(void);
static int mca_coll_ucx_component_register(void);
int mca_coll_ucx_init_query(bool enable_progress_threads,
                            bool enable_mpi_threads);
mca_coll_base_module_t *
mca_coll_ucx_comm_query(struct ompi_communicator_t *comm, int *priority);

mca_coll_ucx_component_t mca_coll_ucx_component = {

    /* First, the mca_component_t struct containing meta information
       about the component itfca */
    {
        .collm_version = {
            MCA_COLL_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "ucx",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION,
                                  OMPI_MINOR_VERSION, OMPI_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component            = mca_coll_ucx_component_open,
            .mca_close_component           = mca_coll_ucx_component_close,
            .mca_query_component           = NULL,
            .mca_register_component_params = mca_coll_ucx_component_register,
        },
        .collm_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        /* Initialization / querying functions */

        .collm_init_query = mca_coll_ucx_init_query,
        .collm_comm_query = mca_coll_ucx_comm_query,
    },
    91,  /* priority */
    0    /* stable-reduce */
};

/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this component to disqualify itself if it doesn't support the
 * required level of thread support.
 */
int mca_coll_ucx_init_query(bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    return OMPI_SUCCESS;
}

/*
 * Invoked when there's a new communicator that has been created.
 * Look at the communicator and decide which set of functions and
 * priority we want to return.
 */
mca_coll_base_module_t *
mca_coll_ucx_comm_query(struct ompi_communicator_t *comm, int *priority)
{
    /* basic checks */
    if ((OMPI_COMM_IS_INTER(comm)) || (ompi_comm_size(comm) < 2)) {
        return NULL;
    }

    /* create a new module for this communicator */
    COLL_UCX_VERBOSE(10,"Creating ucx_context for comm %p, comm_id %d, comm_size %d",
                 (void*)comm, comm->c_contextid, ompi_comm_size(comm));
    mca_coll_ucx_module_t *ucx_module = OBJ_NEW(mca_coll_ucx_module_t);
    if (!ucx_module) {
        return NULL;
    }

    *priority = mca_coll_ucx_component.priority;
    return &(ucx_module->super);
}

static int mca_coll_ucx_component_register(void)
{
    mca_coll_ucx_component.priority = 91;
    (void) mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version,
                                           "priority",
                                           "Priority of the UCX component",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_3,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_coll_ucx_component.priority);

    mca_coll_ucx_component.stable_reduce = false;
    (void) mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version,
                                           "stable_reduce",
                                           "Whether to do stable reductions",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_1,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_coll_ucx_component.stable_reduce);

    mca_coll_ucx_component.stable_reduce = false;
    (void) mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version,
                                           "stable_reduce",
                                           "Whether to do stable reductions",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_1,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_coll_ucx_component.stable_reduce);

    opal_common_ucx_mca_var_register(&mca_coll_ucx_component.super.collm_version);
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_component_open(void)
{
    int rc;
    size_t dummy;

    COLL_UCX_VERBOSE(1, "mca_coll_ucx_component_open");

    opal_common_ucx_mca_register();

    rc = mca_common_ucx_open("MPI", &dummy);
    if (rc < 0) {
        return rc;
    }

    rc = mca_common_ucx_init(&mca_coll_ucx_component.super.collm_version);
    if (rc < 0) {
        mca_common_ucx_close();
    }

    return rc;
}

static int mca_coll_ucx_component_close(void)
{
    int rc;

    COLL_UCX_VERBOSE(1, "mca_coll_ucx_component_close");

    rc = mca_common_ucx_close();

    opal_common_ucx_mca_deregister();

    return rc;
}
