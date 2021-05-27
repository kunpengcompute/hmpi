/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011 Mellanox Technologies. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2020      Huawei Technologies Co., Ltd. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdio.h>

#include <dlfcn.h>
#include <libgen.h>

#include "opal/mca/common/ucx/common_ucx.h"
#include "opal/mca/installdirs/installdirs.h"
#include "ompi/op/op.h"
#include "coll_ucx.h"
#include "coll_ucx_request.h"
#include "coll_ucx_datatype.h"


/*
 * Public string showing the coll ompi_hcol component version number
 */
const char *mca_coll_ucx_component_version_string =
  "Open MPI UCX collective MCA component version " OMPI_VERSION;


static int ucx_open(void);
static int ucx_close(void);
static int ucx_register(void);
static int mca_coll_ucx_init_query(bool enable_progress_threads, bool enable_mpi_threads);
static mca_coll_base_module_t *mca_coll_ucx_comm_query(struct ompi_communicator_t *comm,
                                                       int *priority);

int mca_coll_ucx_output = -1;
mca_coll_ucx_component_t mca_coll_ucx_component = {

    /* First, the mca_component_t struct containing meta information
       about the component itfca */
    {
        .collm_version = {
            MCA_COLL_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "ucx",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                  OMPI_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = ucx_open,
            .mca_close_component = ucx_close,
            .mca_register_component_params = ucx_register,
        },
        .collm_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        /* Initialization querying functions */
        .collm_init_query = mca_coll_ucx_init_query,
        .collm_comm_query = mca_coll_ucx_comm_query,
    },
    .priority = 91,  /* priority */
    .verbose = 0,   /* verbose level */
    .num_disconnect = 0,   /* ucx_enable */
    .topo_aware_level = COLL_UCX_TOPO_LEVEL_SOCKET,
    .topo = {
        .level = COLL_UCX_TOPO_LEVEL_ROOT,
        .locs = NULL,
    },
};

static int mca_coll_ucx_send_worker_address(void)
{
    ucp_address_t *address = NULL;
    ucs_status_t status;
    size_t addrlen;
    int rc;

    status = ucp_worker_get_address(mca_coll_ucx_component.ucp_worker, &address, &addrlen);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to get worker address, %s", ucs_status_string(status));
        return OMPI_ERROR;
    }

    OPAL_MODEX_SEND(rc, OPAL_PMIX_GLOBAL, &mca_coll_ucx_component.super.collm_version,
                    (void*)address, addrlen);
    if (OPAL_SUCCESS != rc) {
        COLL_UCX_ERROR("Open MPI couldn't distribute EP connection details");
        return OMPI_ERROR;
    }

    ucp_worker_release_address(mca_coll_ucx_component.ucp_worker, address);

    return OMPI_SUCCESS;
}

static int mca_coll_ucx_recv_worker_address(ompi_proc_t *proc,
                                            ucp_address_t **address_p,
                                            size_t *addrlen_p)
{
    int ret;

    *address_p = NULL;
    OPAL_MODEX_RECV(ret, &mca_coll_ucx_component.super.collm_version,
                    &proc->super.proc_name, (void**)address_p, addrlen_p);
    if (ret != OPAL_SUCCESS) {
        COLL_UCX_ERROR("Failed to receive UCX worker address: %s (%d)", opal_strerror(ret), ret);
    }
    return ret;
}

static ucs_status_t mca_coll_ucx_resolve_address(void *cb_group_obj,
                                                 ucg_group_member_index_t rank,
                                                 ucp_address_t **addr,
                                                 size_t *addr_len)
{
    /* Sanity checks */
    ompi_communicator_t* comm = (ompi_communicator_t*)cb_group_obj;
    if (rank == (ucg_group_member_index_t)comm->c_my_rank) {
        return UCS_ERR_UNSUPPORTED;
    }

    /* Check the cache for a previously established connection to that rank */
    ompi_proc_t *proc_peer = ompi_comm_peer_lookup(comm, rank);
    *addr = proc_peer->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_COLL];
    *addr_len = 1;
    if (*addr != NULL) {
        return UCS_OK;
    }

    /* Obtain the UCP address of the remote */
    int ret = mca_coll_ucx_recv_worker_address(proc_peer, addr, addr_len);
    if (ret < 0) {
        COLL_UCX_ERROR("mca_coll_ucx_recv_worker_address(proc=%d rank=%lu) failed",
                       proc_peer->super.proc_name.vpid, rank);
        return UCS_ERR_INVALID_ADDR;
    }

    /* Cache the connection for future invocations with this rank */
    proc_peer->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_COLL] = *addr;
    return UCS_OK;
}

static void mca_coll_ucx_release_address(ucp_address_t *addr)
{
    /* no need to free - the address is stored in proc_peer->proc_endpoints */
    return;
}

static void mca_coll_ucx_mpi_reduce(void *mpi_op,
                                    void *src,
                                    void *dst,
                                    int count,
                                    void *mpi_dtype)
{
    return ompi_op_reduce((ompi_op_t*)mpi_op, src, dst, count, (ompi_datatype_t*)mpi_dtype);
}

static int mca_coll_ucx_mpi_op_is_commute(void *mpi_op)
{
    return ompi_op_is_commute((ompi_op_t*)mpi_op);
}

static ucg_group_member_index_t mca_coll_ucx_get_global_member_idx(void *cb_group_obj,
                                                                   ucg_group_member_index_t index)
{
    ompi_communicator_t* comm = (ompi_communicator_t*)cb_group_obj;
    struct ompi_proc_t *proc = ompi_comm_peer_lookup(comm, (int)index);
    if (proc == NULL) {
        return (ucg_group_member_index_t)-1;
    }

    unsigned i;
    unsigned member_count = ompi_comm_size(MPI_COMM_WORLD);
    for (i = 0; i < member_count; ++i) {
        struct ompi_proc_t *global_proc = ompi_comm_peer_lookup(MPI_COMM_WORLD, i);
        if (global_proc == proc) {
            return i;
        }
    }

    return (ucg_group_member_index_t)-1;
}

static int mca_coll_ucg_datatype_convert(ompi_datatype_t *mpi_dt,
                                         ucp_datatype_t *ucp_dt)
{
    *ucp_dt = mca_coll_ucx_get_datatype(mpi_dt);
    return 0;
}

static ptrdiff_t coll_ucx_datatype_span(void *dt_ext, int count, ptrdiff_t *gap)
{
    struct ompi_datatype_t *dtype = (struct ompi_datatype_t *)dt_ext;
    ptrdiff_t gp = 0;
    ptrdiff_t dsize = opal_datatype_span(&dtype->super, count, &gp);
    *gap = gp;
    return dsize;
}

static int mca_coll_ucx_init_ucp()
{
    ucp_context_attr_t attr;
    ucp_params_t params;
    ucp_config_t *config = NULL;
    ucs_status_t status;

    /* Read options */
    status = ucp_config_read("MPI", NULL, &config);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to read ucp config, status %d", status);
        return OMPI_ERROR;
    }

    /* Initialize UCX context */
    params.field_mask        = UCP_PARAM_FIELD_FEATURES |
                               UCP_PARAM_FIELD_REQUEST_SIZE |
                               UCP_PARAM_FIELD_REQUEST_INIT |
                               UCP_PARAM_FIELD_REQUEST_CLEANUP |
                               UCP_PARAM_FIELD_MT_WORKERS_SHARED |
                               UCP_PARAM_FIELD_ESTIMATED_NUM_EPS;
    params.features          = UCP_FEATURE_TAG |
                               UCP_FEATURE_RMA |
                               UCP_FEATURE_AMO32 |
                               UCP_FEATURE_AMO64 |
                               UCP_FEATURE_AM;
    params.request_size      = sizeof(ompi_request_t);
    params.request_init      = mca_coll_ucx_request_init;
    params.request_cleanup   = mca_coll_ucx_request_cleanup;
    params.mt_workers_shared = 0; /* we do not need mt support for context
                                     since it will be protected by worker */
    params.estimated_num_eps = ompi_proc_world_size();

    status = ucp_init(&params, config, &mca_coll_ucx_component.ucp_context);
    ucp_config_release(config);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to init ucp context, %s", ucs_status_string(status));
        return OMPI_ERROR;
    }
    /* Query UCX attributes */
    attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
    status = ucp_context_query(mca_coll_ucx_component.ucp_context, &attr);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to query ucp context, %s", ucs_status_string(status));
        goto err_cleanup_ucp_context;
    }
    mca_coll_ucx_component.request_size = attr.request_size;
    int i;
    mca_coll_ucx_component.datatype_attr_keyval = MPI_KEYVAL_INVALID;
    for (i = 0; i < OMPI_DATATYPE_MAX_PREDEFINED; ++i) {
        mca_coll_ucx_component.predefined_types[i] = COLL_UCX_DATATYPE_INVALID;
    }
    return OMPI_SUCCESS;

err_cleanup_ucp_context:
    ucp_cleanup(mca_coll_ucx_component.ucp_context);
    mca_coll_ucx_component.ucp_context = NULL;
    return OMPI_ERROR;
}

static int mca_coll_ucx_init_ucg()
{
    ucs_status_t status;
    ucg_params_t params;

    params.field_mask = UCG_PARAM_FIELD_ADDRESS_CB |
                        UCG_PARAM_FIELD_REDUCE_CB |
                        UCG_PARAM_FIELD_COMMUTE_CB |
                        UCG_PARAM_FIELD_GLOBALIDX_CB |
                        UCG_PARAM_FIELD_DTCONVERT_CB |
                        UCG_PARAM_FIELD_DATATYPESPAN_CB |
                        UCG_PARAM_FIELD_MPI_IN_PLACE;
    params.address.lookup_f = mca_coll_ucx_resolve_address;
    params.address.release_f = mca_coll_ucx_release_address;
    params.mpi_reduce_f = mca_coll_ucx_mpi_reduce;
    params.op_is_commute_f = mca_coll_ucx_mpi_op_is_commute;
    params.mpi_global_idx_f = mca_coll_ucx_get_global_member_idx;
    params.mpi_dt_convert = mca_coll_ucg_datatype_convert;
    params.mpi_datatype_span = coll_ucx_datatype_span;
    params.mpi_in_place = (void*)MPI_IN_PLACE;

    status = ucg_init(&params, NULL, &mca_coll_ucx_component.ucg_context);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to init ucg config, %s", ucs_status_string(status));
        return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}

static int mca_coll_ucx_create_worker()
{
    ucp_worker_params_t params;
    ucs_status_t status;

    params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    if (ompi_mpi_thread_multiple) {
        params.thread_mode = UCS_THREAD_MODE_MULTI;
    } else {
        params.thread_mode = UCS_THREAD_MODE_SINGLE;
    }
    status = ucp_worker_create(mca_coll_ucx_component.ucp_context,
                               &params,
                               &mca_coll_ucx_component.ucp_worker);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to create ucp worker, %s", ucs_status_string(status));
        return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}

static int mca_coll_ucx_check_worker()
{
    ucs_status_t status;
    ucp_worker_attr_t attr;

    attr.field_mask = UCP_WORKER_ATTR_FIELD_THREAD_MODE;
    status = ucp_worker_query(mca_coll_ucx_component.ucp_worker, &attr);
    if (UCS_OK != status) {
        COLL_UCX_ERROR("Failed to query UCP worker thread mode, %s", ucs_status_string(status));
        return OMPI_ERROR;
    }

    /* UCX does not support multithreading, disqualify current PML for now */
    if (ompi_mpi_thread_multiple && (attr.thread_mode != UCS_THREAD_MODE_MULTI)) {
        COLL_UCX_ERROR("UCP worker does not support MPI_THREAD_MULTIPLE");
        return OMPI_ERR_NOT_SUPPORTED;
    }

    return OMPI_SUCCESS;
}

static int mca_coll_ucx_progress(void)
{
    mca_coll_ucx_module_t *module = NULL;
    ucs_list_for_each(module, &mca_coll_ucx_component.group_head, ucs_list) {
        ucg_group_progress(module->ucg_group);
    }
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_init_worker(void)
{
    COLL_UCX_VERBOSE(1, "mca_coll_ucx_init_worker");
    if (OMPI_SUCCESS != mca_coll_ucx_create_worker()) {
        return OMPI_ERROR;
    }

    if (OMPI_SUCCESS != mca_coll_ucx_check_worker()) {
        goto err_destroy_worker;
    }

    /* Share my UCP address, so it could be later obtained via @ref mca_coll_ucx_resolve_address */
    if (OMPI_SUCCESS != mca_coll_ucx_send_worker_address()) {
         goto err_destroy_worker;
    }

    /* Initialize the free lists */
    OBJ_CONSTRUCT(&mca_coll_ucx_component.convs, mca_coll_ucx_freelist_t);
    COLL_UCX_FREELIST_INIT(&mca_coll_ucx_component.convs,
                           mca_coll_ucx_convertor_t,
                           128, -1, 128);

    if (OPAL_SUCCESS != opal_progress_register(mca_coll_ucx_progress)) {
        COLL_UCX_ERROR("Failed to progress register, automatically select other available and highest "
                       "priority collective component.");
        goto err_destroy_worker;
    }

    return OMPI_SUCCESS;

err_destroy_worker:
    ucp_worker_destroy(mca_coll_ucx_component.ucp_worker);
    mca_coll_ucx_component.ucp_worker = NULL;
    return OMPI_ERROR;
}

void mca_coll_ucx_cleanup_worker(void)
{
    COLL_UCX_VERBOSE(1, "mca_coll_ucx_cleanup_worker");

    opal_progress_unregister(mca_coll_ucx_progress);

    OBJ_DESTRUCT(&mca_coll_ucx_component.convs);

    if (mca_coll_ucx_component.ucp_worker != NULL) {
        ucp_worker_destroy(mca_coll_ucx_component.ucp_worker);
        mca_coll_ucx_component.ucp_worker = NULL;
    }

    return;
}

static int mca_coll_ucx_open(void)
{
    COLL_UCX_VERBOSE(1, "mca_coll_ucx_open");

    /* Initialize UCP context */
    if (OMPI_SUCCESS != mca_coll_ucx_init_ucp()) {
        return OMPI_ERROR;
    }

    /* Initialize UCG context */
    if (OMPI_SUCCESS != mca_coll_ucx_init_ucg()) {
        goto err_ucp_cleanup;
    }

    /* Initialize UCP worker */
    if (OMPI_SUCCESS != mca_coll_ucx_init_worker()) {
        goto err_ucg_cleaup;
    }

    ucs_list_head_init(&mca_coll_ucx_component.group_head);
    return OMPI_SUCCESS;

err_ucg_cleaup:
    ucg_cleanup(mca_coll_ucx_component.ucg_context);
    mca_coll_ucx_component.ucg_context = NULL;
err_ucp_cleanup:
    ucp_cleanup(mca_coll_ucx_component.ucp_context);
    mca_coll_ucx_component.ucp_context = NULL;

    return OMPI_ERROR;
}

static int mca_coll_ucx_close(void)
{
    COLL_UCX_VERBOSE(1, "mca_coll_ucx_close");

    int i;
    for (i = 0; i < OMPI_DATATYPE_MAX_PREDEFINED; ++i) {
        if (mca_coll_ucx_component.predefined_types[i] != COLL_UCX_DATATYPE_INVALID) {
            ucp_dt_destroy(mca_coll_ucx_component.predefined_types[i]);
            mca_coll_ucx_component.predefined_types[i] = COLL_UCX_DATATYPE_INVALID;
        }
    }

    mca_coll_ucx_cleanup_worker();
    if (mca_coll_ucx_component.ucg_context != NULL) {
        ucg_cleanup(mca_coll_ucx_component.ucg_context);
        mca_coll_ucx_component.ucg_context = NULL;
    }

    if (mca_coll_ucx_component.ucp_context != NULL) {
        ucp_cleanup(mca_coll_ucx_component.ucp_context);
        mca_coll_ucx_component.ucp_context = NULL;
    }

    mca_coll_ucx_destroy_global_topo();

    return OMPI_SUCCESS;
}

static int ucx_open(void)
{
    mca_coll_ucx_component.output = opal_output_open(NULL);
    opal_output_set_verbosity(mca_coll_ucx_component.output, mca_coll_ucx_component.verbose);

    if (OMPI_SUCCESS != mca_coll_ucx_open()) {
        return OMPI_ERROR;
    }
    opal_common_ucx_mca_register();

    return OMPI_SUCCESS;
}

static int ucx_close(void)
{
    if (mca_coll_ucx_component.ucp_worker == NULL) {
        return OMPI_ERROR;
    }
    opal_common_ucx_mca_deregister();

    return mca_coll_ucx_close();
}

static int ucx_register(void)
{
    int status;
    mca_coll_ucx_component.verbose = 0;
    status = mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version, "verbosity",
                                             "Verbosity of the UCX component",
                                             MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                             OPAL_INFO_LVL_3,
                                             MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_coll_ucx_component.verbose);
    if (status < OPAL_SUCCESS) {
        return OMPI_ERROR;
    }

    mca_coll_ucx_component.priority = 91;
    status = mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version, "priority",
                                             "Priority of the UCX component",
                                             MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                             OPAL_INFO_LVL_3,
                                             MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_coll_ucx_component.priority);
    if (status < OPAL_SUCCESS) {
        return OMPI_ERROR;
    }

    mca_coll_ucx_component.num_disconnect = 1;
    status = mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version, "num_disconnect",
                                             "How may disconnects go in parallel",
                                             MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                             OPAL_INFO_LVL_3,
                                             MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_coll_ucx_component.num_disconnect);
    if (status < OPAL_SUCCESS) {
        return OMPI_ERROR;
    }

    mca_coll_ucx_component.topo_aware_level = COLL_UCX_TOPO_LEVEL_SOCKET;
    status = mca_base_component_var_register(&mca_coll_ucx_component.super.collm_version, "topo_aware_level",
                                             "Topology aware level for ucg",
                                             MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                             OPAL_INFO_LVL_3,
                                             MCA_BASE_VAR_SCOPE_LOCAL,
                                             &mca_coll_ucx_component.topo_aware_level);
    if (status < OPAL_SUCCESS) {
        return OMPI_ERROR;
    }

    opal_common_ucx_mca_var_register(&mca_coll_ucx_component.super.collm_version);
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_init_query(bool enable_progress_threads,
                                   bool enable_mpi_threads)
{
    return OMPI_SUCCESS;
}

static mca_coll_base_module_t *mca_coll_ucx_comm_query(struct ompi_communicator_t *comm, int *priority)
{
   /* basic checks */
    if ((OMPI_COMM_IS_INTER(comm)) || (ompi_comm_size(comm) < 2)) {
        return NULL;
    }

    /* create a new module for this communicator */
    COLL_UCX_VERBOSE(10, "Creating ucx_context for comm %p, comm_id %d, comm_size %d",
                     (void*)comm, comm->c_contextid, ompi_comm_size(comm));
    mca_coll_ucx_module_t *ucx_module = OBJ_NEW(mca_coll_ucx_module_t);
    if (!ucx_module) {
        return NULL;
    }

    *priority = mca_coll_ucx_component.priority;
    return &(ucx_module->super);
}

ucp_worker_h mca_coll_ucx_get_component_worker()
{
    return mca_coll_ucx_component.ucp_worker;
}
