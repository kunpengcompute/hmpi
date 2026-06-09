/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2025 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "ompi_config.h"
#include "coll_ucg.h"
#include "coll_ucg_dt.h"
#include "coll_ucg_debug.h"
#include "coll_ucg_request.h"

#include "opal/util/argv.h"

/*
 * Public string showing the coll ompi_ucg component version number
 */
const char *mca_coll_ucg_component_version_string =
  "Open MPI UCG collective MCA component version " OMPI_VERSION;

/*
 * Global variable
 */
int mca_coll_ucg_output = -1;

/*
 * Local function
 */
static int mca_coll_ucg_register(void);
static int mca_coll_ucg_open(void);
static int mca_coll_ucg_close(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

mca_coll_ucg_component_t mca_coll_ucg_component = {
    /* First, fill in the super */
    {
        /* First, the mca_component_t struct containing meta information
           about the component itself */
        .collm_version = {
#if OMPI_MAJOR_VERSION > 4
            MCA_COLL_BASE_VERSION_2_4_0,
#else
            MCA_COLL_BASE_VERSION_2_0_0,
#endif

            /* Component name and version */
            .mca_component_name = "ucg",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                  OMPI_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = mca_coll_ucg_open,
            .mca_close_component = mca_coll_ucg_close,
            .mca_register_component_params = mca_coll_ucg_register,
        },
        .collm_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        /* Initialization / querying functions */
        .collm_init_query = mca_coll_ucg_init_query,
        .collm_comm_query = mca_coll_ucg_comm_query,
    },
    .initialized = false,
    /* MCA parameter */
    .priority = 90,             /* priority */
    .verbose = 2,               /* verbose level */
    .max_rcache_size = 10,
    .disable_coll = NULL,
    .enable_coll = NULL,
    .topology = NULL,
    .npolls = 10,

    .ucg_context = NULL,
    //TODO: More parameters should be added below.
};

static int mca_coll_ucg_register(void)
{
    unsigned long long cpu_id;
    __asm__ volatile ("mrs %0, MIDR_EL1":"=r"(cpu_id));
    unsigned long long vendor = (cpu_id >> 0x18) & 0xFF;
    unsigned long long part_id = (cpu_id >> 0x4) & 0xFFF;
    // Reduce priority of UCG on some CPU archs.
    if ((vendor == 0x48) && (part_id == 0xD22)) {
        mca_coll_ucg_component.priority = 0;
    }

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "priority",
                                          "Priority of the UCG component",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_6,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.priority);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "verbose",
                                          "Verbosity of the UCG component, "
                                          "0:fatal, 1:error, 2:warn, 3:info, 4:debug, >4:fine-grained trace logs",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_LOCAL,
                                          &mca_coll_ucg_component.verbose);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "max_rcache_size",
                                          "Max size of request cache",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_6,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.max_rcache_size);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "disable_coll",
                                          "Comma separated list of collective operations to disable",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.disable_coll);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "enable_coll",
                                          "Comma separated list of collective operations to enable",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.enable_coll);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "enable_coll_cache",
                                          "Comma separated list of collective operations to enable cache",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.enable_coll_cache);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "topology",
                                          "Path of the topology file required by the net-topo-aware algorithm",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.topology);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "npolls",
                                          "Set how many poll counts of ucg progress before opal_progress, "
                                          "can fine tune performance by setting this value, range [1, 100]",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_6,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.npolls);

    return OMPI_SUCCESS;
}

/**
 * @brief Parse the topology file and find the subnet ID corresponding to the rank
 *
 * Temporary solution, which does not consider the overhead of repeatedly
 * opening and traversing files. This solution will be changed later.
 */
static ucg_status_t mca_coll_ucg_get_subnet_id(ucg_rank_t myrank, char *topology,
                                               int32_t *subnet_id)
{
    if (topology == NULL) {
        UCG_DEBUG("No topology file is specified");
        return UCG_ERR_NOT_FOUND;
    }

    FILE *fp = fopen(topology, "r");
    if (fp == NULL) {
        UCG_DEBUG("Topology file %s doesn't seem to exist", topology);
        return UCG_ERR_NOT_FOUND;
    }

    ucg_status_t status = UCG_OK;
    char line[1024];
    ucg_rank_t temp_rank;
    int32_t temp_id;
    while (!feof(fp)) {
        fgets(line, sizeof(line) - 1, fp);
        int rc = sscanf(line, "rank %d subnet_id %d", &temp_rank, &temp_id);
        if (rc != 2) {
            goto err;
        } else if (temp_rank == myrank) {
            *subnet_id = temp_id;
            goto out;
        }
    }
err:
    status = UCG_ERR_INVALID_PARAM;
    UCG_DEBUG("Failed to parse the topology file. Rank %d is not found", myrank);
out:
    fclose(fp);
    return status;
}

static ucg_status_t mca_coll_ucg_set_local_location(ucg_location_t *location)
{
    if (location == NULL) {
        return UCG_ERR_INVALID_PARAM;
    }

    int rc;
    opal_process_name_t proc_name = {
        .jobid = OMPI_PROC_MY_NAME->jobid,
        .vpid = OMPI_PROC_MY_NAME->vpid,
    };
    location->field_mask = 0;
    location->subnet_id = -1;
    location->node_id = -1;
    location->socket_id = -1;

    // get subnet id
    int32_t subnet_id = 0;
    ucg_status_t status;
    status = mca_coll_ucg_get_subnet_id(OMPI_PROC_MY_NAME->vpid,
                                        mca_coll_ucg_component.topology,
                                        &subnet_id);
    if (status == UCG_OK) {
        location->field_mask |= UCG_LOCATION_FIELD_SUBNET_ID;
        location->subnet_id = subnet_id;
    }

    // get node id
    uint32_t node_id = 0;
    uint32_t *pnode_id = &node_id;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, PMIX_NODEID, &proc_name, &pnode_id, PMIX_UINT32);
    if (rc != OPAL_SUCCESS) {
        goto out;
    }
    location->field_mask |= UCG_LOCATION_FIELD_NODE_ID;
    location->node_id = (int32_t)node_id;

    // get socket id
    char *locality = NULL;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, PMIX_LOCALITY_STRING,
                                   &proc_name, &locality, PMIX_STRING);
    if (rc != OPAL_SUCCESS || locality == NULL) {
        goto out;
    }
    char *socket = strstr(locality, "SK");
    if (socket == NULL) {
        goto out_free_locality;
    }
    location->field_mask |= UCG_LOCATION_FIELD_SOCKET_ID;
    location->socket_id = atoi(socket + 2);

out_free_locality:
    free(locality);
out:
    return UCG_OK;
}

/*
 * Call pmix_put for sending my own process infor to pmix server
 */
static int mca_coll_ucg_send_local_proc_info(void)
{
    char rank_addr_identify[32] = {0};
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    const char *mca_type_name = cm->super.collm_version.mca_type_name;
    const char *mca_component_name = cm->super.collm_version.mca_component_name;
    uint32_t jobid = (uint32_t)ORTE_PROC_MY_NAME->jobid;
    uint32_t vpid = (uint32_t)ORTE_PROC_MY_NAME->vpid;
    sprintf(rank_addr_identify, "%s.%s.%u.%u", mca_type_name, mca_component_name, jobid, vpid);

    int rc;
    ucg_proc_info_t *proc = ucg_get_allocated_local_proc_info(cm->ucg_context);
    if (!proc) {
        UCG_ERROR("Failed to get local proc info!");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    mca_coll_ucg_set_local_location(&proc->location);

    uint32_t proc_size = *(uint32_t *)proc;
    UCG_DEBUG("key: %s value: %p (size: %d, location: %d,%d,%d)", rank_addr_identify, proc, proc_size,
        proc->location.subnet_id, proc->location.node_id, proc->location.socket_id);

    OPAL_MODEX_SEND_STRING(rc, OPAL_PMIX_GLOBAL, rank_addr_identify, proc, proc_size);
    ucg_free_proc_info(proc);
    if (rc != OMPI_SUCCESS) {
        return rc;
    }
    return OMPI_SUCCESS;
}

static int mca_coll_ucg_open(void)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    mca_coll_ucg_output = opal_output_open(NULL);
    opal_output_set_verbosity(mca_coll_ucg_output, cm->verbose);

    int rc = mca_coll_ucg_init_once();
    if (rc != OMPI_SUCCESS) {
        return rc;
    }

    rc = mca_coll_ucg_send_local_proc_info();
    if (rc != OMPI_SUCCESS) {
        return rc;
    }

    return OMPI_SUCCESS;
}

static int mca_coll_ucg_close(void)
{
    /* In some cases, mpi_comm_world is not the last comm to free.
     * call mca_coll_ucg_cleanup_once here, ensure cleanup ucg resources at last.
     */
    mca_coll_ucg_cleanup_once();
    return OMPI_SUCCESS;
}
