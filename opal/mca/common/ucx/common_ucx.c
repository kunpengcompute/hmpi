/*
 * Copyright (C) Mellanox Technologies Ltd. 2018. ALL RIGHTS RESERVED.
 * Copyright (c) 2019      Intel, Inc.  All rights reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2021      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2020      Huawei Technologies Co., Ltd.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "common_ucx.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/mca/pmix/pmix-internal.h"
#include "opal/memoryhooks/memory.h"
#include "opal/util/argv.h"

#include <fnmatch.h>
#include <stdio.h>
#include <ucm/api/ucm.h>

/***********************************************************************/

extern mca_base_framework_t opal_memory_base_framework;

opal_common_ucx_module_t opal_common_ucx = {.verbose = 0,
                                            .progress_iterations = 100,
                                            .registered = 0,
                                            .opal_mem_hooks = 0,
                                            .tls = NULL,
                                            .ref_count = 0,
                                            .first_version = NULL};

static opal_mutex_t opal_common_ucx_mutex = OPAL_MUTEX_STATIC_INIT;

static void opal_common_ucx_mem_release_cb(void *buf, size_t length, void *cbdata, bool from_alloc)
{
    ucm_vm_munmap(buf, length);
}

OPAL_DECLSPEC void opal_common_ucx_mca_var_register(const mca_base_component_t *component)
{
    static const char *default_tls = "rc_verbs,ud_verbs,rc_mlx5,dc_mlx5,cuda_ipc,rocm_ipc";
    static const char *default_devices = "mlx*";
    static int hook_index;
    static int verbose_index;
    static int progress_index;
    static int tls_index;
    static int devices_index;
    int param;

    OPAL_THREAD_LOCK(&opal_common_ucx_mutex);

    param = mca_base_var_find("opal", "opal_common", "ucx", "verbose");
    if (0 > param) {
        verbose_index = mca_base_var_register("opal", "opal_common", "ucx", "verbose",
                                              "Verbose level of the UCX components",
                                              MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                              MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_3,
                                              MCA_BASE_VAR_SCOPE_LOCAL, &opal_common_ucx.verbose);
    }

    param = mca_base_var_find("opal", "opal_common", "ucx", "progress_iterations");
    if (0 > param) {
        progress_index = mca_base_var_register("opal", "opal_common", "ucx", "progress_iterations",
                                               "Set number of calls of internal UCX progress "
                                               "calls per opal_progress call",
                                               MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                               MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_3,
                                               MCA_BASE_VAR_SCOPE_LOCAL,
                                               &opal_common_ucx.progress_iterations);
    }

    param = mca_base_var_find("opal", "opal_common", "ucx", "opal_mem_hooks");
    if (0 > param) {
        hook_index = mca_base_var_register("opal", "opal_common", "ucx", "opal_mem_hooks",
                                           "Use OPAL memory hooks, instead of UCX internal "
                                           "memory hooks",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0, OPAL_INFO_LVL_3,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &opal_common_ucx.opal_mem_hooks);
    }

    param = mca_base_var_find("opal", "opal_common", "ucx", "tls");
    if (0 > param) {

        /*
         * this monkey business is needed because of the way the MCA VARs framework tries to handle pointers to strings
         * when destructing the MCA var database.  If you don't do something like this,the MCA var framework will try
         * to dereference a pointer which itself is no longer a valid address owing to having been previously dlclosed.
         * Same for the devices pointer below.
         */
        if (NULL == opal_common_ucx.tls) {
            opal_common_ucx.tls  = malloc(sizeof(*opal_common_ucx.tls));
            assert(NULL != opal_common_ucx.tls);
        }
        *opal_common_ucx.tls = strdup(default_tls);
        tls_index = mca_base_var_register(
            "opal", "opal_common", "ucx", "tls",
            "List of UCX transports which should be supported on the system, to enable "
            "selecting the UCX component. Special values: any (any available). "
            "A '^' prefix negates the list. "
            "For example, in order to exclude on shared memory and TCP transports, "
            "please set to '^posix,sysv,self,tcp,cma,knem,xpmem'.",
            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_3, MCA_BASE_VAR_SCOPE_LOCAL,
            opal_common_ucx.tls);
    }

    param = mca_base_var_find("opal", "opal_common", "ucx", "devices");
    if (0 > param) {

        if (NULL == opal_common_ucx.devices) {
            opal_common_ucx.devices = malloc(sizeof(*opal_common_ucx.devices));
            assert(NULL != opal_common_ucx.devices);
        }
        *opal_common_ucx.devices = strdup(default_devices);
        devices_index = mca_base_var_register(
            "opal", "opal_common", "ucx", "devices",
            "List of device driver pattern names, which, if supported by UCX, will "
            "bump its priority above ob1. Special values: any (any available)",
            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_3, MCA_BASE_VAR_SCOPE_LOCAL,
            opal_common_ucx.devices);
    }

    if (component) {
        mca_base_var_register_synonym(verbose_index, component->mca_project_name,
                                      component->mca_type_name, component->mca_component_name,
                                      "verbose", 0);
        mca_base_var_register_synonym(progress_index, component->mca_project_name,
                                      component->mca_type_name, component->mca_component_name,
                                      "progress_iterations", 0);
        mca_base_var_register_synonym(hook_index, component->mca_project_name,
                                      component->mca_type_name, component->mca_component_name,
                                      "opal_mem_hooks", 0);
        mca_base_var_register_synonym(tls_index, component->mca_project_name,
                                      component->mca_type_name, component->mca_component_name,
                                      "tls", 0);
        mca_base_var_register_synonym(devices_index, component->mca_project_name,
                                      component->mca_type_name, component->mca_component_name,
                                      "devices", 0);
    }

    OPAL_THREAD_UNLOCK(&opal_common_ucx_mutex);
}

OPAL_DECLSPEC void opal_common_ucx_mca_register(void)
{
    int ret;

    opal_common_ucx.registered++;
    if (opal_common_ucx.registered > 1) {
        /* process once */
        return;
    }

    opal_common_ucx.output = opal_output_open(NULL);
    opal_output_set_verbosity(opal_common_ucx.output, opal_common_ucx.verbose);

    /* Set memory hooks */
    if (opal_common_ucx.opal_mem_hooks) {
        ret = mca_base_framework_open(&opal_memory_base_framework, 0);
        if (OPAL_SUCCESS != ret) {
            /* failed to initialize memory framework - just exit */
            MCA_COMMON_UCX_VERBOSE(1,
                                   "failed to initialize memory base framework: %d, "
                                   "memory hooks will not be used",
                                   ret);
            return;
        }

        if ((OPAL_MEMORY_FREE_SUPPORT | OPAL_MEMORY_MUNMAP_SUPPORT)
            == ((OPAL_MEMORY_FREE_SUPPORT | OPAL_MEMORY_MUNMAP_SUPPORT)
                & opal_mem_hooks_support_level())) {
            MCA_COMMON_UCX_VERBOSE(1, "%s", "using OPAL memory hooks as external events");
            ucm_set_external_event(UCM_EVENT_VM_UNMAPPED);
            opal_mem_hooks_register_release(opal_common_ucx_mem_release_cb, NULL);
        }
    }
}

OPAL_DECLSPEC void opal_common_ucx_mca_deregister(void)
{
    /* unregister only on last deregister */
    opal_common_ucx.registered--;
    assert(opal_common_ucx.registered >= 0);
    if (opal_common_ucx.registered) {
        return;
    }
    opal_mem_hooks_unregister_release(opal_common_ucx_mem_release_cb);
    opal_output_close(opal_common_ucx.output);
}

#if HAVE_DECL_OPEN_MEMSTREAM
static bool opal_common_ucx_check_device(const char *device_name, char **device_list)
{
    char sysfs_driver_link[PATH_MAX];
    char driver_path[PATH_MAX];
    char *ib_device_name;
    char *driver_name;
    char **list_item;
    ssize_t ret;

    /* mlx5_0:1 */
    ret = sscanf(device_name, "%m[^:]%*d", &ib_device_name);
    if (ret != 1) {
        return false;
    }

    sysfs_driver_link[sizeof(sysfs_driver_link) - 1] = '\0';
    snprintf(sysfs_driver_link, sizeof(sysfs_driver_link) - 1,
             "/sys/class/infiniband/%s/device/driver", ib_device_name);
    free(ib_device_name);

    driver_path[sizeof(driver_path) - 1] = '\0';
    ret = readlink(sysfs_driver_link, driver_path, sizeof(driver_path) - 1);
    if (ret < 0) {
        MCA_COMMON_UCX_VERBOSE(2, "readlink(%s) failed: %s", sysfs_driver_link, strerror(errno));
        return false;
    }

    driver_name = basename(driver_path);
    for (list_item = device_list; *list_item != NULL; ++list_item) {
        if (!fnmatch(*list_item, driver_name, 0)) {
            MCA_COMMON_UCX_VERBOSE(2, "driver '%s' matched by '%s'", driver_path, *list_item);
            return true;
        }
    }

    return false;
}
#endif

OPAL_DECLSPEC opal_common_ucx_support_level_t opal_common_ucx_support_level(ucp_context_h context)
{
    opal_common_ucx_support_level_t support_level = OPAL_COMMON_UCX_SUPPORT_NONE;
    static const char *support_level_names[]
        = {[OPAL_COMMON_UCX_SUPPORT_NONE] = "none",
           [OPAL_COMMON_UCX_SUPPORT_TRANSPORT] = "transports only",
           [OPAL_COMMON_UCX_SUPPORT_DEVICE] = "transports and devices"};
#if HAVE_DECL_OPEN_MEMSTREAM
    char *rsc_tl_name, *rsc_device_name;
    char **tl_list, **device_list, **list_item;
    bool is_any_tl, is_any_device;
    bool found_tl, negate;
    char line[128];
    FILE *stream;
    char *buffer;
    size_t size;
    int ret;
#endif

    is_any_tl = !strcmp(*opal_common_ucx.tls, "any");
    is_any_device = !strcmp(*opal_common_ucx.devices, "any");

    /* Check for special value "any" */
    if (is_any_tl && is_any_device) {
        MCA_COMMON_UCX_VERBOSE(1, "ucx is enabled on any transport or device",
                               *opal_common_ucx.tls);
        support_level = OPAL_COMMON_UCX_SUPPORT_DEVICE;
        goto out;
    }

#if HAVE_DECL_OPEN_MEMSTREAM
    /* Split transports list */
    negate = ('^' == (*opal_common_ucx.tls)[0]);
    tl_list = opal_argv_split(*opal_common_ucx.tls + (negate ? 1 : 0), ',');
    if (tl_list == NULL) {
        MCA_COMMON_UCX_VERBOSE(1, "failed to split tl list '%s', ucx is disabled",
                               *opal_common_ucx.tls);
        goto out;
    }

    /* Split devices list */
    device_list = opal_argv_split(*opal_common_ucx.devices, ',');
    if (device_list == NULL) {
        MCA_COMMON_UCX_VERBOSE(1, "failed to split devices list '%s', ucx is disabled",
                               *opal_common_ucx.devices);
        goto out_free_tl_list;
    }

    /* Open memory stream to dump UCX information to */
    stream = open_memstream(&buffer, &size);
    if (stream == NULL) {
        MCA_COMMON_UCX_VERBOSE(1,
                               "failed to open memory stream for ucx info (%s), "
                               "ucx is disabled",
                               strerror(errno));
        goto out_free_device_list;
    }

    /* Print ucx transports information to the memory stream */
    ucp_context_print_info(context, stream);

    /* Rewind and read transports/devices list from the stream */
    fseek(stream, 0, SEEK_SET);
    while ((support_level != OPAL_COMMON_UCX_SUPPORT_DEVICE)
           && (fgets(line, sizeof(line), stream) != NULL)) {
        rsc_tl_name = NULL;
        ret = sscanf(line,
                     /* "# resource 6  :  md 5  dev 4  flags -- rc_verbs/mlx5_0:1" */
                     "# resource %*d : md %*d dev %*d flags -- %m[^/ \n\r]/%m[^/ \n\r]",
                     &rsc_tl_name, &rsc_device_name);
        if (ret != 2) {
            free(rsc_tl_name);
            continue;
        }

        /* Check if 'rsc_tl_name' is found  provided list */
        found_tl = is_any_tl;
        for (list_item = tl_list; !found_tl && (*list_item != NULL); ++list_item) {
            found_tl = !strcmp(*list_item, rsc_tl_name);
        }

        /* Check if the transport has a match (either positive or negative) */
        assert(!(is_any_tl && negate));
        if (found_tl != negate) {
            if (is_any_device || opal_common_ucx_check_device(rsc_device_name, device_list)) {
                MCA_COMMON_UCX_VERBOSE(2, "%s/%s: matched both transport and device list",
                                       rsc_tl_name, rsc_device_name);
                support_level = OPAL_COMMON_UCX_SUPPORT_DEVICE;
            } else {
                MCA_COMMON_UCX_VERBOSE(2, "%s/%s: matched transport list but not device list",
                                       rsc_tl_name, rsc_device_name);
                support_level = OPAL_COMMON_UCX_SUPPORT_TRANSPORT;
            }
        } else {
            MCA_COMMON_UCX_VERBOSE(2, "%s/%s: did not match transport list", rsc_tl_name,
                                   rsc_device_name);
        }

        free(rsc_device_name);
        free(rsc_tl_name);
    }

    MCA_COMMON_UCX_VERBOSE(2, "support level is %s", support_level_names[support_level]);
    fclose(stream);
    free(buffer);

out_free_device_list:
    opal_argv_free(device_list);
out_free_tl_list:
    opal_argv_free(tl_list);
out:
#else
    MCA_COMMON_UCX_VERBOSE(2, "open_memstream() was not found, ucx is disabled");
#endif
    return support_level;
}

void opal_common_ucx_empty_complete_cb(void *request, ucs_status_t status)
{
}

static void opal_common_ucx_mca_fence_complete_cb(int status, void *fenced)
{
    *(int *) fenced = 1;
}

#if HAVE_DECL_UCM_TEST_EVENTS
static ucs_status_t opal_common_ucx_mca_test_external_events(int events)
{
#    if HAVE_DECL_UCM_TEST_EXTERNAL_EVENTS
    return ucm_test_external_events(UCM_EVENT_VM_UNMAPPED);
#    else
    return ucm_test_events(UCM_EVENT_VM_UNMAPPED);
#    endif
}

static void opal_common_ucx_mca_test_events(void)
{
    static int warned = 0;
    const char *suggestion;
    ucs_status_t status;

    if (!warned) {
        if (opal_common_ucx.opal_mem_hooks) {
            suggestion = "Please check OPAL memory events infrastructure.";
            status = opal_common_ucx_mca_test_external_events(UCM_EVENT_VM_UNMAPPED);
        } else {
            suggestion = "Pls try adding --mca opal_common_ucx_opal_mem_hooks 1 "
                         "to mpirun/oshrun command line to resolve this issue.";
            status = ucm_test_events(UCM_EVENT_VM_UNMAPPED);
        }

        if (status != UCS_OK) {
            MCA_COMMON_UCX_WARN("UCX is unable to handle VM_UNMAP event. "
                                "This may cause performance degradation or data "
                                "corruption. %s",
                                suggestion);
            warned = 1;
        }
    }
}
#endif

void opal_common_ucx_mca_proc_added(void)
{
#if HAVE_DECL_UCM_TEST_EVENTS
    opal_common_ucx_mca_test_events();
#endif
}

OPAL_DECLSPEC int opal_common_ucx_mca_pmix_fence_nb(int *fenced)
{
    return PMIx_Fence_nb(NULL, 0, NULL, 0, opal_common_ucx_mca_fence_complete_cb, (void *) fenced);
}

OPAL_DECLSPEC int opal_common_ucx_mca_pmix_fence(ucp_worker_h worker)
{
    volatile int fenced = 0;
    int ret = OPAL_SUCCESS;

    if (OPAL_SUCCESS
        != (ret = PMIx_Fence_nb(NULL, 0, NULL, 0, opal_common_ucx_mca_fence_complete_cb,
                                (void *) &fenced))) {
        return ret;
    }

    while (!fenced) {
        ucp_worker_progress(worker);
    }

    return ret;
}

static void opal_common_ucx_wait_all_requests(void **reqs, int count,
        ucp_worker_h worker, enum opal_common_ucx_req_type type)
{
    int i;

    MCA_COMMON_UCX_VERBOSE(2, "waiting for %d disconnect requests", count);
    for (i = 0; i < count; ++i) {
        opal_common_ucx_wait_request(reqs[i], worker, type, "ucp_disconnect_nb");
        reqs[i] = NULL;
    }
}

OPAL_DECLSPEC int opal_common_ucx_del_procs_nofence(opal_common_ucx_del_proc_t *procs, size_t count,
                                                    size_t my_rank, size_t max_disconnect,
                                                    ucp_worker_h worker)
{
    size_t num_reqs;
    size_t max_reqs;
    void *dreq, **dreqs;
    size_t i;
    size_t n;

    MCA_COMMON_UCX_ASSERT(procs || !count);
    MCA_COMMON_UCX_ASSERT(max_disconnect > 0);

    max_reqs = (max_disconnect > count) ? count : max_disconnect;

    dreqs = malloc(sizeof(*dreqs) * max_reqs);
    if (dreqs == NULL) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    num_reqs = 0;

    for (i = 0; i < count; ++i) {
        n = (i + my_rank) % count;
        if (procs[n].ep == NULL) {
            continue;
        }

        MCA_COMMON_UCX_VERBOSE(2, "disconnecting from rank %zu", procs[n].vpid);
        dreq = ucp_disconnect_nb(procs[n].ep);
        if (dreq != NULL) {
            if (UCS_PTR_IS_ERR(dreq)) {
                MCA_COMMON_UCX_ERROR("ucp_disconnect_nb(%zu) failed: %s", procs[n].vpid,
                                     ucs_status_string(UCS_PTR_STATUS(dreq)));
                continue;
            } else {
                dreqs[num_reqs++] = dreq;
                if (num_reqs >= max_disconnect) {
                    opal_common_ucx_wait_all_requests(dreqs, num_reqs, worker,
                            OPAL_COMMON_UCX_REQUEST_TYPE_UCP);
                    num_reqs = 0;
                }
            }
        }
    }
    /* num_reqs == 0 is processed by opal_common_ucx_wait_all_requests routine,
     * so suppress coverity warning */
    /* coverity[uninit_use_in_call] */
    opal_common_ucx_wait_all_requests(dreqs, num_reqs, worker,
            OPAL_COMMON_UCX_REQUEST_TYPE_UCP);
    free(dreqs);

    return OPAL_SUCCESS;
}

OPAL_DECLSPEC int opal_common_ucx_del_procs(opal_common_ucx_del_proc_t *procs, size_t count,
                                            size_t my_rank, size_t max_disconnect,
                                            ucp_worker_h worker)
{
    opal_common_ucx_del_procs_nofence(procs, count, my_rank, max_disconnect, worker);

    return opal_common_ucx_mca_pmix_fence(worker);
}


#if HAVE_UCP_WORKER_ADDRESS_FLAGS
static int opal_common_ucx_send_worker_address_type(const mca_base_component_t *version,
                                                    int addr_flags, int modex_scope)
{
    ucs_status_t status;
    ucp_worker_attr_t attrs;
    int rc;

    attrs.field_mask    = UCP_WORKER_ATTR_FIELD_ADDRESS |
                          UCP_WORKER_ATTR_FIELD_ADDRESS_FLAGS;
    attrs.address_flags = addr_flags;

    status = ucp_worker_query(opal_common_ucx.ucp_worker, &attrs);
    if (UCS_OK != status) {
        MCA_COMMON_UCX_ERROR("Failed to query UCP worker address");
        return OPAL_ERROR;
    }

    OPAL_MODEX_SEND(rc, modex_scope, version, (void*)attrs.address, attrs.address_length);

    ucp_worker_release_address(opal_common_ucx.ucp_worker, attrs.address);

    if (OPAL_SUCCESS != rc) {
        return OPAL_ERROR;
    }

    MCA_COMMON_UCX_VERBOSE(2, "Pack %s worker address, size %ld",
                    (modex_scope == PMIX_LOCAL) ? "local" : "remote",
                    attrs.address_length);

    return OPAL_SUCCESS;
}
#endif

static int opal_common_ucx_send_worker_address(const mca_base_component_t *version)
{
    ucs_status_t status;

#if !HAVE_UCP_WORKER_ADDRESS_FLAGS
    ucp_address_t *address;
    size_t addrlen;
    int rc;

    status = ucp_worker_get_address(opal_common_ucx.ucp_worker, &address, &addrlen);
    if (UCS_OK != status) {
        MCA_COMMON_UCX_ERROR("Failed to get worker address");
        return OPAL_ERROR;
    }

    MCA_COMMON_UCX_VERBOSE(2, "Pack worker address, size %ld", addrlen);

    OPAL_MODEX_SEND(rc, PMIX_GLOBAL, version, (void*)address, addrlen);

    ucp_worker_release_address(opal_common_ucx.ucp_worker, address);

    if (OPAL_SUCCESS != rc) {
        goto err;
    }
#else
    /* Pack just network device addresses for remote node peers */
    status = opal_common_ucx_send_worker_address_type(version,
                                                      UCP_WORKER_ADDRESS_FLAG_NET_ONLY,
                                                      PMIX_REMOTE);
    if (UCS_OK != status) {
        goto err;
    }

    status = opal_common_ucx_send_worker_address_type(version, 0, PMIX_LOCAL);
    if (UCS_OK != status) {
        goto err;
    }
#endif

    return OPAL_SUCCESS;

err:
    MCA_COMMON_UCX_ERROR("Open MPI couldn't distribute EP connection details");
    return OPAL_ERROR;
}

int opal_common_ucx_recv_worker_address(opal_process_name_t *proc_name,
                                        ucp_address_t **address_p,
                                        size_t *addrlen_p)
{
    int ret;

    const mca_base_component_t *version = opal_common_ucx.first_version;

    *address_p = NULL;
    OPAL_MODEX_RECV(ret, version, proc_name, (void**)address_p, addrlen_p);
    if (ret < 0) {
        MCA_COMMON_UCX_ERROR("Failed to receive UCX worker address: %s (%d)",
                      opal_strerror(ret), ret);
    }

    return ret;
}

int opal_common_ucx_open(const char *prefix,
                         const ucp_params_t *ucp_params,
                         size_t *request_size)
{
    unsigned major_version, minor_version, release_number;
    ucp_context_attr_t attr;
    ucs_status_t status;

    if (opal_common_ucx.first_version != NULL) {
        goto query;
    }

    /* Check version */
    ucp_get_version(&major_version, &minor_version, &release_number);
    MCA_COMMON_UCX_VERBOSE(1, "opal_common_ucx_open: UCX version %u.%u.%u",
                           major_version, minor_version, release_number);

    if ((major_version == 1) && (minor_version == 8)) {
        /* disabled due to issue #8321 */
        MCA_COMMON_UCX_VERBOSE(1, "UCX is disabled because the run-time UCX"
                               " version is 1.8, which has a known catastrophic"
                               " issue");
        return OMPI_ERROR;
    }

    ucp_config_t *config;
    status = ucp_config_read("MPI", NULL, &config);
    if (UCS_OK != status) {
        return OPAL_ERROR;
    }

    status = ucp_init(&ucp_params, config, &opal_common_ucx.ucp_context);
    ucp_config_release(config);

    if (UCS_OK != status) {
        return OPAL_ERROR;
    }

    /* Initialize CUDA, if supported */
    opal_common_ucx.cuda_initialized = false;
#if HAVE_UCP_ATTR_MEMORY_TYPES && OPAL_CUDA_SUPPORT
    if (attr.memory_types & UCS_BIT(UCS_MEMORY_TYPE_CUDA)) {
        mca_common_cuda_stage_one_init();
        opal_common_ucx.cuda_initialized = true;
    }
#endif

query:
    /* Query UCX attributes */
    attr.field_mask  = UCP_ATTR_FIELD_REQUEST_SIZE;
#if HAVE_UCP_ATTR_MEMORY_TYPES
    attr.field_mask |= UCP_ATTR_FIELD_MEMORY_TYPES;
#endif
    status = ucp_context_query(opal_common_ucx.ucp_context, &attr);
    if (UCS_OK != status) {
        goto cleanup_ctx;
    }

    *request_size = attr.request_size;

    return OPAL_SUCCESS;

cleanup_ctx:
    ucp_cleanup(opal_common_ucx.ucp_context);
    return OPAL_ERROR;
}

int opal_common_ucx_close(void)
{
    MCA_COMMON_UCX_VERBOSE(1, "opal_common_ucx_close");

#if OPAL_CUDA_SUPPORT
    if (opal_common_ucx.cuda_initialized) {
        mca_common_cuda_fini();
    }
#endif

    if (opal_common_ucx.ucp_context != NULL) {
        ucp_cleanup(opal_common_ucx.ucp_context);
        opal_common_ucx.ucp_context = NULL;
    }

    return OPAL_SUCCESS;
}

static int opal_common_ucx_init_worker(int enable_mpi_threads)
{
    ucp_worker_params_t params;
    ucp_worker_attr_t attr;
    ucs_status_t status;
    int rc;


    /* TODO check MPI thread mode */
    params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    if (enable_mpi_threads) {
        params.thread_mode = UCS_THREAD_MODE_MULTI;
    } else {
        params.thread_mode = UCS_THREAD_MODE_SINGLE;
    }

    status = ucp_worker_create(opal_common_ucx.ucp_context, &params,
                               &opal_common_ucx.ucp_worker);
    if (UCS_OK != status) {
        MCA_COMMON_UCX_ERROR("Failed to create UCP worker");
        return OPAL_ERROR;
    }

#if HAVE_DECL_UCP_WORKER_FLAG_IGNORE_REQUEST_LEAK
    if (!opal_common_ucx.request_leak_check) {
        params.field_mask |= UCP_WORKER_PARAM_FIELD_FLAGS;
        params.flags      |= UCP_WORKER_FLAG_IGNORE_REQUEST_LEAK;
    }
#endif

    attr.field_mask = UCP_WORKER_ATTR_FIELD_THREAD_MODE;
    status = ucp_worker_query(opal_common_ucx.ucp_worker, &attr);
    if (UCS_OK != status) {
        MCA_COMMON_UCX_ERROR("Failed to query UCP worker thread level");
        rc = OPAL_ERROR;
        goto err_destroy_worker;
    }

    if (enable_mpi_threads && (attr.thread_mode != UCS_THREAD_MODE_MULTI)) {
        /* UCX does not support multithreading, disqualify component for now */
        /* TODO: we should let OMPI to fallback to THREAD_SINGLE mode */
        MCA_COMMON_UCX_WARN("UCP worker does not support MPI_THREAD_MULTIPLE");
        rc = OPAL_ERR_NOT_SUPPORTED;
        goto err_destroy_worker;
    }

    MCA_COMMON_UCX_VERBOSE(2, "created ucp context %p, worker %p",
                           (void *)opal_common_ucx.ucp_context,
                           (void *)opal_common_ucx.ucp_worker);

    return OPAL_SUCCESS;

err_destroy_worker:
    ucp_worker_destroy(opal_common_ucx.ucp_worker);
    return rc;
}

static int opal_common_ucx_progress(void)
{
    return (int) ucp_worker_progress(opal_common_ucx.ucp_worker);
}

int opal_common_ucx_init(int enable_mpi_threads,
                         const mca_base_component_t *version)
{
    int rc;

    if (opal_common_ucx.ref_count++ > 0) {
        return (opal_common_ucx.first_version == NULL) ?
                OPAL_ERROR : OPAL_SUCCESS;
    }

    rc = opal_common_ucx_init_worker(enable_mpi_threads);
    if (rc < 0) {
        return rc;
    }

    /* Early bird gets the worm */
    rc = opal_common_ucx_send_worker_address(version);
    if (rc < 0) {
        MCA_COMMON_UCX_ERROR("Failed to send worker address")
        ucp_worker_destroy(opal_common_ucx.ucp_worker);
    } else {
        opal_common_ucx.first_version = version;
    }

    opal_progress_register(opal_common_ucx_progress);

    return rc;
}

int opal_common_ucx_cleanup(void)
{
    MCA_COMMON_UCX_ASSERT(opal_common_ucx.ref_count > 0);

    if (--opal_common_ucx.ref_count > 0) {
        return OPAL_SUCCESS;
    }

    opal_progress_unregister(opal_common_ucx_progress);

    if (opal_common_ucx.ucp_worker != NULL) {
        ucp_worker_destroy(opal_common_ucx.ucp_worker);
        opal_common_ucx.ucp_worker = NULL;
    }

    return OPAL_SUCCESS;
}
