/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2022 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "coll_ucg_dt.h"
#include "coll_ucg_debug.h"

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/attribute/attribute.h"

static mca_coll_ucg_type_table_t ucg_type_table = {.attr_key = MPI_KEYVAL_INVALID};
static mca_coll_ucg_conv_pool_t mca_coll_ucg_conv_pool;

static ucg_status_t mca_coll_ucg_op_reduce(void *op, const void *source, void *target,
                                           int32_t count, void *dt)
{
    ompi_op_t *ompi_op = (ompi_op_t*)op;
    ompi_datatype_t *ompi_dt = (ompi_datatype_t*)dt;
    ompi_op_reduce(ompi_op, (void*)source, target, count, ompi_dt);
    return UCG_OK;
}

static inline mca_coll_ucg_convertor_t* mca_coll_ucg_conv_pool_get(void)
{
    return (mca_coll_ucg_convertor_t *)opal_free_list_wait(&mca_coll_ucg_conv_pool.flist);
}

static inline void mca_coll_ucg_conv_pool_put(mca_coll_ucg_convertor_t *conv)
{
    opal_free_list_return(&mca_coll_ucg_conv_pool.flist, (opal_free_list_item_t*)conv);
    return;
}

static void* mca_coll_ucg_conv_start_pack(const void *user_buf, void *user_dt, int32_t count)
{
    ompi_datatype_t *datatype = (ompi_datatype_t *)user_dt;
    mca_coll_ucg_convertor_t *convertor;

    convertor = mca_coll_ucg_conv_pool_get();
    OMPI_DATATYPE_RETAIN(datatype);
    convertor->datatype = datatype;
    int rc = opal_convertor_copy_and_prepare_for_send(ompi_mpi_local_convertor,
                                                      &datatype->super,
                                                      count, user_buf, 0,
                                                      &convertor->opal_conv);
    if (rc != OPAL_SUCCESS) {
        OMPI_DATATYPE_RELEASE(datatype);
        mca_coll_ucg_conv_pool_put(convertor);
        convertor = NULL;
    }
    return convertor;
}

static ucg_status_t mca_coll_ucg_conv_pack(void *conv, uint64_t offset, void *buffer, uint64_t *length)
{
    mca_coll_ucg_convertor_t *convertor = (mca_coll_ucg_convertor_t *)conv;
    uint32_t iov_count;
    struct iovec iov;
    size_t pack_length;
    int rc;

    iov_count    = 1;
    iov.iov_base = buffer;
    iov.iov_len  = *length;

    opal_convertor_set_position(&convertor->opal_conv, &offset);
    pack_length = *length;
    rc = opal_convertor_pack(&convertor->opal_conv, &iov, &iov_count, &pack_length);
    if (OPAL_UNLIKELY(rc < 0)) {
        UCG_ERROR("Failed to pack datatype structure");
        return UCG_ERR_NO_RESOURCE;
    }

    *length = pack_length;
    return UCG_OK;
}

static void* mca_coll_ucg_conv_start_unpack(void *user_buf, void *user_dt, int32_t count)
{
    ompi_datatype_t *datatype = (ompi_datatype_t *)user_dt;
    mca_coll_ucg_convertor_t *convertor;

    convertor = mca_coll_ucg_conv_pool_get();
    OMPI_DATATYPE_RETAIN(datatype);
    convertor->datatype = datatype;
    convertor->offset = 0;
    int rc = opal_convertor_copy_and_prepare_for_recv(ompi_mpi_local_convertor,
                                                      &datatype->super,
                                                      count, user_buf, 0,
                                                      &convertor->opal_conv);
    if (rc != OPAL_SUCCESS) {
        OMPI_DATATYPE_RELEASE(datatype);
        mca_coll_ucg_conv_pool_put(convertor);
        convertor = NULL;
    }
    return convertor;
}

static ucg_status_t mca_coll_ucg_conv_unpack(void *conv, uint64_t offset, const void *buffer, uint64_t *length)
{
    mca_coll_ucg_convertor_t *convertor = (mca_coll_ucg_convertor_t *)conv;
    int rc;
    uint32_t iov_count;
    uint64_t unpack_length;
    struct iovec iov;
    opal_convertor_t opal_conv;

    iov_count    = 1;
    iov.iov_base = (void*)buffer;
    iov.iov_len  = *length;

    /* in case if unordered message arrived - create separate convertor to
     * unpack data. */
    if (offset != convertor->offset) {
        OBJ_CONSTRUCT(&opal_conv, opal_convertor_t);
        opal_convertor_copy_and_prepare_for_recv(ompi_mpi_local_convertor,
                                                 &convertor->datatype->super,
                                                 convertor->opal_conv.count,
                                                 convertor->opal_conv.pBaseBuf, 0,
                                                 &opal_conv);
        opal_convertor_set_position(&opal_conv, &offset);
        rc = opal_convertor_unpack(&opal_conv, &iov, &iov_count, &unpack_length);
        opal_convertor_cleanup(&opal_conv);
        OBJ_DESTRUCT(&opal_conv);
        /* permanently switch to un-ordered mode */
        convertor->offset = 0;
    } else {
        rc = opal_convertor_unpack(&convertor->opal_conv, &iov, &iov_count, &unpack_length);
        convertor->offset += unpack_length;
    }
    if (OPAL_UNLIKELY(rc < 0)) {
        UCG_ERROR("Failed to unpack datatype structure");
        return UCG_ERR_NO_RESOURCE;
    }

    *length = unpack_length;
    return UCG_OK;
}

static void mca_coll_ucg_conv_finish(void *conv)
{
    mca_coll_ucg_convertor_t *convertor = (mca_coll_ucg_convertor_t *)conv;

    opal_convertor_cleanup(&convertor->opal_conv);
    OMPI_DATATYPE_RELEASE(convertor->datatype);
    mca_coll_ucg_conv_pool_put(convertor);
}

static void mca_coll_ucg_convertor_construct(mca_coll_ucg_convertor_t *convertor)
{
    OBJ_CONSTRUCT(&convertor->opal_conv, opal_convertor_t);
}

static void mca_coll_ucg_convertor_destruct(mca_coll_ucg_convertor_t *convertor)
{
    OBJ_DESTRUCT(&convertor->opal_conv);
}

OBJ_CLASS_INSTANCE(mca_coll_ucg_convertor_t,
                   opal_free_list_item_t,
                   mca_coll_ucg_convertor_construct,
                   mca_coll_ucg_convertor_destruct);

static inline ucg_dt_type_t ompi_dt_2_ucg_dt_type(ompi_datatype_t *ompi_dt)
{
    switch (ompi_dt->id) {
        case OMPI_DATATYPE_MPI_INT8_T:
            return UCG_DT_TYPE_INT8;
        case OMPI_DATATYPE_MPI_INT16_T:
            return UCG_DT_TYPE_INT16;
        case OMPI_DATATYPE_MPI_INT32_T:
            return UCG_DT_TYPE_INT32;
        case OMPI_DATATYPE_MPI_INT64_T:
            return UCG_DT_TYPE_INT64;

        case OMPI_DATATYPE_MPI_UINT8_T:
            return UCG_DT_TYPE_UINT8;
        case OMPI_DATATYPE_MPI_UINT16_T:
            return UCG_DT_TYPE_UINT16;
        case OMPI_DATATYPE_MPI_UINT32_T:
            return UCG_DT_TYPE_UINT32;
        case OMPI_DATATYPE_MPI_UINT64_T:
            return UCG_DT_TYPE_UINT64;
        case OMPI_DATATYPE_MPI_SHORT_FLOAT:
            return UCG_DT_TYPE_FP16;
        case OMPI_DATATYPE_MPI_FLOAT:
            return UCG_DT_TYPE_FP32;
        case OMPI_DATATYPE_MPI_DOUBLE:
            return UCG_DT_TYPE_FP64;

        default:
            return UCG_DT_TYPE_USER;
    }
}

static inline ucg_op_type_t ompi_op_2_ucg_op_type(ompi_op_t *ompi_op)
{
    switch (ompi_op->op_type) {
        case OMPI_OP_MAX:
            return UCG_OP_TYPE_MAX;
        case OMPI_OP_MIN:
            return UCG_OP_TYPE_MIN;
        case OMPI_OP_SUM:
            return UCG_OP_TYPE_SUM;
        case OMPI_OP_PROD:
            return UCG_OP_TYPE_PROD;
        default:
            return UCG_OP_TYPE_USER;
    }
}

static int mca_coll_ucg_type_destroy_user_dt(ompi_datatype_t* datatype, int keyval,
                                             void *attr_val, void *extra)
{
    ucg_dt_h ucg_dt = (ucg_dt_h)attr_val;
    ucg_dt_destroy(ucg_dt);
    return OMPI_SUCCESS;
}

static int mca_coll_ucg_type_create_user_dt(ompi_datatype_t *ompi_dt, ucg_dt_h *ucg_dt)
{
    size_t size;
    ompi_datatype_type_size(ompi_dt, &size);
    ptrdiff_t lb;
    ptrdiff_t extent;
    ptrdiff_t true_lb;
    ptrdiff_t true_extent;
    ompi_datatype_get_extent(ompi_dt, &lb, &extent);
    ompi_datatype_get_true_extent(ompi_dt, &true_lb, &true_extent);
    ucg_dt_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE |
                        UCG_DT_PARAMS_FIELD_USER_DT |
                        UCG_DT_PARAMS_FIELD_SIZE |
                        UCG_DT_PARAMS_FIELD_EXTENT |
                        UCG_DT_PARAMS_FIELD_TRUE_LB |
                        UCG_DT_PARAMS_FIELD_TRUE_EXTENT;
    params.type = UCG_DT_TYPE_USER;
    params.user_dt = ompi_dt;
    params.size = (uint32_t)size;
    params.extent = (uint32_t)extent;
    params.true_lb = (int32_t)true_lb;
    params.true_extent = (uint32_t)true_extent;
    if (size != (size_t)extent) {
        params.field_mask |= UCG_DT_PARAMS_FIELD_CONV;
        params.conv.start_pack = mca_coll_ucg_conv_start_pack;
        params.conv.pack = mca_coll_ucg_conv_pack;
        params.conv.start_unpack = mca_coll_ucg_conv_start_unpack;
        params.conv.unpack = mca_coll_ucg_conv_unpack;
        params.conv.finish = mca_coll_ucg_conv_finish;
    }
    ucg_status_t status = ucg_dt_create(&params, ucg_dt);
    if (status != UCG_OK) {
        UCG_ERROR("Failed to create user-defined dt");
        return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}

void mca_coll_ucg_type_free_kv(void)
{
    if (ucg_type_table.attr_key != MPI_KEYVAL_INVALID) {
        ompi_attr_free_keyval(TYPE_ATTR, &ucg_type_table.attr_key, false);
        ucg_type_table.attr_key = MPI_KEYVAL_INVALID;
    }
}

static void mca_coll_ucg_type_destroy_dt(void)
{
    ucg_dt_type_t type = UCG_DT_TYPE_INT8;
    for (; type < UCG_DT_TYPE_PREDEFINED_LAST; ++type) {
        if (ucg_type_table.predefined_dt[type] != NULL) {
            ucg_dt_destroy(ucg_type_table.predefined_dt[type]);
            ucg_type_table.predefined_dt[type] = NULL;
        }
    }

    return;
}

static int mca_coll_ucg_type_create_dt(void)
{
    ucg_dt_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE;
    ucg_dt_type_t type = UCG_DT_TYPE_INT8;
    for (; type < UCG_DT_TYPE_PREDEFINED_LAST; ++type) {
        params.type = type;
        ucg_status_t status = ucg_dt_create(&params, &ucg_type_table.predefined_dt[type]);
        if (status != UCG_OK) {
            goto err_destroy_dt;
        }
    }

    /* Create a key for adding user-defined ucg_dt to ompi_dt */
    ompi_attribute_fn_ptr_union_t copy_fn;
    ompi_attribute_fn_ptr_union_t del_fn;
    copy_fn.attr_datatype_copy_fn = (MPI_Type_internal_copy_attr_function*)MPI_TYPE_NULL_COPY_FN;
    del_fn.attr_datatype_delete_fn = mca_coll_ucg_type_destroy_user_dt;
    int rc = ompi_attr_create_keyval(TYPE_ATTR, copy_fn, del_fn,
                                     &ucg_type_table.attr_key, NULL, 0,
                                     NULL);
    if (rc != OMPI_SUCCESS) {
        UCG_ERROR("Failed to create keyval: %d", rc);
        goto err_destroy_dt;
    }
    return OMPI_SUCCESS;

err_destroy_dt:
    mca_coll_ucg_type_destroy_dt();
    return OMPI_ERROR;
}

static void mca_coll_ucg_type_destroy_op(void)
{
    ucg_op_type_t type = UCG_OP_TYPE_MAX;
    for (; type < UCG_OP_TYPE_PREDEFINED_LAST; ++type) {
        if (ucg_type_table.predefined_op[type] != NULL) {
            ucg_op_destroy(ucg_type_table.predefined_op[type]);
            ucg_type_table.predefined_op[type] = NULL;
        }
    }

    return;
}

static int mca_coll_ucg_type_create_op(void)
{
    ucg_op_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE;
    ucg_op_type_t type = UCG_OP_TYPE_MAX;
    for (; type < UCG_OP_TYPE_PREDEFINED_LAST; ++type) {
        params.type = type;
        ucg_status_t status = ucg_op_create(&params, &ucg_type_table.predefined_op[type]);
        if (status != UCG_OK) {
            goto err_destroy_op;
        }
    }
    return OMPI_SUCCESS;

err_destroy_op:
    mca_coll_ucg_type_destroy_op();
    return OMPI_ERROR;
}

static int mca_coll_ucg_type_adapt_dt(ompi_datatype_t *ompi_dt,
                                      ucg_dt_type_t type,
                                      ucg_dt_h *ucg_dt)
{
    if (type != UCG_DT_TYPE_USER) {
        *ucg_dt = ucg_type_table.predefined_dt[type];
        return OMPI_SUCCESS;
    }

    int rc;
    int found = 0;
    rc = ompi_attr_get_c(ompi_dt->d_keyhash, ucg_type_table.attr_key, (void**)ucg_dt, &found);
    if (rc == OMPI_SUCCESS && found) {
        return OMPI_SUCCESS;
    }

    rc = mca_coll_ucg_type_create_user_dt(ompi_dt, ucg_dt);
    if (rc != OMPI_SUCCESS) {
        return rc;
    }

    rc = ompi_attr_set_c(TYPE_ATTR, ompi_dt, &ompi_dt->d_keyhash,
                         ucg_type_table.attr_key, (void*)*ucg_dt, false);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_type_destroy_user_dt(ompi_dt, ucg_type_table.attr_key, (void*)*ucg_dt, NULL);
        UCG_ERROR("Failed to add UCG datatype attribute for %s: %d", ompi_dt->name, rc);
        return rc;
    }
    return OMPI_SUCCESS;
}

static int mca_coll_ucg_type_adapt_op(ompi_op_t *ompi_op,
                                      ucg_op_type_t type,
                                      ucg_op_h *ucg_op)
{
    if (type != UCG_OP_TYPE_USER) {
        *ucg_op = ucg_type_table.predefined_op[type];
        return OMPI_SUCCESS;
    }
    /* *ucg_op should point to a memory space of size UCG_OP_SIZE */
    assert(*ucg_op != NULL);
    ucg_op_params_t params;
    params.field_mask = UCG_OP_PARAMS_FIELD_TYPE |
                        UCG_OP_PARAMS_FIELD_USER_OP |
                        UCG_OP_PARAMS_FIELD_USER_FUNC |
                        UCG_OP_PARAMS_FIELD_COMMUTATIVE;
    params.type = type;
    params.user_op = (void*)ompi_op;
    params.user_func = mca_coll_ucg_op_reduce;
    params.commutative = ompi_op_is_commute(ompi_op);
    ucg_status_t status = ucg_op_init(&params, *ucg_op, UCG_OP_SIZE);
    if (status != UCG_OK) {
        UCG_ERROR("Failed to initialize ucg op: %d", status);
        return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}

int mca_coll_ucg_conv_pool_init(void)
{
    OBJ_CONSTRUCT(&mca_coll_ucg_conv_pool.flist, opal_free_list_t);
    int rc = opal_free_list_init(&mca_coll_ucg_conv_pool.flist, sizeof(mca_coll_ucg_convertor_t),
                                 opal_cache_line_size, OBJ_CLASS(mca_coll_ucg_convertor_t),
                                 0, 0,
                                 128, INT_MAX, 128,
                                 NULL, 0, NULL, NULL, NULL);
    return rc == OPAL_SUCCESS ? OMPI_SUCCESS : OMPI_ERROR;
}

void mca_coll_ucg_conv_pool_cleanup(void)
{
    OBJ_DESTRUCT(&mca_coll_ucg_conv_pool.flist);
    return;
}

int mca_coll_ucg_type_init(void)
{
    int rc;
    rc = mca_coll_ucg_type_create_dt();
    if (rc != OMPI_SUCCESS) {
        return rc;
    }

    rc = mca_coll_ucg_type_create_op();
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_type_destroy_dt();
        return rc;
    }

    return OMPI_SUCCESS;
}

void mca_coll_ucg_type_cleanup(void)
{
    mca_coll_ucg_type_destroy_op();
    mca_coll_ucg_type_destroy_dt();
    return;
}

int mca_coll_ucg_type_adapt(ompi_datatype_t *ompi_dt, ucg_dt_h *ucg_dt,
                            ompi_op_t *ompi_op, ucg_op_h *ucg_op)
{
    if (!ompi_datatype_is_valid(ompi_dt)) {
        return OMPI_ERR_NOT_SUPPORTED;
    }

    ucg_dt_type_t dt_type = ompi_dt_2_ucg_dt_type(ompi_dt);
    if (ompi_op == NULL) {
        return mca_coll_ucg_type_adapt_dt(ompi_dt, dt_type, ucg_dt);
    }

    /* Both the dt_type and op_type must be predefined or user-defined.*/
    ucg_op_type_t op_type = ompi_op_2_ucg_op_type(ompi_op);
    if (dt_type == UCG_DT_TYPE_USER || op_type == UCG_OP_TYPE_USER) {
        dt_type = UCG_DT_TYPE_USER;
        op_type = UCG_OP_TYPE_USER;
    }

    int rc = mca_coll_ucg_type_adapt_op(ompi_op, op_type, ucg_op);
    if (rc != OMPI_SUCCESS) {
        return rc;
    }
    return mca_coll_ucg_type_adapt_dt(ompi_dt, dt_type, ucg_dt);
}