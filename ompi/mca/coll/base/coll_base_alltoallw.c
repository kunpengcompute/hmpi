/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2025-2025 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "opal/datatype/opal_convertor_internal.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "coll_base_topo.h"
#include "coll_base_util.h"

int
mca_coll_base_alltoallw_intra_basic_inplace(const void *rbuf, const int *rcounts, const int *rdisps,
                                            struct ompi_datatype_t * const *rdtypes,
                                            struct ompi_communicator_t *comm,
                                            mca_coll_base_module_t *module)
{
    int i, size, rank, left, right, err = MPI_SUCCESS;
    ompi_request_t *req = MPI_REQUEST_NULL;
    char *tmp_buffer = NULL;
    size_t max_size = 0, packed_size, msg_size_left, msg_size_right;
    opal_convertor_t convertor;

    size = ompi_comm_size(comm);
    if (1 == size) {  /* If only one process, we're done. */
        return MPI_SUCCESS;
    }
    rank = ompi_comm_rank(comm);

    /* Find the largest amount of packed send/recv data among all peers where
     * we need to pack before the send.
     */
    for (i = 1 ; i <= (size >> 1) ; ++i) {
        right = (rank + i) % size;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        ompi_proc_t *ompi_proc = ompi_comm_peer_lookup(comm, right);

        if( OPAL_LIKELY(opal_local_arch == ompi_proc->super.proc_convertor->master->remote_arch))  {
            opal_datatype_type_size(&rdtypes[right]->super, &packed_size);
        } else {
            packed_size = opal_datatype_compute_remote_size(&rdtypes[right]->super,
                                                            ompi_proc->super.proc_convertor->master->remote_sizes);
        }
#else
        opal_datatype_type_size(&rdtypes[right]->super, &packed_size);
#endif  /* OPAL_ENABLE_HETEROGENEOUS_SUPPORT */
        packed_size *= rcounts[right];
        max_size = packed_size > max_size ? packed_size : max_size;
    }

    /* Allocate a temporary buffer */
    tmp_buffer = calloc (max_size, 1);
    if (NULL == tmp_buffer) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (i = 1 ; i <= (size >> 1) ; ++i) {
        struct iovec iov = {.iov_base = tmp_buffer, .iov_len = max_size};
        uint32_t iov_count = 1;

        right = (rank + i) % size;
        left  = (rank + size - i) % size;

        ompi_datatype_type_size(rdtypes[right], &msg_size_right);
        msg_size_right *= rcounts[right];

        ompi_datatype_type_size(rdtypes[left], &msg_size_left);
        msg_size_left *= rcounts[left];

        if( 0 != msg_size_right ) {  /* nothing to exchange with the peer on the right */
            ompi_proc_t *right_proc = ompi_comm_peer_lookup(comm, right);
            opal_convertor_clone(right_proc->super.proc_convertor, &convertor, 0);
            opal_convertor_prepare_for_send(&convertor, &rdtypes[right]->super, rcounts[right],
                                            (char *) rbuf + rdisps[right]);
            packed_size = max_size;
            err = opal_convertor_pack(&convertor, &iov, &iov_count, &packed_size);
            if (1 != err) { goto error_hndl; }

            /* Receive data from the right */
            err = MCA_PML_CALL(irecv ((char *) rbuf + rdisps[right], rcounts[right], rdtypes[right],
                                      right, MCA_COLL_BASE_TAG_ALLTOALLW, comm, &req));
            if (MPI_SUCCESS != err) { goto error_hndl; }
        }

        if( (left != right) && (0 != msg_size_left) ) {
            /* Send data to the left */
            err = MCA_PML_CALL(send ((char *) rbuf + rdisps[left], rcounts[left], rdtypes[left],
                                     left, MCA_COLL_BASE_TAG_ALLTOALLW, MCA_PML_BASE_SEND_STANDARD,
                                     comm));
            if (MPI_SUCCESS != err) { goto error_hndl; }

            err = ompi_request_wait (&req, MPI_STATUSES_IGNORE);
            if (MPI_SUCCESS != err) { goto error_hndl; }

            /* Receive data from the left */
            err = MCA_PML_CALL(irecv ((char *) rbuf + rdisps[left], rcounts[left], rdtypes[left],
                                      left, MCA_COLL_BASE_TAG_ALLTOALLW, comm, &req));
            if (MPI_SUCCESS != err) { goto error_hndl; }
        }

        if( 0 != msg_size_right ) {  /* nothing to exchange with the peer on the right */
            /* Send data to the right */
            err = MCA_PML_CALL(send ((char *) tmp_buffer,  packed_size, MPI_PACKED,
                                     right, MCA_COLL_BASE_TAG_ALLTOALLW, MCA_PML_BASE_SEND_STANDARD,
                                     comm));
            if (MPI_SUCCESS != err) { goto error_hndl; }
        }

        err = ompi_request_wait (&req, MPI_STATUSES_IGNORE);
        if (MPI_SUCCESS != err) { goto error_hndl; }
    }

 error_hndl:
    /* Free the temporary buffer */
    free (tmp_buffer);

    /* All done */

    return err;
}


int
ompi_coll_base_alltoallw_intra_pairwise(const void *sbuf, const int *scounts, const int *sdisps,
                                        struct ompi_datatype_t * const *sdtypes,
                                        void *rbuf, const int *rcounts, const int *rdisps,
                                        struct ompi_datatype_t * const *rdtypes,
                                        struct ompi_communicator_t *comm,
                                        mca_coll_base_module_t *module)
{
    int size, rank, err;
    char *psnd, *prcv;
    int sendto, recvfrom, step;
    /* Initialize */
    if (MPI_IN_PLACE == sbuf) {
        return mca_coll_base_alltoallw_intra_basic_inplace(rbuf, rcounts, rdisps,
                                                           rdtypes, comm, module);
    }

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    if (scounts[rank] > 0 || rcounts[rank] > 0) {
        psnd = ((char *) sbuf) + sdisps[rank];
        prcv = ((char *) rbuf) + rdisps[rank];

        err = ompi_datatype_sndrcv(psnd, scounts[rank], sdtypes[rank],
                                   prcv, rcounts[rank], rdtypes[rank]);
        if (MPI_SUCCESS != err) {
            return err;
        }
    }

    if (1 == size) {
        return MPI_SUCCESS;
    }

    ompi_request_t *step_reqs[2];
    MPI_Status step_stats[2];

    for (step = 1; step < size; step++) {
        int nreqs_step = 0;

        sendto = (rank + step) % size;
        recvfrom = (rank + size - step) % size;

        if (rcounts[recvfrom] > 0) {
            prcv = ((char *) rbuf) + rdisps[recvfrom];
            err = MCA_PML_CALL(irecv(prcv, rcounts[recvfrom], rdtypes[recvfrom],
                                    recvfrom, MCA_COLL_BASE_TAG_ALLTOALLW, comm,
                                    &step_reqs[nreqs_step]));
            if (MPI_SUCCESS != err) {
                ompi_coll_base_free_reqs(step_reqs, nreqs_step);
                return err;
            }
            nreqs_step++;
        }

        if (scounts[sendto] > 0) {
            psnd = ((char *) sbuf) + sdisps[sendto];
            err = MCA_PML_CALL(isend(psnd, scounts[sendto], sdtypes[sendto],
                                    sendto, MCA_COLL_BASE_TAG_ALLTOALLW,
                                    MCA_PML_BASE_SEND_STANDARD, comm,
                                    &step_reqs[nreqs_step]));
            if (MPI_SUCCESS != err) {
                ompi_coll_base_free_reqs(step_reqs, nreqs_step);
                return err;
            }
            nreqs_step++;
        }

        if (nreqs_step > 0) {
            err = ompi_request_wait_all(nreqs_step, step_reqs, step_stats);
            if (MPI_SUCCESS != err) {
                ompi_coll_base_free_reqs(step_reqs, nreqs_step);
                return err;
            }

            ompi_coll_base_free_reqs(step_reqs, nreqs_step);
        }
    }

    return MPI_SUCCESS;
}


int
ompi_coll_base_alltoallw_intra_basic_linear(const void *sbuf, const int *scounts, const int *sdisps,
                                            struct ompi_datatype_t * const *sdtypes,
                                            void *rbuf, const int *rcounts, const int *rdisps,
                                            struct ompi_datatype_t * const *rdtypes,
                                            struct ompi_communicator_t *comm,
                                            mca_coll_base_module_t *module)
{
    int i, size, rank, err, nreqs;
    char *psnd, *prcv;
    ompi_request_t **preq, **reqs;
    mca_coll_base_module_t *base_module = (mca_coll_base_module_t*) module;
    mca_coll_base_comm_t *data = base_module->base_data;

    if (MPI_IN_PLACE == sbuf) {
        return mca_coll_base_alltoallw_intra_basic_inplace(rbuf, rcounts, rdisps,
                                                          rdtypes, comm, module);
    }

    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT((ompi_coll_base_framework.framework_output,
                 "coll:base:alltoallw_intra_basic_linear rank %d", rank));

    psnd = ((char *) sbuf) + (ptrdiff_t)sdisps[rank];
    prcv = ((char *) rbuf) + (ptrdiff_t)rdisps[rank];
    if (0 != scounts[rank]) {
        err = ompi_datatype_sndrcv(psnd, scounts[rank], sdtypes[rank],
                                   prcv, rcounts[rank], rdtypes[rank]);
        if (MPI_SUCCESS != err) {
            return err;
        }
    }

    if (1 == size) {
        return MPI_SUCCESS;
    }

    nreqs = 0;
    reqs = preq = ompi_coll_base_comm_get_reqs(data, 2 * (size - 1));
    if (NULL == reqs) {
        err = OMPI_ERR_OUT_OF_RESOURCE;
        goto err_hndl;
    }

    for (i = 0; i < size; ++i) {
        if (i == rank || rcounts[i] <= 0) {
            continue;
        }

        prcv = ((char *) rbuf) + (ptrdiff_t)rdisps[i];
        err = MCA_PML_CALL(irecv(prcv, rcounts[i], rdtypes[i],
                                i, MCA_COLL_BASE_TAG_ALLTOALLW, comm,
                                preq++));
        if (MPI_SUCCESS != err) {
            goto err_hndl;
        }
        nreqs++;
    }

    for (i = 0; i < size; ++i) {
        if (i == rank || scounts[i] <= 0) {
            continue;
        }

        psnd = ((char *) sbuf) + (ptrdiff_t)sdisps[i];
        err = MCA_PML_CALL(isend(psnd, scounts[i], sdtypes[i],
                                i, MCA_COLL_BASE_TAG_ALLTOALLW,
                                MCA_PML_BASE_SEND_STANDARD, comm,
                                preq++));
        if (MPI_SUCCESS != err) {
            goto err_hndl;
        }
        nreqs++;
    }

    if (nreqs > 0) {
        err = ompi_request_wait_all(nreqs, reqs, MPI_STATUSES_IGNORE);
        if (MPI_SUCCESS != err) {
            goto err_hndl;
        }
    } else {
        err = MPI_SUCCESS;
    }

 err_hndl:
    if (MPI_ERR_IN_STATUS == err && nreqs > 0) {
        for (i = 0; i < nreqs; i++) {
            if (MPI_REQUEST_NULL == reqs[i]) continue;
            if (MPI_ERR_PENDING == reqs[i]->req_status.MPI_ERROR) continue;
            if (reqs[i]->req_status.MPI_ERROR != MPI_SUCCESS) {
                err = reqs[i]->req_status.MPI_ERROR;
                break;
            }
        }
    }
    ompi_coll_base_free_reqs(reqs, nreqs);

    return err;
}