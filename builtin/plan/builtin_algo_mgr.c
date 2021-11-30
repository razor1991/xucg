/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021.  All rights reserved.
 * Description: Managing open and closed source algorithms
 * Author: liangjiakun
 * Create: 2021-09-06
 */

#include <ucg/api/ucg.h>
#include "builtin_algo_mgr.h"

ucg_builtin_algo_pool_t ucg_builtin_algo_manager;
ucg_inc_func_t ucg_inc;

ucs_status_t ucg_builtin_algo_find(int type, int id, ucg_builtin_coll_algo_h *algo)
{
    UCS_MODULE_FRAMEWORK_DECLARE(ucg);
    UCS_MODULE_FRAMEWORK_LOAD(ucg, 0);

    *algo = NULL;
    switch (type) {
        case COLL_TYPE_BARRIER:
            ucs_assert(id < UCG_ALGORITHM_BARRIER_LAST);
            *algo = ucg_builtin_algo_manager.barrier_algos[id];
            break;
        case COLL_TYPE_BCAST:
            ucs_assert(id < UCG_ALGORITHM_BCAST_LAST);
            *algo = ucg_builtin_algo_manager.bcast_algos[id];
            break;
        case COLL_TYPE_ALLREDUCE:
            ucs_assert(id < UCG_ALGORITHM_ALLREDUCE_LAST);
            *algo = ucg_builtin_algo_manager.allreduce_algos[id];
            break;
        case COLL_TYPE_ALLTOALLV:
            ucs_assert(id < UCG_ALGORITHM_ALLTOALLV_LAST);
            *algo = ucg_builtin_algo_manager.alltoallv_algos[id];
            break;
        default:
            ucs_error("The current type [%d] is not supported", type);
            break;
    }
    return *algo != NULL ? UCS_OK : UCS_ERR_NO_ELEM;
}