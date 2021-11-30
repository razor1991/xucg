/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021.  All rights reserved.
 * Description: Managing open and closed source algorithms
 * Author: liangjiakun
 * Create: 2021-09-06
 */

#ifndef UCG_BUILTIN_ALGO_MGMT_H
#define UCG_BUILTIN_ALGO_MGMT_H

#include <ucs/sys/module.h>
#include <ucs/debug/log.h>
#include <ucg/api/ucg.h>
#include "builtin_plan.h"
#include "builtin_algo_mgr.h"

BEGIN_C_DECLS

typedef ucs_status_t (*ucg_builtin_plan_creator) (ucg_builtin_group_ctx_t *ctx,
                                                  enum ucg_builtin_plan_topology_type plan_topo_type,
                                                  const ucg_builtin_config_t *config,
                                                  const ucg_group_params_t *group_params,
                                                  const ucg_collective_params_t *coll_params,
                                                  ucg_builtin_plan_t **plan_p);

typedef struct ucg_builtin_coll_algo {
    int type; // collective operation type
    int id; // algorithm id
    ucg_builtin_plan_creator create; // function for creating plan
} ucg_builtin_coll_algo_t;
                                          
typedef ucg_builtin_coll_algo_t *ucg_builtin_coll_algo_h;

typedef struct ucg_builtin_algo_pool {
    ucg_builtin_coll_algo_t *barrier_algos[UCG_ALGORITHM_BARRIER_LAST];
    ucg_builtin_coll_algo_t *bcast_algos[UCG_ALGORITHM_BCAST_LAST];
    ucg_builtin_coll_algo_t *allreduce_algos[UCG_ALGORITHM_ALLREDUCE_LAST];
    ucg_builtin_coll_algo_t *alltoallv_algos[UCG_ALGORITHM_ALLTOALLV_LAST];
} ucg_builtin_algo_pool_t;
extern ucg_builtin_algo_pool_t ucg_builtin_algo_manager; // global algo mgmt object

typedef struct ucg_inc_func {
    size_t (*inc_enable_f)(void* config);
    size_t (*inc_available_f)(const void *group);
    size_t (*inc_used_f)(const void *params);
    void (*inc_send_cb_f)(void *req);
    int (*inc_get_header_size_f)(void);
    ucs_status_t (*inc_create_f)(void *group, void *config, const void *params);
    ucs_status_t (*inc_destroy_f)(void *group, uint8_t fail_cause);
    ucs_status_t (*inc_comp_recv_one_f)(void *req, uint64_t offset, const void *data, size_t length);
    ucs_status_t (*inc_comp_recv_many_f)(void *req, uint64_t offset, const void *data, size_t length);
    ucs_status_t (*ucg_builtin_add_inc_f)(void *tree, void *phase, const void *params, uct_ep_h **eps,
                                          unsigned *phs_increase_cnt, unsigned *step_increase_cnt, unsigned ppx,
                                          enum ucg_group_hierarchy_level topo_level);
} ucg_inc_func_t;
extern ucg_inc_func_t ucg_inc;

/*only used for inc_enable, inc_available, inc_used */
#define UCG_BUILTIN_INC_CHECK(_func, _args)                              \
    ((ucg_inc._func##_f == NULL) ? 0 : (int)ucg_inc._func##_f(_args))

#define UCG_BUILTIN_INC_REGISTER(_enable, _available, _used, _send_cb,    \
                                 _get_header_size, _create, _destroy,     \
                                 _inc_comp_recv_one, _inc_comp_recv_many, \
                                 _ucg_builtin_add_inc)                    \
            UCS_STATIC_INIT {                                             \
                ucg_inc.inc_enable_f = _enable;                           \
                ucg_inc.inc_available_f = _available;                     \
                ucg_inc.inc_used_f = _used;                               \
                ucg_inc.inc_send_cb_f = _send_cb;                         \
                ucg_inc.inc_get_header_size_f = _get_header_size;         \
                ucg_inc.inc_create_f = _create;                           \
                ucg_inc.inc_destroy_f = _destroy;                         \
                ucg_inc.inc_comp_recv_one_f = _inc_comp_recv_one;         \
                ucg_inc.inc_comp_recv_many_f = _inc_comp_recv_many;       \
                ucg_inc.ucg_builtin_add_inc_f = _ucg_builtin_add_inc;     \
            }

/**
 * @brief Algorithm registration interface.
 */
#define UCG_BUILTIN_ALGO_REGISTER(_coll_type_lname, _type, _id, _create)  \
    ucg_builtin_coll_algo_t coll_algo_##_coll_type_lname##_id = {         \
        .type = (_type),                                                  \
        .id = (_id),                                                      \
        .create = (_create)                                               \
    };                                                                    \
    UCS_STATIC_INIT {                                                     \
        ucg_builtin_algo_manager._coll_type_lname##_algos[_id]            \
        = &coll_algo_##_coll_type_lname##_id;                             \
    }

/**
 * @brief Get algorithm interface.
 * 
 * @return UCS_OK represents success, UCS_ERR_NO_ELEM represents this algo isn't existed.
 */
ucs_status_t ucg_builtin_algo_find(int type, int id, ucg_builtin_coll_algo_h *algo);


END_C_DECLS

#endif