/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_ucx_group.h"
#include "planc_ucx_global.h"
#include "util/ucg_malloc.h"
#include "util/ucg_helper.h"
#include "util/ucg_log.h"
#include "core/ucg_def.h"
#include "core/ucg_group.h"

ucg_status_t ucg_planc_ucx_group_create(ucg_planc_context_h context,
                                        const ucg_planc_group_params_t *params,
                                        ucg_planc_group_h *planc_group)
{
    UCG_CHECK_NULL_INVALID(context, params, planc_group);

    ucg_status_t status;
    ucg_planc_ucx_group_t *ucx_group;

    ucx_group = ucg_calloc(1, sizeof(ucg_planc_ucx_group_t), "ucg planc ucx group");
    if (ucx_group == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_planc_group_t, &ucx_group->super, params->group);
    if (status != UCG_OK) {
        ucg_error("Failed to init planc_ucx_group->super");
        goto err_free_ucx_group;
    }

    ucx_group->context = (ucg_planc_ucx_context_t *)context;
    for (int i = 0; i < UCG_ALGO_GEOUP_STATE_LAST; ++i) {
        ucx_group->groups[i].super.myrank = UCG_INVALID_RANK;
        ucx_group->groups[i].super.group = params->group;
        ucx_group->groups[i].state = UCG_ALGO_GEOUP_STATE_NOT_INIT;
    }

    *planc_group = (ucg_planc_group_h)ucx_group;
    return UCG_OK;

err_free_ucx_group:
    ucg_free(ucx_group);
    return status;
}

void ucg_planc_ucx_group_destroy(ucg_planc_group_h planc_group)
{

}

ucg_status_t ucg_planc_ucx_create_node_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup)
{

}

ucg_status_t ucg_planc_ucx_create_socket_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup)
{

}