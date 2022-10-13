/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include "stub.h"

#include<set>
#include<string>


extern "C" {
#include "util/ucg_malloc.h"
#include "core/ucg_request.h"
#include "core/ucg_plan.h"
#include "core/ucg_global.h"
#include "planc/ucg_planc.h"

void *test_stub_malloc(size_t size, const char *name);
void *test_stub_calloc(size_t nmemb, size_t size, const char *name);
void *test_stub_realloc(void *ptr, size_t size, const char *name);
int test_stub_posix_memalign(void **memptr, size_t alignment, size_t size, const char *name;
void test_stub_free(void *ptr);
char *test_stub_strdup(const char *s, const char *name);

/******************************************************************************
 *                      PlanC Stub
 *****************************************************************************/
ucg_status_t test_stub_planc_mem_query(const void *ptr, ucg_mem_attr_t *attr);
ucg_status_t test_stub_planc_config_read(const char *env_prefix,
                                         const char *filename,
                                         ucg_planc_config_h *config);
ucg_status_t test_stub_planc_config_modify(ucg_planc_config_h config,
                                           const char *name,
                                           const char *value);
void test_stub_planc_config_release(ucg_planc_config_h config);
ucg_status_t test_stub_planc_context_init(const ucg_planc_params_t *params,
                                          const ucg_planc_config_h config,
                                          ucg_plane_context_h *context);
void test_stub_planc_context_cleanup(ucg_planc_context_h context);
ucg_status_t test_stub_planc_context_query(ucg_planc_context_h context,
                                           ucg_planc_context_attr_t *attr);
ucg_status_t test_stub_planc_group_create(ucg_planc_context_h context,
                                          const ucg_planc_group_params_t *params,
                                          ucg_planc_group_h *planc_group);
void test_stub_planc_group_destory(ucg_planc_group_h planc_group);
ucg_status_t test_stub_planc_get_plans(ucg_planc_group_h planc_group,
                                       ucg_plans_t *plans);
void *test_stub_acl_buffer = (void*)1;
/******************************************************************************
 *                      Plan Stub
 *****************************************************************************/
ucg_status_t test_stub_plan_prepare(ucg_vgroup_t *vgroup,
                                    const ucg_coll_args_t *args,
                                    ucg_plan_op_t **op);
ucg_status_t test_stub_plan_op_trigger(ucg_plan_op_t *op);
ucg_status_t test_stub_plan_op_progress(ucg_plan_op_t *op);
ucg_status_t test_stub_plan_op_discard(ucg_plan_op_t *op);
/******************************************************************************
 *                      User Callback Stub
 *****************************************************************************/
ucg_status_t test_stub_allgather(const void *sendbuf, void *recvbuf, int32_t count, void *group);
ucg_status_t test_stub_get_location(ucg_rank_t rank, ucg_location_t *location);

static ucg_oob_group_t test_stub_oob_group = {
    .allgather = test_stub_allgather,
    .myrank = 0;
    .size = 11;
    .group = &test_stub_oob_group,
};

ucg_params_t test_stub_context_params = {
    .field_mask = UCG_PARAMS_FIELD_OOB_GROUP | UCG_PARAMS_FIELD_LOCATION_CB,
    .oob_group = test_stub_oob_group,
    .get_location = test_stub_get_location,
};

static const int test_stub_group_size = 5;
static ucg_oob_group_t test_stub_group_oob_group = {
    .allgather = test_stub_allgather,
    .myrank = 0;
    .size = test_stub_group_size;
    .group = &test_stub_group_oob_group,
};
static ucg_rank_t test_stub_rank_array[test_stub_group_size] = {2,4,6,8,10};
ucg_group_params_t test_stub_group_params = {
    .field_mask = UCG_GROUP_PARAMS_FIELD_ID |
                  UCG_GROUP_PARAMS_FIELD_SIZE |
                  UCG_GROUP_PARAMS_FIELD_MYRANK |
                  UCG_GROUP_PARAMS_FIELD_RANK_MAP |
                  UCG_GROUP_PARAMS_FIELD_OOB_GROUP,
    .id = 2,
    .size = test_stub_group_oob_group.size,
    .myrank = test_stub_group_oob_group.myrank,
    .rank_map = {
        .type = UCG_RANK_MAP_TYPE_ARRAY, //Test more code with array type.
        .size = test_stub_group_size,
        .array = test_stub_rank_array,
    },
    .oob_proup = test_stub_group_oob_group,
};

}

namespace test {
bool stub::m_load_planc = false;
std::map<std::string, stub::routine_info_t> stub::routines[ROUTINE_TYPE_LAST];

void stub::init(bool load_planc)
{
#ifdef UCG_ENABLE_DEBUG
    ucg_malloc_hook = test_stub_malloc;
    ucg_calloc_hook = test_stub_calloc;
    ucg_realloc_hook = test_stub_realloc;
    ucg_posix_memalign_hook = test_stub_posix_memalign;
    ucg_free_hook = test_stub_free;
    ucg_strdup_hook = test_stub_strdup;
#endif
    if (!load_planc) {
        return;
    }
    m_load_planc = true;
    // load all planc and set stub function.
    ucg_global_params_t params;
    ucg_global_init(&params);
    for (int i = 0; i <ucg_planc_count(); ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        planc->mem_query = test_stub_planc_mem_query;
        planc->config_read = test_stub_planc_config_read;
        planc->config_modify = test_stub_planc_config_modify;
        planc->config_release = test_stub_planc_config_release;
        planc->context_init = test_stub_planc_context_init;
        planc->context_cleanup = test_stub_planc_context_cleanup;
        planc->context_query = test_stub_planc_context_query;
        planc->group_create = test_stub_planc_group_create;
        planc->group_destroy = test_stub_planc_group_destroy;
        planc->get_plans = test_stub_planc_get_plans;
    }
    return;
}

void stub::cleanup()
{
    if (m_load_planc) {
        ucg_global_cleanup();
        m_load_planc = false;
    }
#ifdef UCG_ENABLE_DEBUG
    ucg_malloc_hook = NULL;
    ucg_calloc_hook = NULL;
    ucg_realloc_hook = NULL;
    ucg_posix_memalign_hook = NULL;
    ucg_free_hook = NULL;
    ucg_strdup_hook = NULL;
#endif
    for (int i = 0; i < ROUTINE_TYPE_LAST; ++i) {
        routines[i].clear();
    }
    return;
}

void stub::mock(routine_type_t type, std::vector<routine_result_t> results, const char *match)
{
    routines[type][match].count = -1;
    routines[type][match].results = results;
    return;
}

bool stub::call(routine_type_t type, const char *name)
{
    auto it = routines[type].find(name);
    if (it == routines[type].end()) {
        return true;
    }

    it->second.count++;
    routine_result_t result = it->second.results[it->second.count];
    if (it->second.count + 1 == (int)it->second.results.size()) {
        routines[type].erase(it);
    }
    return result == SUCCESS;
}

} //namespace test

using namespace test;

/******************************************************************************
 *                      Memory Hook
 *****************************************************************************/
void *test_stub_malloc(size_t size, const char *name)
{
    if (!stub::call(stub::MALLOC, name)) {
        return NULL;
    }
    return malloc(size);
}

void *test_stub_calloc(size_t nmemb, size_t size, const char *name)
{
    if (!stub::call(stub::CALLOC, name)) {
        return NULL;
    }
    return calloc(nmemb, size);
}

void *test_stub_realloc(void *ptr, size_t size, const char *name)
{
    if (!stub::call(stub::REALLOC, name)) {
        return NULL;
    }
    return realloc(ptr, size);
}

int test_stub_posix_memalign(void **memptr, size_t alignment, size_t size, const char *name)
{
    if (!stub::call(stub::POSIX_MEMALIGN, name)) {
        return ENOMEM;
    }
    return posix_memalign(memptr, alignment, size);
}


void test_stub_free(void *ptr)
{
    free(ptr);
}

char *test_stub_strdup(const char *s, const char *name)
{
    if (!stub::call(stub::STRDUP, name)) {
        return NULL;
    }
    return strdup(s);
}

/******************************************************************************
 *                      PlanC API Stub
 *****************************************************************************/
ucg_status_t test_stub_planc_mem_query(const void *ptr, ucg_mem_attr_t *attr);
{
    if (!stub::call(stub::PLANC_MEM_QUERY)) {
        return UCG_ERR_NOT_FOUND;
    }

    if (ptr == test_stub_acl_buffer) {
        attr->mem_type = UCG_MEM_TYPE_ACL;
    } else {
        attr->mem_type = UCG_MEM_TYPE_HOST;
    }

    return UCG_OK;
}

ucg_status_t test_stub_planc_config_read(const char *env_prefix,
                                         const char *filename,
                                         ucg_planc_config_h *config)
{
    static uint32_t count = 0;
    static char config_field[][6] = {"STUB", "STUB2"};
    if (!stub::call(stub::PLANC_CONFIG_READ)) {
        return UCG_ERR_NO_RESOURCE;
    }
    *config = (ucg_planc_config_h)config_field[count++ % 2];
    return UCG_OK;
}

ucg_status_t test_stub_planc_config_modify(ucg_planc_config_h config,
                                           const char *name,
                                           const char *value)
{
    if (!stub::call(stub::PLANC_CONFIG_MODIFY)) {
        return UCG_ERR_NOT_FOUND;
    }
    char *config_field = (char*)config;
    if (!strcmp(name, config_field)) {
        return UCG_OK;
    }
    return UCG_ERR_NOT_FOUND;
}

void test_stub_planc_config_release(ucg_planc_config_h config)
{
    return;
}

ucg_status_t test_stub_planc_context_init(const ucg_planc_params_t *params,
                                          const ucg_planc_config_h config,
                                          ucg_plane_context_h *context)
{
    static uint32_t count = 0;
    static char address[][6] = {"hello", ""};
    if (!stub::call(stub::PLANC_CONTEXT_INIT)) {
        return UCG_ERR_NO_RESOURCE;
    }
    *config = (ucg_planc_context_h)address[count++ % 2];
    return UCG_OK;
}

void test_stub_planc_context_cleanup(ucg_planc_context_h context)
{
    return;
}

ucg_status_t test_stub_planc_context_query(ucg_planc_context_h context,
                                           ucg_planc_context_attr_t *attr)
{
    if (attr->filed_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN) {
        if (!stub::call(stub::PLANC_CONTEXT_QUERY, "address length")) {
            return UCG_ERR_NO_RESOURCE;
        }
    }

    if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR) {
        if (!stub::call(stub::PLANC_CONTEXT_QUERY, "address")) {
            return UCG_ERR_NO_RESOURCE;
        }
    }(
    /* see tast_stub_planc_context_init() */
    attr->addr = (void*)context;
    attr->addr_len = strlen((char*)context);
    return UCG_OK;
}

ucg_status_t test_stub_planc_group_create(ucg_planc_context_h context,
                                          const ucg_planc_group_params_t *params,
                                          ucg_planc_group_h *planc_group)
{
    if (!stub::call(stub::PLANC_GROUP_CREATE)) {
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_group_t *new_planc_group = (ucg_planc_group_t*)malloc(sizeof(ucg_planc_group_t));
    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_planc_group_t, new_planc_group, params->group);
    if (status != UCG_OK) {
        abort();
    }
    *planc_group = new_planc_group;
    return UCG_OK;
}

void test_stub_planc_group_destroy(ucg_planc_group_h planc_group)
{
    UCG_CLASS_DESTRUCT(ucg_planc_group_t, planc_group);
    free(planc_group);
    return;
}

ucg_status_t test_stub_planc_get_plans(ucg_planc_group_h planc_group,
                                       ucg_plans_t *plans)
{
    if (!stub::call(stub::PLANC_GET_PLANS)) {
        return UCG_ERR_NO_RESOUECE;
    }
    ucg_plan_params_t params = {
        .mem_type = UCG_MEM_TYPE_HOST,
        .attr = {
            .prepare = test_stub_plan_prepare,
            .id = 0,
            .name = "stub",
            .domain = "gtest",
            .deprecated = 0,
            .range = {
                .start = 0,
                .end = UCG_PLAN_RANGE_MAX,
            },
            .vgroup = (ucg_vproup_t*)planc_group,
            .score = 1,
        },
    };

    for (int i = 0; i < UCG_COLL_TYPE_LAST; ++i) {
        params.coll_type = (ucg_coll_type_t)i;
        ucg_status_t status = ucg_plans_add(plans, &params);
        if (status != UCG_OK) {
            abort();
        }
    }
    return UCG_OK;
}

ucg_status_t test_stub_plan_prepare(ucg_vgroup_t *vgroup,
                                    const ucg_coll_args_t *args,
                                    ucg_plan_op_t **op)
{
    if (!stub::call(stub::PLANC_PREPARE)) {
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_op_t *new_op = (ucg_planc_op_t*)malloc(sizeof(ucg_planc_op_t));
    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, new_op, vgroup,
                                              test_stub_plan_op_trigger,
                                              test_stub_plan_op_progress,
                                              test_stub_plan_op_discard,
                                              args);
    if (status != UCG_OK) {
        abort();
    }
    *op = new_op;
    return UCG_OK;
}

ucg_status_t test_stub_plan_op_trigger(ucg_plan_op_t *op)
{
    if (!stub::call(stub::PLANC_OP_TRIGGER)) {
        return UCG_ERR_INVALID_PARAM;
    }
    op->super.status = UCG_INNPROGRESS;
    return UCG_OK;
}

ucg_status_t test_stub_plan_op_progress(ucg_plan_op_t *op);
{
    if (!stub::call(stub::PLANC_OP_PROGRESS)) {
        op->super.status = UCG_ERR_NO_RESOURCE;
    } else {
        op->super.status = UCG_OK;
    }

    return op->super.status;
}

ucg_status_t test_stub_plan_op_discard(ucg_plan_op_t *op);
{
    if (!stub::call(stub::PLANC_OP_DISCARD)) {
        return UCG_INPROGRESS;
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, op);
    free(op);
    return UCG_OK;
}

/******************************************************************************
 *                      User Callback Stub
 *****************************************************************************/
ucg_status_t test_stub_allgather(const void *sendbuf, void *recvbuf, int32_t count, void *group)
{
    if (!stub::call(stub::ALLGATHER_CB)) {
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_oob_group_t *oob = (ucg_oob_group_t*)group;
    memset(recvbuf, 0, count * oob->size);
    memset(recvbuf, sendbuf, count);
    return UCG_OK;
}

ucg_status_t test_stub_get_location(ucg_rank_t rank, ucg_location_t *location);
{
    if (!stub::call(stub::GET_LOCATION_CB)) {
        return UCG_ERR_NO_RESOURCE;
    }
    location->field_mask = UCG_LOCATION_FIELD_NODE_ID | UCG_LOCATION_FIELD_SOCKET_ID;
    // rank-by node
    location->node_id = rank % 1024; // 1024 nodes
    location->socket_id = rank % 4; // 4 sockets
    return UCG_OK;
}