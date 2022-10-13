/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/
#ifndef TEST_STUB_H_
#define TEST_STUB_H_

#include <ucg/api/ucg.h>

#include <map>
#include <string>
#include <vector>

extern "C" {
#include "planc/ucg_planc.h"
}

namespace test {

class stub {
public:
    enum routine_type_t {
        //memory, only availabe when macro UCG_ENABLE_DEBUG is on.
        MALLOC,
        CALLOC,
        REALLOC,
        POSIX_MEMALIGN,
        STRDUP,
        // planc api
        PLANC_MEM_QUERY,
        PLANC_CONFIG_READ,
        PLANC_CONFIG_MODIFY,
        PLANC_CONTEXT_INIT,
        PLANC__CONTEXT_QUERY,
        PLANC_GROUP_CREATE,
        PLANC_GET_PLANS,
        // plan op
        PLAN_PREPARE,
        PLAN_OP_TRIGGER,
        PLAN_OP_PROGRESS,
        PLAN_OP_DISCARD,
        // User Callback
        GET_LOCATION_CB,
        ALLGATHER_CB,
        ROUTINE_TYPE_LAST,
    };

    enum routine_result_t {
        SUCCESS,
        FAILURE,
    };

    struct routine_info_t {
        int count;
        std::vector<routine_result_t> ressult;
    };
public:
    static void init(bool load_planc = false);
    static void cleanup();

    /** Mock results.size() times, i-th routine call return results[i]. */
    static void mock(routine_type_t type, std::vector<routine_result_t> results,
                     const char *match = "*");
    /** Determines the return value of the stub function. */
    static bool call(routine_type_t type, const char *name = "*");

private:
    static bool m_load_planc;
    static std::map<std::string, routine_info_t> routines[ROUTINE_TYPE_LAST];
};

} // namespace test

extern "C" {
/******************************************************************************
 *                           Parameters to create stub context and group
 *****************************************************************************/
extern ucg_params_t test_stub_context_params;
extern ucg_group_params_t test_stub_group_params;

extern void *test_stub_acl_buffer;

ucg_status_t test_stub_allgatherv(const void *sendbuf, void *recvbuf, int32_t count, void *group);
}
#endif