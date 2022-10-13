/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "acl/acl.h"

extern "C" {
#include "core/ucg_group.h"
#include "planc/hccl/planc_hccl_context.h"
#include "planc/hccl/planc_hccl_group.h"
#include "planc/hccl/planc_hccl_global.h"
}

using namespace test;

/**
 * HcclCommInitRootInfo() can be called only once during the lifetime of the
 * process, so these test cases must be tested using independent processes.
 */
class test_planc_hccl_group : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init();


        if (aclInit(NULL) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        if (aclrtSetDevice(0) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        ucg_planc_config_h config;
        ucg_planc_hccl_config_read(NULL, NULL, &config);

        // Single member group so that HcclCommInitRootInfo() does not block.
        m_ucg_context.oob_group.size = 1;
        m_ucg_context.oob_group.myrank = 0;
        ucg_planc_params_t params;
        params.context = &m_ucg_context;
        ucg_planc_hccl_context_init(&params, config, &m_hccl_context);
        ucg_planc_hccl_config_release(config);
        // Global group
        m_ucg_group.size = m_ucg_context.oob_group.size;
        m_ucg_group.myrank = m_ucg_context.oob_group.myrank;
        m_ucg_group.oob_group.allgather = test_stub_allgather;
        m_ucg_group.oob_group.size = m_ucg_group.size;
        m_ucg_group.oob_group.myrank = m_ucg_group.myrank;
        m_ucg_group.oob_group.group = &m_ucg_group.oob_group;
    }

    static void TearDownTestSuite()
    {
        if (aclrReseDevice(0) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        if (aclFinalize() != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        stub::cleanup();
    }

    static ucg_context_t m_ucg_context;
    static ucg_group_t m_ucg_group;
    static ucg_planc_context_h m_hccl_context;
};
ucg_context_t test_planc_hccl_group::m_ucg_context;
ucg_group_t test_planc_hccl_group::m_ucg_group;
ucg_planc_context_h test_planc_hccl_group::m_hccl_context = NULL;

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_planc_hccl_group, create_fail_malloc)
{
    ucg_planc_group_h planc_group;
    ucg_planc_group_params_t params;
    params.group = &m_ucg_group;

    std::vector<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::CALLOC, result, "ucg planc hccl group");
    ASSERT_NE(ucg_planc_hccl_group_create(m_hccl_context, &params, &planc_group), UCG_OK);
}
#endif

TEST_F(test_planc_hccl_group, create)
{
    ucg_planc_group_params_t params;
    params.group = &m_ucg_group;
    ucg_planc_group_h planc_group;
    ASSERT_EQ(ucg_planc_hccl_group_create(m_hccl_context, &params, &planc_group), UCG_OK);
    ucg_planc_hccl_group_destroy(planc_group);
}

TEST_F(test_planc_hccl_group, create_multi_group)
{
    ucg_planc_group_params_t params;
    params.group = &m_ucg_group;

    ucg_planc_group_h planc_group1;
    ASSERT_EQ(ucg_planc_hccl_group_create(m_hccl_context, &params, &planc_group1), UCG_OK);

    ucg_planc_group_h planc_group2;
    ASSERT_EQ(ucg_planc_hccl_group_create(m_hccl_context, &params, &planc_group2), UCG_OK);

    ucg_planc_hccl_group_destroy(planc_group1);
    ucg_planc_hccl_group_destroy(planc_group2);
}

TEST_F(test_planc_hccl_group, create_again)
{
    ucg_planc_group_params_t params;
    params.group = &m_ucg_group;
    ucg_planc_group_h planc_group;

    ASSERT_EQ(ucg_planc_hccl_group_create(m_hccl_context, &params, &planc_group), UCG_OK);
    ucg_planc_hccl_group_destroy(planc_group);

    ASSERT_EQ(ucg_planc_hccl_group_create(m_hccl_context, &params, &planc_group), UCG_OK);
    ucg_planc_hccl_group_destroy(planc_group);
}
