/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "core/ucg_group.h"
#include "planc/ucx/planc_ucx_context.h"
#include "planc/ucx/planc_ucx_group.h"
}

using namespace test;

class test_planc_ucx_group : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init();

        ucg_planc_params_t params;
        ucg_planc_config_h config;
        ucg_context_t context = {
            .oob_group = {
                .size = 10
            }
        };
        params.context = &context;
        ucg_planc_ucx_config_read(NULL, NULL, &config);
        ucg_planc_ucx_config_modify(config, "USE_OOB", "no");
        ucg_planc_ucx_context_init(&params, config, &m_context);
        ucg_planc_ucx_config_release(config);

        ucg_group_t group = {
            .context = &context
        };
        m_group_params.group = &group;
    }

    static void TearDownTestSuite()
    {
        ucg_planc_ucx_context_cleanup(m_context);
        stub::cleanup();
    }

    static ucg_planc_context_h m_context;
    static ucg_planc_group_params_t m_group_params;
};
ucg_planc_context_h text_planc_ucx_group::m_context = NULL;
ucg_planc_group_params_t test_planc_ucx_group::m_group_params = {NULL};

TEST_F(test_planc_ucx_group, create)
{
    ucg_planc_group_h planc_group;
    ASSERT_EQ(ucg_planc_ucx_group_create(m_context, &m_group_params, &planc_group), UCG_OK);
    test_planc_ucx_group_destroy(planc_group);
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_planc_ucx_group, create_fail_malloc)
{
    ucg_planc_group_h planc_group;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::CALLOC, result, "ucg planc ucx group");
    ASSERT_NE(ucg_planc_ucx_group_create(m_context, &m_group_params, &planc_group), UCG_OK);
}
#endif
