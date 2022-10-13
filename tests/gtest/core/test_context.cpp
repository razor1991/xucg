/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include <ucg/api/ucg.h>
#include "stub.h"

extern "C" {
#include "core/ucg_group.h"
#include "planc/ucg_planc.h"
#include "core/ucg_global.h"
}

using namespace test;

class test_ucg_config : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init(true);
    }

    static void TearDownTestSuite()
    {
        stub::cleanup();
    }
};

class test_ucg_context : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init(true);
        // use planc fake and fake2 to test.
        setenv("UCG_PLANC", "fake, fake2", 1);
        ucg_config_read(NULL, NULL, &m_config);
    }

    static void TearDownTestSuite()
    {
        unsetenv("UCG_PLANC");
        ucg_config_release(m_config);
        stub::cleanup();
    }

    static ucg_config_h m_config;
};
ucg_config_h test_ucg_context::m_context = NULL;

TEST_T(test_ucg_config, read_default_config)
{
    ucg_config_h config;
    ASSERT_EQ(ucg_config_read(NULL, NULL, &config), UCG_OK);
    // Default UCG_PLANC=all
    ASSERT_EQ(config->planc.count, 1);
    ASSERT_STREQ(config->planc.names[0], "all");
    ucg_config_release(config);
}

TEST_T(test_ucg_config, read_user_config)
{
    ucg_config_h config;
    setenv("UCG_PLANC", "ucx", 1);
    ASSERT_EQ(ucg_config_read(NULL, NULL, &config), UCG_OK);
    ASSERT_EQ(config->planc.count, 1);
    ASSERT_STREQ(config->planc.names[0], "ucx");
    ucg_config_release(config);
}

TEST_T(test_ucg_config, read_user_env_prefix)
{
    ucg_config_h config;
    setenv("GTEST_UCG_PLANC", "ucx", 1);
    ASSERT_EQ(ucg_config_read("GTEST", NULL, &config), UCG_OK);
    ASSERT_EQ(config->planc.count, 1);
    ASSERT_STREQ(config->planc.names[0], "ucx");
    ucg_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_config, read_invalid_args)
{
    ucg_config_h config;
    // invalid config pointer
    ASSERT_EQ(ucg_config_read(NULL, NULL, NULL), UCG_ERR_INVALID_PARAM);
    //filename is unsupported now
    ASSERT_EQ(ucg_config_read(NULL, "unsupported_param", &config), UCG_ERR_UNSUPPORTED);
}
#endif

TEST_F(test_ucg_config, read_fail_planc_cfg)
{
    ucg_config_h config;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::PLANC_CONFIG_READ, result);
    ASSERT_NE(ucg_config_read(NULL, NULL, &config), UCG_OK);
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_ucg_config, read_fail_malloc)
{
    ucg_config_h config;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::MALLOC, result, "ucg config");
    ASSERT_NE(ucg_config_read(NULL, NULL, &config), UCG_OK);

    stub::mock(stub::STRDUP, result, "default env prefix");
    ASSERT_NE(ucg_config_read(NULL, NULL, &config), UCG_OK);

    stub::mock(stub::MALLOC, result, "ucg env prefix");
    ASSERT_NE(ucg_config_read("GTEST", NULL, &config), UCG_OK);

    stub::mock(stub::CALLOC, result, "ucg planc cfg");
    ASSERT_NE(ucg_config_read(NULL, NULL, &config), UCG_OK);
}
#endif

TEST_T(test_ucg_config, modify)
{
    ucg_config_h config;
    ASSERT_EQ(ucg_config_read(NULL, NULL, &config), UCG_OK);

    ASSERT_EQ(ucg_config_modify(config, "PLANC", "value"), UCG_OK);
    ASSERT_EQ(config->planc.count, 1);
    ASSERT_STREQ(config->planc.names[0], "value");

    ASSERT_EQ(ucg_config_modify(config, "PLANC", "ucx,hccl"), UCG_OK);
    ASSERT_EQ(config->planc.count, 2);
    ASSERT_STREQ(config->planc.names[0], "ucx");
    ASSERT_STREQ(config->planc.names[0], "hccl");

    // modify configuration of planc fake
    ASSERT_EQ(ucg_config_modify(config, "STUB", "ucx,hccl"), UCG_OK);

    // modify configuration of planc fake2
    ASSERT_EQ(ucg_config_modify(config, "STUB2", "ucx,hccl"), UCG_OK);
    ucg_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_config, modify_invalid_args)
{
    ucg_config_h config;
    ASSERT_EQ(ucg_config_read(NULL, NULL, &config), UCG_OK);

    ASSERT_EQ(ucg_config_modify(config, NULL, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_config_modify(config, NULL, "value"), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_config_modify(config, "name", NULL), UCG_ERR_INVALID_PARAM);

    ucg_config_release(config);
}
#endif

TEST_T(test_ucg_config, modify_invalid_args)
{
    ucg_config_h config;
    ASSERT_EQ(ucg_config_read(NULL, NULL, &config), UCG_OK);
    ASSERT_NE(ucg_config_modify(config, "NON_EXIST", "value"), UCG_OK);
    ucg_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_config, release_invalid_args)
{
    //expect no segmentation fault.
    ucg_config_release(NULL);
}
#endif

TEST_T(test_ucg_context, init)
{
    ucg_context_h context = NULL;
    ucg_params_t params = test_stub_context_params;
    #ifdef UCG_ENABLE_MT
    params.field_mask |= UCG_PARAMS_FIELD_THREAD_MODE;
    params.thread_mode = UCG_THREAD_MODE_MULTI;
#endif
    ASSERT_EQ(ucg_init(&params, m_config, &context), UCG_OK);
    ASSERT_TRUE(context != NULL);
    ASSERT_TRUE(!memcmp(&context->oob_group, &params.oob_group, sizeof(ucg_oob_group_t)));
    ASSERT_TRUE(context->get_location == params.get_location);
    // use planc fake and planc fake2
    ASSERT_EQ(context->num_planc_rscs, 2);
#ifdef UCG_ENABLE_MT
    ASSERT_TRUE(context->thread_mode == UCG_THREAD_MODE_MULTI);
#endif 
    ucg_cleanup(context);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_context, init_invalid_args)
{
    ucg_context_h context;
    ucg_params_t params;
    ASSERT_EQ(ucg_init(NULL, m_config, &context), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_init(&params, NULL, &context), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_init(&params, m_config, NULL), UCG_ERR_INVALID_PARAM);
}
#endif

TEST_F(test_ucg_context, init_invalid_params)
{
    ucg_config_h config;
    ucg_params_t params;

    params.field_mask = 0;
    ASSERT_EQ(ucg_init(&params, m_config, &context), UCG_ERR_INVALID_PARAM);

    // No UCG_PARAMS_FIELD_LOCATION_CB
    params.field_mask = UCG_PARAMS_FIELD_OOB_GROUP;
    ASSERT_EQ(ucg_init(&params, m_config, &context), UCG_ERR_INVALID_PARAM);

    // No UCG_PARAMS_FIELD_OOB_GROUP
    params.field_mask = UCG_PARAMS_FIELD_LOCATION_CB;
    ASSERT_EQ(ucg_init(&params, m_config, &context), UCG_ERR_INVALID_PARAM);

#ifndef UCG_ENABLE_MT
    params = test_stub_context_params;
    params.field_mask |= UCG_PARAMS_FIELD_THREAD_MODE;
    params.thread_mode = UCG_THREAD_MODE_MULTI;
    ASSERT_EQ(ucg_init(&params, m_config, &context), UCG_ERR_INVALID_PARAM);
#endif
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_ucg_context, init_fail_malloc)
{
    ucg_context_h context;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};

    stub::mock(stub::CALLOC, result, "ucg context");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    stub::mock(stub::CALLOC, result, "ucg resource planc");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    stub::mock(stub::MALLOC, result, "local proc");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    stub::mock(stub::MALLOC, result, "size array");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    stub::mock(stub::MALLOC, result, "procs");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);
}
#endif

TEST_F(test_ucg_context, init_incompatible_version)
{
    ucg_context_h context;
    ASSERT_EQ(ucg_init_version(UCG_API_MAJOR-1, UCG_API_MINOR,
                                &test_stub_context_params, m_config, &context),
              UCG_ERR_INCOMPATIBLE);
    
    ASSERT_EQ(ucg_init_version(UCG_API_MAJOR+1, UCG_API_MINOR,
                                &test_stub_context_params, m_config, &context),
              UCG_ERR_INCOMPATIBLE);
    
    ASSERT_EQ(ucg_init_version(UCG_API_MAJOR, UCG_API_MINOR+1,
                                &test_stub_context_params, m_config, &context),
              UCG_ERR_INCOMPATIBLE);
}

TEST_F(test_ucg_context, init_fail_planc_context_init)
{
    ucg_context_h context;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};

    stub::mock(stub::CALLOC, result, "ucg context");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);
}

TEST_F(test_ucg_context, init_fail_planc_context_query)
{
    ucg_context_h context;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};

    stub::mock(stub::PLANC_CONTEXT_QUERY, result, "address length");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    stub::mock(stub::PLANC_CONTEXT_QUERY, result, "address");
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);
}

TEST_F(test_ucg_context, init_fail_get_location)
{
    ucg_context_h context;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};

    stub::mock(stub::GET_LOCATION_CB, result);
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);
}

TEST_F(test_ucg_context, init_fail_allgather)
{
    ucg_context_h context;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};

    // Failed to get size of process information
    stub::mock(stub::ALLGATHER_CB, result);
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    // Failed to get process information.
    result.clear();
    result.push_back(stub::SUCCESS); // get size Successfully.
    result.push_back(stub::FAILURE);
    stub::mock(stub::ALLGATHER_CB, result);
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);
}

TEST_T(test_ucg_context, init_fail_no_required_planc)
{
    // require planc ucx that is not exist.
    setenv("UCG_PLANC", "UCX", 1);
    ucg_config_h config;
    ASSERT_EQ(ucg_config_read(NULL, NULL, &config), UCG_OK);

    ucg_context_h context;
    ASSERT_NE(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    ucg_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_context, cleanup_invalid_args)
{
    // expect no segmentation fault.
    ucg_cleanup(NULL);
}
#endif


TEST_F(test_ucg_context, get_proc_addr)
{
    ucg_context_h context;
    ASSERT_EQ(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    ucg_planc_t *planc = ucg_planc_get_by_idx(0);
    char *addr = (char*)ucg_context_get_proc_addr(context, 0, planc);
    // Related to test_stub_planc_context_query().
    ASSERT_STREQ(addr, "hello");

    planc = ucg_planc_get_by_idx(1);
    addr = (char*)ucg_context_get_proc_addr(context, 0, planc);
    // address length is 0, return NULL.
    ASSERT_TRUE(addr == NULL);

    ucg_cleanup(context);
}

TEST_F(test_ucg_context, get_proc_addr_invalid)
{
    ucg_context_h context;
    ASSERT_EQ(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    // No required planc
    ucg_planc_t *planc = (ucg_planc_t*)1;
    char *addr = (char*)ucg_context_get_proc_addr(context, 0, planc);
    ASSERT_TRUE(addr == NULL);

    ucg_cleanup(context);
}

TEST_F(test_ucg_context, get_location)
{
    ucg_context_h context;
    ASSERT_EQ(ucg_init(&test_stub_context_params, m_config, &context), UCG_OK);

    ucg_location_t location;
    for (uint32_t i = 0; i < test_stub_context_params.oob_group.size; ++i) {
        ASSERT_EQ(ucg_context_get_location(context, i, &location), UCG_OK);
    }

    ucg_cleanup(context);
}