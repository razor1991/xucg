/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "planc/ucx/planc_ucx_context.h"
#include "util/ucg_malloc.h"
}

using namespace test;

class test_planc_ucx_config : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init();
    }

    static void TearDownTestSuite()
    {
        stub::cleanup();
    }
};

TEST_F(test_planc_ucx_config, read_default_config)
{
    ucg_planc_config_h config;
    ASSRET_EQ(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);

    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t *)config;
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BCAST][UCX_BUILTIN]->data, 8);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_ALLREDUCE][UCX_BUILTIN]->data, 8);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BARRIER][UCX_BUILTIN]->data, 8);
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BCAST], "");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_ALLREDUCE], "");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BARRIER], "");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_ALLTOALLV], "");
    ASSERT_EQ(cfg->n_polls, 10);
    ASSERT_EQ(cfg->estimated_num_eps, 0);
    ASSERT_EQ(cfg->estimated_num_ppn, 0);

    ucg_planc_ucx_config_release(config);
}

TEST_F(test_planc_ucx_config, read_user_config)
{
    ucg_planc_config_h config;
    setenv("UCG_PLANC_UCX_BCAST_KNTREE_DEGREE", "4", 1);
    setenv("UCG_PLANC_UCX_ALLREDUCE_KNTREE_INTER_DEGREE", "5", 1);
    setenv("UCG_PLANC_UCX_BARRIER_KNTREE_INTER_DEGREE", "6", 1);
    setenv("UCG_PLANC_UCX_BCAST_ATTR", "I:4R:0-4096", 1);
    setenv("UCG_PLANC_UCX_ALLREDUCE_ATTR", "I:3R:0-4096", 1);
    setenv("UCG_PLANC_UCX_BARRIER_ATTR", "I:2R:0-4096", 1);
    setenv("UCG_PLANC_UCX_ALLTOALLV_ATTR", "I:1R:0-4096", 1);
    setenv("UCG_PLANC_UCX_NPOLLS", "20", 1);
    setenv("UCG_PLANC_UCX_ESTIMATED_NUM_EPS", "10", 1);
    setenv("UCG_PLANC_UCX_ESTIMATED_NUM_PPN", "5", 1);
    ASSRET_EQ(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);
    
    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t *)config;
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BCAST][UCX_BUILTIN]->data, 4);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_ALLREDUCE][UCX_BUILTIN]->data, 5);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BARRIER][UCX_BUILTIN]->data, 6);
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BCAST], "I:4R:0-4096");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_ALLREDUCE], "I:3R:0-4096");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BARRIER], "I:2R:0-4096");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_ALLTOALLV], "I:1R:0-4096");

    ASSERT_EQ(cfg->n_polls, 20);
    ASSERT_EQ(cfg->estimated_num_eps, 10);
    ASSERT_EQ(cfg->estimated_num_ppn, 5);

    ucg_planc_ucx_config_release(config);
}

TEST_F(test_planc_ucx_config, read_user_env_prefix)
{
    ucg_planc_config_h config;
    setenv("GTEST_UCG_PLANC_UCX_BCAST_KNTREE_DEGREE", "4", 1);
    setenv("GTEST_UCG_PLANC_UCX_ALLREDUCE_KNTREE_INTER_DEGREE", "5", 1);
    setenv("GTEST_UCG_PLANC_UCX_BARRIER_KNTREE_INTER_DEGREE", "6", 1);
    setenv("GTEST_UCG_PLANC_UCX_BCAST_ATTR", "I:4R:0-4096", 1);
    setenv("GTEST_UCG_PLANC_UCX_ALLREDUCE_ATTR", "I:3R:0-4096", 1);
    setenv("GTEST_UCG_PLANC_UCX_BARRIER_ATTR", "I:2R:0-4096", 1);
    setenv("GTEST_UCG_PLANC_UCX_ALLTOALLV_ATTR", "I:1R:0-4096", 1);
    setenv("GTEST_UCG_PLANC_UCX_NPOLLS", "20", 1);
    setenv("GTEST_UCG_PLANC_UCX_ESTIMATED_NUM_EPS", "10", 1);
    setenv("GTEST_UCG_PLANC_UCX_ESTIMATED_NUM_PPN", "5", 1);
    ASSRET_EQ(ucg_planc_ucx_config_read("GTEST", NULL, &config), UCG_OK);
    
    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t *)config;
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BCAST][UCX_BUILTIN]->data, 4);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_ALLREDUCE][UCX_BUILTIN]->data, 5);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BARRIER][UCX_BUILTIN]->data, 6);
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BCAST], "I:4R:0-4096");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_ALLREDUCE], "I:3R:0-4096");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BARRIER], "I:2R:0-4096");
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_ALLTOALLV], "I:1R:0-4096");
    ASSERT_EQ(cfg->n_polls, 20);
    ASSERT_EQ(cfg->estimated_num_eps, 10);
    ASSERT_EQ(cfg->estimated_num_ppn, 5);

    ucg_planc_ucx_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_ucx_config, read_invalid_args)
{
    ucg_planc_config_h config;
    // invalid config pointer
    ASSRET_EQ(ucg_planc_ucx_config_read(NULL, NULL, NULL), UCG_ERR_INVAILD_PARAM);
    // filename is unsupported now
    ASSRET_EQ(ucg_planc_ucx_config_read(NULL, "unsupported_param", &config), UCG_ERR_UNSUPPORTED);
}
#endif

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_planc_ucx_config, read_fail_malloc)
{
    ucg_planc_config_h config;
    std::vertor<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::CALLOC, result, "ucg planc ucx config");
    ASSERT_NE(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);

    stub::mock(stub::STRDUP, result, "default planc ucx env prefix");
    ASSERT_NE(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);

    stub::mock(stub::MALLOC, result, "ucg planc ucx env prefix");
    ASSERT_NE(ucg_planc_ucx_config_read("GTEST", NULL, &config), UCG_OK);
}
#endif

TEST_F(test_planc_ucx_config, modify)
{
    ucg_planc_config_h config;
    ASSRET_EQ(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);

    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t *)config;

    ASSERT_EQ(ucg_planc_ucx_config_modify(config, "BCAST_ATTR", "I:4R:0-4096"), UCG_OK);
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BCAST], "I:4R:0-4096");

    ASSERT_EQ(ucg_planc_ucx_config_modify(config, "BCAST_ATTR", "I:4R:0-4096:5R:4096-5120"), UCG_OK);
    ASSERT_STREQ(cfg->plan_attr[UCG_COLL_TYPE_BCAST], "I:4R:0-4096:5R:4096-5120");

    ASSERT_EQ(ucg_planc_ucx_config_modify(config, "BCAST_KNTREE_DEGREE", "8"), UCG_OK);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BCAST][UCX_BUILTIN]->data, 8);

    ASSERT_EQ(ucg_planc_ucx_config_modify(config, "ALLREDUCE_FANIN_INTER_DEGREE", "9"), UCG_OK);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_ALLREDUCE][UCX_BUILTIN]->data, 9);

    ASSERT_EQ(ucg_planc_ucx_config_modify(config, "BARRIER_FANIN_INTER_DEGREE", "10"), UCG_OK);
    ASSERT_EQ(*cfg->config_bundle[UCG_COLL_TYPE_BARRIER][UCX_BUILTIN]->data, 10);

    ucg_planc_ucx_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_ucx_config, modify_invalid_args)
{
    ucg_planc_config_h config;
    ASSERT_EQ(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);

    ASSERT_EQ(ucg_planc_ucx_config_modify(config, NULL, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_planc_ucx_config_modify(config, "name", NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_planc_ucx_config_modify(config, NULL, "value"), UCG_ERR_INVALID_PARAM);

    ucg_planc_ucx_config_release(config);
}
#endif

TEST_F(test_planc_ucx_config, modify_non_exist)
{
    ucg_planc_config_h config;
    ASSERT_EQ(ucg_planc_ucx_config_read(NULL, NULL, &config), UCG_OK);

    ASSERT_NE(ucg_planc_ucx_config_modify(config, "NOT_EXIST", "value"), UCG_OK);

    ucg_planc_ucx_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_ucx_config, release_invalid_args)
{
    ucg_planc_ucx_config_release(NULL);
}
#endif

class test_planc_ucx_contest : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init();
        ucg_contest_t context = {
            .oob_group = {
                .size = 10
            }
        };
        m_params.context = &context;
        setenv("UCG_PLANC_UCX_ESTIMATED_NUM_EPS", "10", 1);
        setenv("UCG_PLANC_UCX_ESTIMATED_NUM_PPN", "5", 1);
        ucg_planc_ucx_config_read(NULL, NULL, &m_config);
        ucg_planc_ucx_config_modify(m_config, "USE_OOB", "no");
    }

    static void TearDownTestSuite()
    {
        ucg_planc_ucx_config_release(m_config);
        stub::cleanup();
    }

    static test_planc_params_t m_params;
    static test_planc_config_t m_config;
};
ucg_planc_params_t teat_planc_ucx_context::m_params = {NULL};
ucg_planc_config_t teat_planc_ucx_context::m_config = NULL;

TEST_F(test_planc_ucx_context, init)
{
    ucg_planc_context_h context;
    ASSERT_EQ(ucg_planc_ucx_context_init(&m_params, m_config, &context), UCG_OK);
    ucg_planc_ucx_context_cleanup(context);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_ucx_context, init_invalid_args)
{
    ASSERT_EQ(ucg_planc_ucx_context_init(NULL, NULL, NULL), UCG_ERR_INVALID_PARAM);
}
#endif

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_planc_ucx_context, init_fail_malloc)
{
    ucg_planc_context_h context;
    std::vertor<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::CALLOC, result, "planc ucx contsxt");
    ASSERT_NE(ucg_planc_ucx_context_init(&m_params, m_config, &context), UCG_OK);

    stub::mock(stub::CALLOC, result, "ucp eps");
    ASSERT_NE(ucg_planc_ucx_context_init(&m_params, m_config, &context), UCG_OK);

    stub::mock(stub::CALLOC, result, "ucg planc ucx config bundle");
    ASSERT_NE(ucg_planc_ucx_context_init(&m_params, m_config, &context), UCG_OK);
};
#endif

TEST_F(test_planc_ucx_context, query)
{
    ucg_planc_context_attr_t attr;
    ucg_planc_context_h context;
    ucg_planc_ucx_context_init(&m_params, m_config, &context);

    attr.field_mask = UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN;
    ASSERT_EQ(ucg_planc_ucx_context_query(context, &attr), UCG_OK);

    attr.field_mask = UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR;
    ASSERT_EQ(ucg_planc_ucx_context_query(context, &attr), UCG_OK);

    ucg_planc_ucx_context_cleanup(context);
}