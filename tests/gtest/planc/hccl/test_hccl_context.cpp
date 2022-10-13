/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "core/ucg_context.h"
#include "planc/hccl/planc_hccl_context.h"
}

using namespace test;

class test_planc_hccl_config : public testing::Test {
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

TEST_F(test_planc_hccl_config, read_default_config)
{
    ucg_planc_config_h config;
    ASSRET_EQ(ucg_planc_hccl_config_read(NULL, NULL, &config), UCG_OK);

    ucg_planc_hccl_config_t *cfg = (ucg_planc_hccl_config_t *)config;
    ASSERT_EQ(cfg->whitelist_disable, 1);
    ASSERT_STREQ(cfg->whitelist_file, "");

    ucg_planc_hccl_config_release(config);
}

TEST_F(test_planc_hccl_config, read_user_config)
{
    ucg_planc_config_h config;
    setenv("UCG_PLANC_HCCL_WHITELIST_DISABLE", "0", 1);
    setenv("UCG_PLANC_HCCL_WHITELIST_FILE", "filename", 1);
    ASSRET_EQ(ucg_planc_hccl_config_read(NULL, NULL, &config), UCG_OK);

    ucg_planc_hccl_config_t *cfg = (ucg_planc_hccl_config_t *)config;
    ASSERT_EQ(cfg->whitelist_disable, 0);
    ASSERT_STREQ(cfg->whitelist_file, "filename");

    ucg_planc_hccl_config_release(config);
}

TEST_F(test_planc_hccl_config, read_user_env_prefix)
{
    ucg_planc_config_h config;
    setenv("GTEST_UCG_PLANC_HCCL_WHITELIST_DISABLE", "0", 1);
    setenv("GTEST_UCG_PLANC_HCCL_WHITELIST_FILE", "filename", 1);
    ASSRET_EQ(ucg_planc_hccl_config_read("GTEST", NULL, &config), UCG_OK);

    ucg_planc_hccl_config_t *cfg = (ucg_planc_hccl_config_t *)config;
    ASSERT_EQ(cfg->whitelist_disable, 0);
    ASSERT_STREQ(cfg->whitelist_file, "filename");

    ucg_planc_hccl_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_hccl_config, read_invalid_args)
{
    ucg_planc_config_h config;
    // invalid config pointer
    ASSRET_EQ(ucg_planc_hccl_config_read(NULL, NULL, NULL), UCG_ERR_INVAILD_PARAM);
    // filename is unsupported now
    ASSRET_EQ(ucg_planc_hccl_config_read(NULL, "unsupported_param", &config), UCG_ERR_UNSUPPORTED);
}
#endif

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_planc_hccl_config, read_fail_malloc)
{
    ucg_planc_config_h config;
    std::vertor<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::CALLOC, result, "ucg planc hccl config");
    ASSERT_NE(ucg_planc_hccl_config_read(NULL, NULL, &config), UCG_OK);
}
#endif

TEST_F(test_planc_hccl_config, modify)
{
    ucg_planc_config_h config;
    ASSRET_EQ(ucg_planc_hccl_config_read(NULL, NULL, &config), UCG_OK);

    ucg_planc_hccl_config_t *cfg = (ucg_planc_hccl_config_t *)config;

    ASSERT_EQ(ucg_planc_hccl_config_modify(config, "WHITELIST_DISABLE", "0"), UCG_OK);
    ASSERT_EQ(cfg->whitelist_disable, 0);

    ucg_planc_hccl_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_hccl_config, modify_invalid_args)
{
    ucg_planc_config_h config;
    ASSERT_EQ(ucg_planc_hccl_config_read(NULL, NULL, &config), UCG_OK);

    ASSERT_EQ(ucg_planc_hccl_config_modify(config, NULL, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_planc_hccl_config_modify(config, "name", NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_planc_hccl_config_modify(config, NULL, "value"), UCG_ERR_INVALID_PARAM);

    ucg_planc_hccl_config_release(config);
}
#endif

TEST_F(test_planc_hccl_config, modify_non_exist)
{
    ucg_planc_config_h config;
    ASSERT_EQ(ucg_planc_hccl_config_read(NULL, NULL, &config), UCG_OK);

    ASSERT_NE(ucg_planc_hccl_config_modify(config, "NOT_EXIST", "value"), UCG_OK);

    ucg_planc_hccl_config_release(config);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_hccl_config, release_invalid_args)
{
    ucg_planc_hccl_config_release(NULL);
}
#endif

class test_planc_hccl_contest : public testing::Test {
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
        m_ucg_context.oob_group.size = 0;
        ucg_planc_hccl_config_read(NULL, NULL, &m_config);
    }

    static void TearDownTestSuite()
    {
        ucg_planc_hccl_config_release(m_config);

        if (aclrtResetDevice(0) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        if (aclFinalize() != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        stub::cleanup();
    }

    static ucg_planc_config_h m_config;
    static ucg_context_t m_ucg_context;
};
ucg_planc_config_h test_planc_hccl_contest::m_config = NULL;
ucg_context_t test_planc_hccl_contest::m_ucg_context;

TEST_F(test_planc_hccl_context, init_and_query)
{
    ucg_planc_context_h context;
    ucg_planc_params_t params;
    params.context = &m_ucg_context;
    ASSERT_EQ(ucg_planc_hccl_context_init(&params, m_config, &context), UCG_OK);

    ucg_planc_context_attr_t attr;
    attr.field_mask = UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN |
                      UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR;
    ASSERT_EQ(ucg_planc_hccl_context_query(context, &attr), UCG_OK);
    ASSERT_TRUE(attr.addr_len == 0);
    ASSERT_TRUE(attr.addr == NULL);
    ucg_planc_hccl_context_cleanup(context);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_planc_hccl_context, init_invalid_args)
{
    ASSERT_EQ(ucg_planc_hccl_context_init(NULL, NULL, NULL), UCG_ERR_INVALID_PARAM);
}
#endif

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_planc_hccl_context, init_fail_malloc)
{
    ucg_planc_context_h context;
    ucg_planc_params_t params;
    std::vertor<stub::routine_result_t> result = {stub::FAILURE};
    stub::mock(stub::CALLOC, result, "planc hccl contsxt");
    ASSERT_NE(ucg_planc_hccl_context_init(&params, m_config, &context), UCG_OK);
}
#endif