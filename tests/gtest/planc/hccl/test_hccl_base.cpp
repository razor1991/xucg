/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "acl/acl.h"

extern "C" {
#include "planc/hccl/planc_hccl_base.h"
}

class test_planc_hccl_base : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        if (aclInit(NULL) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        if (aclrtSetDevice(0) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }
    }

    static void TearDownTestSuite()
    {
        if (aclrtResetDevice(0) != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }

        if (aclFinalize() != ACL_SUCCESS) {
            printf("ACL error %s", aclGetRecentErrMsg());
            exit(1);
        }
    }

};

TEST_F(test_planc_hccl_base, query_invalid_args)
{
    ucg_mem_attr attr;
    attr.field_mask = 0;
    ASSERT_NE(ucg_planc_hccl_mem_query(NULL, &attr), UCG_OK);
}

TEST_F(test_planc_hccl_base, query_host_memory)
{
    void *ptr = malloc(128);
    ucg_mem_attr_t attr;
    attr.field_mask = UCG_MEM_ATTR_FIELD_MEM_TYPE;
    /* A failure is returned because the ptr is detected as host memory. */
    ASSERT_NE(ucg_planc_hccl_mem_query(ptr, &attr), UCG_OK);
    free(ptr);
}

TEST_F(test_planc_hccl_base, query_acl_host_memory)
{
    void *ptr;
    ASSERT_EQ(aclrtMallocHost(&ptr, 128), ACL_SUCCESS);
    ucg_mem_attr_t attr;
    attr.field_mask = UCG_MEM_ATTR_FIELD_MEM_TYPE;
    /* A failure is returned because the ptr is detected as host memory. */
    ASSERT_NE(ucg_planc_hccl_mem_query(ptr, &attr), UCG_OK);
    aclrFreeHost(ptr);
}

TEST_F(test_planc_hccl_base, query_acl_dev_memory)
{
    void *ptr;
    ASSERT_EQ(aclrtMalloc(&ptr, 128, ACL_MEM_MALLOC_HUGE_FIRST), ACL_SUCCESS);
    ucg_mem_attr_t attr;
    attr.field_mask = UCG_MEM_ATTR_FIELD_MEM_TYPE;
    ASSERT_EQ(ucg_planc_hccl_mem_query(ptr, &attr), UCG_OK);
    ASSERT_TRUE(attr.mem_type == UCG_MEM_TYPE_ACL);
    aclrFree(ptr);
}

TEST_F(test_planc_hccl_base, query_invalid_memory)
{
    void *ptr = (void*)123;
    ucg_mem_attr_t attr;
    attr.field_mask = UCG_MEM_ATTR_FIELD_MEM_TYPE;
    /* A failure is returned because the ptr is detected as host memory. */
    ASSERT_NE(ucg_planc_hccl_mem_query(ptr, &attr), UCG_OK);
}