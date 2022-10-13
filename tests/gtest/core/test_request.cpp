/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>

#include <ucg/api/ucg.h>
#include "stub.h"

extern "C" {
#include "core/ucg_request.h"
#include "core/ucg_group.h"
#include "core/ucg_plan.h"
#include "core/ucg_vgroup.h"
}

using namespace test;

static void test_ucg_request_complete_cb(void *arg, ucg_status_t status)
{
    int *complete = (int*)arg;
    *complete = 1;
    return;
}

class test_ucg_request : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init(true);
        // use planc fake to test.
        setenv("UCG_PLANC", "fake", 1);
        ucg_config_h config;
        ucg_config_read(NULL, NULL, &config);
        ucg_init(&test_stub_context_params, config, &m_context);
        ucg_config_release(config);
        ucg_group_create(m_context, &test_stub_group_params, &m_group);
    }

    static void TearDownTestSuite()
    {
        ucg_group_destroy(m_group);
        ucg_cleanup(m_context);
        stub::cleanup();
    }

public:
    static ucg_context_h m_context;
    static ucg_group_h m_group;
};
ucg_context_h test_ucg_request::m_context;
ucg_group_h test_ucg_request::m_context;

TEST_T(test_ucg_request, bcast)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    ASSERT_EQ(ucg_request_test(request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, bcast_init_unknown_mem_type)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info;
    ucg_request_h request = nullptr;

    // For bcast, UCG will check the memory type of send buffer internally
    // case 1: Unspecified field
    info.field_mask = 0;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);

    // case 2: specified field, but unknow type.
    info.field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE;
    info.mem.type = UCG_MEM_TYPE_UNKNOWN;
    ASSERT_EQ(ucg_request_bcast_init(&buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);

    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, allreduce)
{
    const int count = 10;
    int sendbuf[count] = {1};
    int recvbuf[count] = {1};
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_op_t op = {
        .type = UCG_OP_TYPE_MAX,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_allreduce_init(sendbuf, recvbuf, count, &dt,
                                         &op, m_group, &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    ASSERT_EQ(ucg_request_test(request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, allreduce_init_unknow_mem_type)
{
    const int count = 10;
    int sendbuf[count] = {1};
    int recvbuf[count] = {1};
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_op_t op = {
        .type = UCG_OP_TYPE_MAX,
    };
    ucg_request_info_t info;
    ucg_request_h request = nullptr;

    // For allreduce, UCG will check the memory type internally
    // case 1: Unspecified field
    info.field_mask = 0;
    ASSERT_EQ(ucg_request_allreduce_init(sendbuf, recvbuf, count, &dt,
                                         &op, m_group, &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);

    // case 2: specified field, but unknow type.
    info.field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE;
    info.mem.type = UCG_MEM_TYPE_UNKNOWN;
    ASSERT_EQ(ucg_request_allreduce_init(sendbuf, recvbuf, count, &dt,
                                         &op, m_group, &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);

    // case 3: Inconsistent memory types are not supported.
    ASSERT_EQ(ucg_request_allreduce_init(sendbuf, test_stub_acl_buffer, count, &dt,
                                         &op, m_group, &info, &request), UCG_OK);
}

TEST_T(test_ucg_request, allreduce)
{
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_barrier_init(m_group,&info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    ASSERT_EQ(ucg_request_test(request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, barrier_init_unknow_mem_type)
{
    ucg_request_info_t info;
    ucg_request_h request = nullptr;
    // For barrier, user must specify the field
    info.field_mask = 0;
    ASSERT_NE(ucg_request_barrier_init(m_group,&info, &request), UCG_OK);

    info.field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE;
    info.mem.type = UCG_MEM_TYPE_UNKNOWN;
    ASSERT_NE(ucg_request_barrier_init(m_group,&info, &request), UCG_OK);
}

TEST_T(test_ucg_request, alltoallv)
{
    const int count = 10;
    int sendbuf[count] = {1};
    int recvbuf[count] = {1};
    int sendcounts[count] = {count};
    int recvcounts[count] = {count};
    int sdispls[count] = {0};
    int rdispls[count] = {0};
    ucg_dt_t sendtype = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_dt_t recvtype = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_alltoallv_init(sendbuf, sendcounts, sdispls, &sendtype,
                                         recvbuf, recvcounts, rdispls, &recvtype,
                                         m_group, &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    ASSERT_EQ(ucg_request_test(request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, alltoallv_init_unknow_mem_type)
{
    const int count = 10;
    int sendbuf[count] = {1};
    int recvbuf[count] = {1};
    int sendcounts[count] = {count};
    int recvcounts[count] = {count};
    int sdispls[count] = {0};
    int rdispls[count] = {0};
    ucg_dt_t sendtype = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_dt_t recvtype = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info;
    ucg_request_h request = nullptr;

    // For alltoallv, UCG will check the memory type internally
    // case 1: Unspecified field
    info.field_mask = 0;
    ASSERT_EQ(ucg_request_alltoallv_init(sendbuf, sendcounts, sdispls, &sendtype,
                                         recvbuf, recvcounts, rdispls, &recvtype,
                                         m_group, &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);

    // case 2: specified field, but unknow type.
    info.field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE;
    info.mem.type = UCG_MEM_TYPE_UNKNOWN;
    ASSERT_EQ(ucg_request_alltoallv_init(sendbuf, sendcounts, sdispls, &sendtype,
                                         recvbuf, recvcounts, rdispls, &recvtype,
                                         m_group, &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);

    // case 3: Inconsistent memory types are not supported.
    ASSERT_EQ(ucg_request_alltoallv_init(sendbuf, sendcounts, sdispls, &sendtype,
                                         recvbuf, recvcounts, rdispls, &recvtype,
                                         m_group, &info, &request), UCG_OK);
}

TEST_T(test_ucg_request, init_fail_prepare)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    stub::mock(stub::PLAN_PREPARE, {stub::FAILURE});
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
}

TEST_T(test_ucg_request, start_inprogress)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    // The request that is in progress cannot be started again.
    ASSERT_NE(ucg_request_start(request), UCG_OK);

    ASSERT_EQ(ucg_request_test(request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, start_multi_times)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(ucg_request_start(request), UCG_OK);
        ASSERT_EQ(ucg_request_test(request), UCG_OK);
    }
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, test_ok)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    // For a request that has been completed, the test routine can still be invoked.
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(ucg_request_test(request), UCG_OK);
    }
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, cleanup_inprogress)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    // Cannot cleanup an active request.
    ASSERT_NE(ucg_request_cleanup(request), UCG_OK);

    ASSERT_EQ(ucg_request_test(request), UCG_OK);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, with_context_progress)
{
    const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE,
        .mem_type = UCG_MEM_TYPE_HOST,
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    ASSERT_EQ(ucg_progress(m_context), 1);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

TEST_T(test_ucg_request, complete_cb)
{
        const int count = 10;
    int buffer[count] = {1};
    ucg_rank_t root = 0;
    ucg_dt_t dt = {
        .type = UCG_DT_TYPE_INT32,
    };
    int complete = 0;
    ucg_request_info_t info = {
        .field_mask = UCG_REQUEST_INFO_FIELD_MEM_TYPE | UCG_REQUEST_INFO_FIELD_CB,
        .mem_type = UCG_MEM_TYPE_HOST,
        .complete_cb = {
            .cb = test_ucg_request_complete_cb,
            .arg = &complete,
        },
    };
    ucg_request_h request = nullptr;
    ASSERT_EQ(ucg_request_bcast_init(buffer, count, &dt, root, m_group,
                                     &info, &request), UCG_OK);
    ASSERT_EQ(ucg_request_start(request), UCG_OK);
    ASSERT_EQ(ucg_progress(m_context), 1);
    ASSERT_EQ(complete, 1);
    ASSERT_EQ(ucg_request_cleanup(request), UCG_OK);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_request, init_invalid_args)
{
    ucg_request_h request = nullptr;

    ASSERT_EQ(ucg_request_bcast_init(NULL, 0, NULL, 0, NULL, NULL, &request),
              UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_request_barrier_init(NULL, NULL, &request), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_request_allreduce_init(NULL, NULL, 0, NULL, NULL, NULL, NULL, &request),
              UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_request_bcast_init(NULL, NULL, NULL, NULL, NULL, NULL, NULL,
              NULL, NULL, NULL, &request), UCG_ERR_INVALID_PARAM);
}

TEST_T(test_ucg_request, start_invalid_args)
{
    ASSERT_EQ(ucg_request_start(NULL), UCG_ERR_INVALID_PARAM);
}

TEST_T(test_ucg_request, test_invalid_args)
{
    ASSERT_EQ(ucg_request_test(NULL), UCG_ERR_INVALID_PARAM);
}

TEST_T(test_ucg_request, cleanup_invalid_args)
{
    ASSERT_EQ(ucg_request_cleanup(NULL), UCG_ERR_INVALID_PARAM);
}
#endif