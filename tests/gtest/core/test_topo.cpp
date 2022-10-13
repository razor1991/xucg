/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/
#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "core/ucg_topo.h"
#include "core/ucg_rank_map.h"
}

using namespace test;

// runtime environment
static const int32_t n_subnet = 8; // subnet num
static const int32_t n_nodes = 1024; // > 1022 to test more branches
static const int32_t n_sockets = 2; // sockets per node
// process information
static const int32_t ppn = 10; // processes per node
static const int32_t pps = ppn / n_sockets; // processes per socket
static const int32_t n_proc = ppn * n_nodes;

static ucg_status_t test_topo_get_location_fail(ucg_group_t *group, ucg_rank_t rank, ucg_location_t *location)
{
    return UCG_ERR_INVALID_PARAM;
}

static ucg_status_t test_topo_get_location(ucg_group_t *group, ucg_rank_t rank, ucg_location_t *location)
{
    location->field_mask = UCG_LOCATION_FIELD_SUBNET_ID |
                           UCG_LOCATION_FIELD_NODE_ID |
                           UCG_LOCATION_FIELD_SOCKET_ID;
    // rank-by core
    location->subnet_id = rank / (n_proc / n_subnet);
    location->node_id = (rank / ppn) % n_nodes;
    location->socket_id = ((rank - location->node_id * ppn) / pps) % n_sockets;
    return UCG_OK;
}

static ucg_status_t test_topo_get_location_no_node_id(ucg_group_t *group, ucg_rank_t rank, ucg_location_t *location)
{
    location->field_mask = UCG_LOCATION_FIELD_SOCKET_ID;
    location->node_id = (rank / ppn) % n_nodes;
    location->socket_id = ((rank - location->node_id * ppn) / pps) % n_sockets;
    return UCG_OK;
}

static ucg_status_t test_topo_get_location_no_socket_id(ucg_group_t *group, ucg_rank_t rank, ucg_location_t *location)
{
    location->field_mask = UCG_LOCATION_FIELD_SOCKET_ID;
    location->node_id = (rank / ppn) % n_nodes;
    location->socket_id = ((rank - location->node_id * ppn) / pps) % n_sockets;
    return UCG_OK;
}

class test_ucg_topo : public ::testing::Test {
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

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_topo, init_invalid_args)
{
    ucg_topo_params_t params;

    ASSERT_EQ(ucg_topo_init(NULL, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_topo_init(&params, NULL), UCG_ERR_INVALID_PARAM);
}

TEST_T(test_ucg_topo, cleanup_invalid_args)
{
    // expect no segmentation fault.
    ucg_topo_cleanup(NULL);
}

TEST_T(test_ucg_topo, get_group_invalid_args)
{
    // NULL topo
    ASSERT_TRUE(ucg_topo_get_group(NULL, UCG_TOPO_GROUP_TYPE_NODE) == NULL);

    // invalid group type
    ucg_topo_t *topo = (ucg_topo_t*)1;
    ASSERT_TRUE(ucg_topo_get_group(NULL, UCG_TOPO_GROUP_TYPE_LAST) == NULL);
}
#endif

TEST_T(test_ucg_topo, init)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = 10;

    ucg_topo_params_t params;
    params.group = NULL;
    params.myrank = 3;
    params.rank_map = &map;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);
    ucg_topo_cleanup(topo);
}

#ifdef UCG_ENABLE_DEBUG
TEST_T(test_ucg_topo, init_fail_malloc)
{
    ucg_topo_t *topo;
    ucg_rank_map_t map;
    ucg_topo_params_t params;
    params.group = NULL;
    params.myrank = 3;
    params.rank_map = &map;
    params.get_location = test_topo_get_location;

    // malloc failed
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = 10;
    stub::mock(stub::CALLOC, {stub::FAILURE}, "ucg topo");
    ASSERT_NE(ucg_topo_init(&params, &topo), UCG_OK);

    // rank map copy failed.
    ucg_rank_t ranks[5] = {1, 2, 5, 6, 9};
    map.type = UCG_RANK_MAP_TYPE_ARRAY;
    map.size = 5;
    map.array = ranks;
    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg rank-map array");
    ASSERT_NE(ucg_topo_init(&params, &topo), UCG_OK);
}
#endif

TEST_T(test_ucg_topo, init_fail_location)
{
    ucg_topo_t *topo;

    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = 10;

    ucg_topo_params_t params;
    params.group = NULL;
    params.myrank = 3;
    params.rank_map = &map;
    params.get_location = test_topo_get_location_fail;

    ASSERT_NE(ucg_topo_init(&params, &topo), UCG_OK);
}

TEST_T(test_ucg_topo, get_socket_group_only)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = n_proc;

    ucg_topo_params_t params;
    params.group = NULL;
    params.rank_map = &map;
    params.myrank = 0;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    // socket group depends on node group which will be created automatically.
    ucg_topo_group_t *group;
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET);
    ASSERT_TRUE(group != NULL);
    ucg_topo_cleanup(topo);
}

TEST_T(test_ucg_topo, get_socket_leader_group_only)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = n_proc;

    ucg_topo_params_t params;
    params.group = NULL;
    params.rank_map = &map;
    params.myrank = 0;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    // socket leader group depends on node group which will be created automatically.
    ucg_topo_group_t *group;
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
    ASSERT_TRUE(group != NULL);
    ucg_topo_cleanup(topo);
}

TEST_T(test_ucg_topo, get_group_fail_location)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = 10;

    ucg_topo_params_t params;
    params.group = NULL;
    params.myrank = 3;
    params.rank_map = &map;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    topo->get_location = test_topo_get_location_fail;
    ucg_topo_group_t *group;
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NODE);
    ASSERT_TRUE(group == NULL);
    // Should still be NULL
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NODE);
    ASSERT_TRUE(group == NULL);
    ucg_topo_cleanup(topo);
}

TEST_T(test_ucg_topo, get_group_no_node_id)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = 10;

    ucg_topo_params_t params;
    params.group = NULL;
    params.myrank = 3;
    params.rank_map = &map;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    topo->get_location = test_topo_get_location_no_node_id;
    ucg_topo_group_t *group;
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NODE);
    ASSERT_TRUE(group == NULL);
    // Should still be NULL
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NODE);
    ASSERT_TRUE(group == NULL);
    ucg_topo_cleanup(topo);
}

TEST_T(test_ucg_topo, get_group_no_socket_id)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = 10;

    ucg_topo_params_t params;
    params.group = NULL;
    params.myrank = 3;
    params.rank_map = &map;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    topo->get_location = test_topo_get_location_no_socket_id;
    ucg_topo_group_t *group;
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET);
    ASSERT_TRUE(group == NULL);
    // Should still be NULL
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET);
    ASSERT_TRUE(group == NULL);
    ucg_topo_cleanup(topo);
}

#ifdef UCG_ENABLE_DEBUG
TEST_T(test_ucg_topo, get_group_fail_malloc)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = n_proc;

    ucg_topo_params_t params;
    params.group = NULL;
    params.rank_map = &map;
    params.myrank = 0;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    ucg_topo_group_t *group;
    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg arrayx init");
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
    ASSERT_TRUE(group == NULL);

    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg array init");
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
    ASSERT_TRUE(group == NULL);

    // The initial capacity of filter is 1022. However, there are 1024 node leaders.
    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg array extend");
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NODE_LEADER);
    ASSERT_TRUE(group == NULL);

    // The initial capacity of ranks is 256. However, there are 10240 processes.
    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg arrayx extend");
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NET);
    ASSERT_TRUE(group == NULL);
    ucg_topo_cleanup(topo);
}
#endif

#ifdef UCG_ENABLE_DEBUG
TEST_T(test_ucg_topo, get_group_fail_malloc_and_retry)
{
    ucg_rank_map_t map;
    map.type = UCG_RANK_MAP_TYPE_FULL;
    map.size = n_proc;

    ucg_topo_params_t params;
    params.group = NULL;
    params.rank_map = &map;
    params.myrank = 0;
    params.get_location = test_topo_get_location;

    ucg_topo_t *topo;
    ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);

    ucg_topo_group_t *group;
    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg arrayx init");
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
    ASSERT_TRUE(group == NULL);

    // Retry should succeed
    group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
    ASSERT_TRUE(group == NULL);
    ucg_topo_cleanup(topo);
}
#endif

class test_ucg_topo_get_group : public ::testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init();
        ucg_rank_map_t map;
        map.type = UCG_RANK_MAP_TYPE_FULL;
        map.size = n_proc;

        ucg_topo_params_t params;
        params.group = NULL;
        params.rank_map = &map;
        params.get_location = test_topo_get_location;

        for (int i = 0; i < n_proc; ++i) {
            // all processes need to initialize topology.
            params.myrank = 1;
            ucg_topo_t *topo;
            ASSERT_EQ(ucg_topo_init(&params, &topo), UCG_OK);
            m_topos.push_back(topo);
        }
    }

    static void TearDownTestSuite()
    {
        stub::cleanup();
        for (int i = 0; i < n_proc; ++i) {
            ucg_topo_cleanup(m_topos[i]);
        }
    }

public:
    static std::vector<ucg_topo_t*> m_topos;
};
std::vector<ucg_topo_t*> test_ucg_topo_get_group::m_topos;

TEST_T(test_ucg_topo_get_group, net)
{
    ucg_topo_group_t *group;
    for (int i = 0; i < n_proc; ++i) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_NET);
        ASSERT_TRUE(group != NULL);
        ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
        ASSERT_EQ(group->super.size, n_proc);
        ASSERT_EQ(group->super.myrank, i);
        ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
    }
}

TEST_T(test_ucg_topo_get_group, subnet)
{
    ucg_topo_group_t *group;
    for (int i = 0; i < n_proc; ++i) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_SUBNET);
        ASSERT_TRUE(group != NULL);
        ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
        ASSERT_EQ(group->super.size, n_proc / n_subnet);
        ASSERT_EQ(group->super.myrank, i % (n_proc / n_subnet));
        ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
    }
}

TEST_T(test_ucg_topo_get_group, subnet_leader)
{
    ucg_topo_group_t *group;
    // Iterating all processes takes too much time, so set stride to 45.
    // Some of the selected processes are leaders, while others are not.
    for (int i = 0; i < n_proc; i += 45) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_SUBNET_LEADER);
        ASSERT_TRUE(group != NULL);
        // Process whose node rank is 0 is selected as the leader.
        if (i % (n_proc / n_subnet) == 0) {
            ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
            ASSERT_EQ(group->super.size, n_proc / n_subnet);
            ASSERT_EQ(group->super.myrank, i % (n_proc / n_subnet));
            ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
        } else {
            ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_DISABLE);
        }
    }
}

TEST_T(test_ucg_topo_get_group, node)
{
    ucg_topo_group_t *group;
    for (int i = 0; i < n_proc; ++i) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_NODE);
        ASSERT_TRUE(group != NULL);
        ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
        ASSERT_EQ(group->super.size, ppn);
        ucg_rank_t node_rank = i - i / ppn * ppn;
        ASSERT_EQ(group->super.myrank, node_rank);
        ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
    }
}

TEST_T(test_ucg_topo_get_group, node_leader)
{
    ucg_topo_group_t *group;
    // Iterating all processes takes too much time, so set stride to 45.
    // Some of the selected processes are leaders, while others are not.
    for (int i = 0; i < n_proc; i += 45) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_NODE_LEADER);
        ASSERT_TRUE(group != NULL);
        // Process whose node rank is 0 is selected as the leader.
        if (i % ppn == 0) {
            ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
            ASSERT_EQ(group->super.size, n_nodes);
            ASSERT_EQ(group->super.myrank, i / ppn);
            ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
        } else {
            ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_DISABLE);
        }
    }
}

TEST_T(test_ucg_topo_get_group, socket)
{
    ucg_topo_group_t *group;
    for (int i = 0; i < n_proc; ++i) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_SOCKET);
        ASSERT_TRUE(group != NULL);
        ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
        ASSERT_EQ(group->super.size, ppn);
        ucg_rank_t node_rank = i - i / ppn * ppn;
        ucg_rank_t socket_rank =node_rank - node_rank / pps * pps;
        ASSERT_EQ(group->super.myrank, socket_rank);
        ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
    }
}

TEST_T(test_ucg_topo_get_group, socket_leader)
{
    ucg_topo_group_t *group;
    for (int i = 0; i < n_proc; ++i) {
        group = ucg_topo_get_group(m_topos[i], UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
        ASSERT_TRUE(group != NULL);
        if (i % ppn == 0) {
            ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_ENABLE);
            ASSERT_EQ(group->super.size, n_sockets);
            ucg_rank_t node_rank = i - i / ppn * ppn;
            ASSERT_EQ(group->super.myrank, node_rank / pps);
            ASSERT_EQ(ucg_rank_map_eval(&group->super.rank_map, group->super.myrank), i);
        } else {
            ASSERT_EQ(group->state, UCG_TOPO_GROUP_STATE_DISABLE);
        }
    }
}
