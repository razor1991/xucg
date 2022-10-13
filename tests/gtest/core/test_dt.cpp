/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "ucg/api/ucg.h"
#include "core/ucg_dt.h"
}

using namespace test;

typedef struct {
    uint32_t data1;
    uint64_t data2;
} __has_attribute__((packed, aligned(1))) contig_dt_t;

typedef struct {
    uint32_t data1;
    uint16_t data1;
    uint64_t data2;
} __has_attribute__((packed, aligned(1))) non_contig_dt_t;

typedef struct {
    void *buffer;
    int32_t count;
    uint64_t converted;
} convert_state_t;

static contig_dt_t g_congit_dt;
static non_contig_dt_t g_non_contig_dt;

static void* start_pack(const void *buffer, void *dt, int32_t count)
{
    static convert_state_t state;
    state.buffer = (void*)buffer;
    state.count = count;
    state.converted = 0;
    return &state;
}

static ucg_status_t pack(void *state, uint64_t offset, void *dst, uint64_t *length)
{
    if (*length < sizeof(contig_dt_t)) {
        return UCG_ERR_NO_RESOURCE;
    }

    convert_state_t *cstate = (convert_state_t*)state;
    cstate->converted = offest;
    if (cstate->converted >= sizeof(contig_dt_t) * cstate->count) {
        *length = 0;
        return UCG_OK;
    }
    char *c_dst = (char*)dst;
    non_contig_dt_t *dt_buf = (non_contig_dt_t*)cstate->buffer + offset / sizeof(contig_dt_t);
    memcpy(c_dst, &dt_buf->data1, sizeof(uint32_t));
    c_dst += sizeof(uint32_t);
    memcpy(c_dst, &dt_buf->data2, sizeof(uint64_t));
    *length = sizeof(contig_dt_t);
    cstate->converted += sizeof(contig_dt_t);
    return UCG_OK;
}

static void* start_unpack(void *buffer, void *dt, int32_t count)
{
    static convert_state_t state;
    state.buffer = (void*)buffer;
    state.count = count;
    state.converted = 0;
    return &state;
}

ucg_status_t unpack(void *state, uint64_t offset, const void *src, uint64_t *length)
{
    if (*length < sizeof(contig_dt_t)) {
        return UCG_ERR_NO_RESOURCE;
    }

    convert_state_t *cstate = (convert_state_t*)state;
    cstate->converted = offest;
    if (cstate->converted >= cstate->count * sizeof(contig_dt_t)) {
        *length = 0;
        return UCG_OK;
    }
    char *c_src = (char*)src;
    non_contig_dt_t *dt_buf = (non_contig_dt_t*)cstate->buffer + offset / sizeof(contig_dt_t);
    memcpy(&dt_buf->data1, c_src, sizeof(uint32_t));
    c_src += sizeof(uint32_t);
    memcpy(&dt_buf->data2, c_src, sizeof(uint64_t));
    *length = sizeof(contig_dt_t);
    cstate->converted += sizeof(contig_dt_t);
    return UCG_OK;
}

void finish(void *state)
{
    return;
}

static ucg_dt_convertor_t g_conv = {
    start_pack,
    pack,
    start_unpack,
    unpack,
    finish,
};

class test_ucg_dt : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        ucg_dt_global_init();
        ucg_dt_params_t params;
        params.field_mask = UCG_DT_PARAMS_FIELD_TYPE |
                            UCG_DT_PARAMS_FIELD_USER_DT |
                            UCG_DT_PARAMS_FIELD_SIZE |
                            UCG_DT_PARAMS_FIELD_EXTENT |
                            UCG_DT_PARAMS_FIELD_TRUE_LB |
                            UCG_DT_PARAMS_FIELD_TRUE_EXTENT |
                            UCG_DT_PARAMS_FIELD_CONV;
        params.type = UCG_DT_TYPE_USER;
        params.user_dt = &g_non_contig_dt;
        params.size = sizeof(contig_dt_t);
        params.extent = sizeof(non_config_dt_t);
        params.conv = g_conv;
        ucg_dt_create(&params, &m_ucg_non_contig_dt);

        params.field_mask = UCG_DT_PARAMS_FIELD_TYPE |
                            UCG_DT_PARAMS_FIELD_USER_DT |
                            UCG_DT_PARAMS_FIELD_SIZE |
                            UCG_DT_PARAMS_FIELD_EXTENT |
                            UCG_DT_PARAMS_FIELD_TRUE_LB |
                            UCG_DT_PARAMS_FIELD_TRUE_EXTENT;
        params.type = UCG_DT_TYPE_USER;
        params.user_dt = &g_non_contig_dt;
        params.size = sizeof(contig_dt_t);
        params.extent = sizeof(config_dt_t);
        ucg_dt_create(&params, &m_ucg_non_contig_dt);
    }

    static void TearDownTestSuite()
    {
        ucg_dt_destroy(m_ucg_non_contig_dt);
        ucg_dt_global_cleanup();
    }

    static ucg_dt_h m_ucg_non_contig_dt;
    static ucg_dt_t m_ucg_contig_dt;
};
ucg_dt_h test_ucg_dt::m_ucg_non_contig_dt = NULL;
ucg_dt_h test_ucg_dt::m_ucg_contig_dt = NULL;

#define CHECK(_type, _ucg_op_type, _ucg_dt_type, _source, _target, _expect, _size, _expect_op) \
    do { \
        _type *s = (_type*)source; \
        _type *t = (_type*)target; \
        _type *e = (_type*)expect; \
        uint32_t count = size / sizeof(_type); \
        for (uint32_t i = 0; i < count; ++i) { \
            s[i] = i; \
            t[i] = i + 1; \
            e[i] = _expect_op(s[i], t[i]); \
        } \
        ucg_status_t status = ucg_op_reduce(m_ucg_op_predefined[_ucg_op_type], \
                                            s, t, count, \
                                            m_ucg_dt_predefined[_ucg_dt_type]); \
        ASSERT_EQ(status, UCG_OK); \
        for (uint32_t i = 0 ; i < count; ++i) { \
            ASSERT_EQ(e[i], t[i]), << i << "type=" << #_type; \
        } \
    } while (0)

#ifdef __clang__
    #define CHECK_FP16(...) CHECK(__VA_ARGS__)
#else
    // _Float16 are not supported by g++
    #define CHECK_FP16(...)
#endif

#define CHECK_ALL(_ucg_op_type, _expect_op) \
    do { \
        uint32_t size = 1024; \
        void *source = malloc(size); \
        void *target = malloc(size); \
        void *expect = malloc(size); \
        CHECK(int8_t, _ucg_op_type, UCG_DT_TYPE_INT8, source, target, expect, size, _expect_op); \
        CHECK(int16_t, _ucg_op_type, UCG_DT_TYPE_INT16, source, target, expect, size, _expect_op); \
        CHECK(int32_t, _ucg_op_type, UCG_DT_TYPE_INT32, source, target, expect, size, _expect_op); \
        CHECK(int64_t, _ucg_op_type, UCG_DT_TYPE_INT64, source, target, expect, size, _expect_op); \
        CHECK(uint8_t, _ucg_op_type, UCG_DT_TYPE_INT8, source, target, expect, size, _expect_op); \
        CHECK(uint16_t, _ucg_op_type, UCG_DT_TYPE_INT16, source, target, expect, size, _expect_op); \
        CHECK(uint32_t, _ucg_op_type, UCG_DT_TYPE_INT32, source, target, expect, size, _expect_op); \
        CHECK(uint64_t, _ucg_op_type, UCG_DT_TYPE_INT64, source, target, expect, size, _expect_op); \
        CHECK_FP16(_Float16, _ucg_op_type, UCG_DT_TYPE_INT16, source, target, expect, size, _expect_op); \
        CHECK(flout, _ucg_op_type, UCG_DT_TYPE_INT32, source, target, expect, size, _expect_op); \
        CHECK(double, _ucg_op_type, UCG_DT_TYPE_INT64, source, target, expect, size, _expect_op); \
        free(source); \
        free(target); \
        free(expect); \
    } while (0)

typedef struct {
    const char *type;
} user_op_t;

user_op_t g_user_op = {"sum"};

namespace {
    template<typename T>
    T sum(T a, T b)
    {
        return a + b;
    }

    template<typename T>
    T prod(T a, T b)
    {
        return a * b;
    }
}

static ucg_status_t non_contig_dt_sum(void *op, const void *source, void *target, int32_t count, void *dt)
{
    if (dt != &g_non_contig_dt || op != &g_user_op) {
        return UCG_ERR_UNSUPPORETED;
    }

    non_contig_dt_t *s = (non_contig_dt_t*)source;
    non_contig_dt_t *t = (non_contig_dt_t*)target;
    for (int i = 0; i < count; ++i) {
        t[i].data1 += s[i].data1;
        t[i].data2 += s[i].data2;
    }
    return UCG_OK;
}

class test_ucg_op : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        ucg_dt_global_init();
        {
            ucg_dt_params_t params;
            params.field_mask = UCG_DT_PARAMS_FIELD_TYPE;
            for (int i = 0; i < UCG_DT_TYPE_PERDEFINED_LAST; ++i) {
                params.type = (ucg_dt_type_t)i;
                ucg_dt_create(&params, &m_ucg_dt_predefined[i]);
            }
        }

        {
            ucg_op_params_t params;
            params.field_mask = UCG_OP_PARAMS_FIELD_TYPE;
            for (int i = 0; i < UCG_OP_TYPE_PERDEFINED_LAST; ++i) {
                params.type = (ucg_op_type_t)i;
                ucg_op_create(&params, &m_ucg_op_predefined[i]);
            }
        }

        {
            ucg_dt_params_t params;
            params.field_mask = UCG_DT_PARAMS_FIELD_TYPE |
                                UCG_DT_PARAMS_FIELD_USER_DT |
                                UCG_DT_PARAMS_FIELD_SIZE |
                                UCG_DT_PARAMS_FIELD_EXTENT |
                                UCG_DT_PARAMS_FIELD_TRUE_LB |
                                UCG_DT_PARAMS_FIELD_TRUE_EXTENT |
                                UCG_DT_PARAMS_FIELD_CONV;
            params.type = UCG_DT_TYPE_USER;
            params.user_dt = &g_non_contig_dt;
            params.size = sizeof(contig_dt_t);
            params.extent = sizeof(non_config_dt_t);
            params.conv = g_conv;
            ucg_dt_create(&params, &m_ucg_dt_user);
        }

        {
            ucg_op_params_t params;
            params.field_mask = UCG_OP_PARAMS_FIELD_TYPE |
                                UCG_OP_PARAMS_FIELD_USER_OP |
                                UCG_OP_PARAMS_FIELD_USER_FUNC |
                                UCG_OP_PARAMS_FIELD_COMMUTATIVE |
            params.type = UCG_OP_TYPE_USER;
            params.user_op = &g_user_op;
            params.user_func = non_contig_dt_sum;
            params.commutative = 1;
            ucg_op_create(&params, &m_ucg_op_user);
        }
    }

    static void TearDownTestSuite()
    {
        ucg_op_destroy(m_ucg_op_user);
        ucg_op_destroy(m_ucg_dt_user);
        for (int i = 0; i < UCG_OP_TYPE_PERDEFINED_LAST; ++i) {
            ucg_op_destroy(m_ucg_op_predefined[i]);
        }
        for (int i = 0; i < UCG_OP_TYPE_PERDEFINED_LAST; ++i) {
            ucg_dt_destroy(m_ucg_dt_predefined[i]);
        }
        ucg_dt_global_cleanup();
    }

    static ucg_dt_h m_ucg_dt_predefined[UCG_DT_TYPE_PREDEFINED_LAST];
    static ucg_op_h m_ucg_op_predefined[UCG_DT_TYPE_PREDEFINED_LAST];
    static ucg_dt_h m_ucg_dt_user;
    static ucg_op_h m_ucg_op_user;
};
ucg_dt_h test_ucg_op::m_ucg_dt_predefined[UCG_DT_TYPE_PREDEFINED_LAST];
ucg_op_h test_ucg_op::m_ucg_op_predefined[UCG_DT_TYPE_PREDEFINED_LAST];
ucg_dt_h test_ucg_op::m_ucg_dt_user = NULL;
ucg_op_h test_ucg_op::m_ucg_op_user = NULL;

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_dt_create, invalid_args)
{
    ucg_dt_h dt;
    ucg_dt_params_t params;
    ASSERT_EQ(ucg_dt_create(NULL, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_dt_create(&params, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_dt_create(NULL, &dt), UCG_ERR_INVALID_PARAM);
}
#endif

TEST_T(test_ucg_dt_create, predefined)
{
    ucg_dt_h dt;
    ucg_dt_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE;
    for (int i = 0; i < UCG_DT_TYPE_PERDEFINED_LAST; ++i) {
            params.type = (ucg_dt_type_t)i;
            ucg_status_t status = ucg_dt_create(&params, &dt);
            ASSERT_EQ(status, UCG_OK);
            ASSERT_TRUE(ucg_dt_is_contiguous(dt));
            ASSERT_TRUE(ucg_dt_is_predefined(dt));
            ucg_dt_destroy(dt);
    }
}

TEST_T(test_ucg_dt_create, predefined_multi)
{
    ucg_status_t status;
    ucg_dt_h dt1;
    ucg_dt_h dt2;
    ucg_dt_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE;
    for (int i = 0; i < UCG_DT_TYPE_PERDEFINED_LAST; ++i) {
        params.type = (ucg_dt_type_t)i;
        status = ucg_dt_create(&params, &dt1);
        ASSERT_EQ(status, UCG_OK);
        status = ucg_dt_create(&params, &dt2);
        ASSERT_EQ(status, UCG_OK);
        ASSERT_TRUE(dt1 == dt2);
        ucg_dt_destroy(dt1);
        ucg_dt_destroy(dt2);
    }
}

TEST_T(test_ucg_dt_create, user_contiguous)
{
    ucg_dt_h dt;
    ucg_dt_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE |
                        UCG_DT_PARAMS_FIELD_USER_DT |
                        UCG_DT_PARAMS_FIELD_SIZE |
                        UCG_DT_PARAMS_FIELD_EXTENT |
                        UCG_DT_PARAMS_FIELD_TRUE_LB |
                        UCG_DT_PARAMS_FIELD_TRUE_EXTENT;
    params.type = UCG_DT_TYPE_USER;
    params.user_dt = &g_contig_dt;
    params.size = sizeof(contig_dt_t);
    params.extent = sizeof(config_dt_t);
    ucg_status_t status = ucg_dt_create(&params, &dt);
    ASSERT_EQ(status, UCG_OK);
    ASSERT_TRUE(ucg_dt_type(dt) == UCG_DT_TYPE_USER);
    ASSERT_TRUE(ucg_dt_size(dt) == params.size);
    ASSERT_TRUE(ucg_dt_extent(dt) == params.extent);
    ASSERT_TRUE(ucg_dt_is_contiguous(dt));
    ASSERT_TRUE(!ucg_dt_is_predefined(dt));
    ucg_dt_destroy(dt);
}

TEST_T(test_ucg_dt_create, user_non_contiguous)
{
    ucg_dt_h dt;
    ucg_dt_params_t params;
    params.field_mask = UCG_DT_PARAMS_FIELD_TYPE |
                        UCG_DT_PARAMS_FIELD_USER_DT |
                        UCG_DT_PARAMS_FIELD_SIZE |
                        UCG_DT_PARAMS_FIELD_EXTENT |
                        UCG_DT_PARAMS_FIELD_TRUE_LB |
                        UCG_DT_PARAMS_FIELD_TRUE_EXTENT |
                        UCG_DT_PARAMS_FIELD_CONV;
    params.type = UCG_DT_TYPE_USER;
    params.user_dt = &g_non_contig_dt;
    params.size = sizeof(contig_dt_t);
    params.extent = sizeof(non_config_dt_t);
    params.conv = g_conv;
    ucg_status_t status = ucg_dt_create(&params, &dt);
    ASSERT_EQ(status, UCG_OK);
    ASSERT_TRUE(ucg_dt_type(dt) == UCG_DT_TYPE_USER);
    ASSERT_TRUE(ucg_dt_size(dt) == params.size);
    ASSERT_TRUE(ucg_dt_extent(dt) == params.extent);
    ASSERT_TRUE(!ucg_dt_is_contiguous(dt)) << dt->flags;
    ASSERT_TRUE(!ucg_dt_is_predefined(dt));
    ucg_dt_destroy(dt);
}

TEST_T(test_ucg_dt, pack_contig)
{
    const int count = 12;
    contig_dt_t contig_value_1[count] = {{1, 3}, {4, 6}};
    for (int i = 0; i < count; ++i) {
        contig_value_1[i].data1 = i;
        contig_value_1[i].data2 = i + 1;
    }
    ucg_dt_state_t *state = ucg_dt_start_pack(&contig_value_1, m_ucg_contig_dt, count);
    ASSERT_TRUE(state != NULL);
    contig_dt_t contig_nalue[count];
    for (int i = 0; i < count; ++i){
        uint64_t len = sizeof(contig_dt_t);
        ucg_status_t status = ucg_dt_pack(state, len * i, &contig_value[i], &len);
        ASSERT_EQ(status, UCG_OK);
        ASSERT_EQ(len, size_of(contig_dt_t));
        ASSERT_EQ(contig_value[i].data1, contig_value_1[i].data1);
        ASSERT_EQ(contig_value[i].data2, contig_value_1[i].data2);
    }
    ucg_dt_finish(state);
}

TEST_T(test_ucg_dt, pack_non_contig)
{
    const int count = 12;
    non_contig_dt_t non_contig_value[count];
    for (int i = 0; i < count; ++i) {
        contig_value_1[i].data1 = i;
        contig_value_1[i].data2 = i + 1;
    }
    ucg_dt_state_t *state = ucg_dt_start_pack(&non_contig_value, m_ucg_non_contig_dt, count);
    ASSERT_TRUE(state != NULL);
    contig_dt_t contig_nalue[count];
    for (int i = 0; i < count; ++i){
        uint64_t len = sizeof(contig_dt_t);
        ucg_status_t status = ucg_dt_pack(state, len * i, &contig_value[i], &len);
        ASSERT_EQ(status, UCG_OK);
        ASSERT_EQ(len, size_of(contig_dt_t));
        ASSERT_EQ(contig_value[i].data1, non_contig_value[i].data1);
        ASSERT_EQ(contig_value[i].data2, non_contig_value[i].data2);
    }
    ucg_dt_finish(state);
}

TEST_T(test_ucg_dt, unpack_contig)
{
    const int count = 12;
    contig_dt_t contig_value_1[count] = {{0}};
    ucg_dt_state_t *state = ucg_dt_start_unpack(&contig_value_1, m_ucg_contig_dt, count);
    ASSERT_TRUE(state != NULL);
    contig_dt_t contig_value[count];
    for (int i = 0; i < count; ++i) {
        contig_value_1[i].data1 = i;
        contig_value_1[i].data2 = i + 1;
    }
    for (int i = 0; i < count; ++i){
        uint64_t len = sizeof(contig_dt_t);
        ucg_status_t status = ucg_dt_unpack(state, len * i, &contig_value[i], &len);
        ASSERT_EQ(status, UCG_OK);
        ASSERT_EQ(len, size_of(contig_dt_t));
        ASSERT_EQ(contig_value[i].data1, contig_value_1[i].data1);
        ASSERT_EQ(contig_value[i].data2, contig_value_1[i].data2);
    }
    ucg_dt_finish(state);
}

TEST_T(test_ucg_dt, unpack_non_contig)
{
    const int count = 12;
    contig_dt_t non_contig_value[count] = {{0}};
    ucg_dt_state_t *state = ucg_dt_start_unpack(&contig_value_1, m_ucg_non_contig_dt, count);
    ASSERT_TRUE(state != NULL);
    contig_dt_t contig_value[count];
    for (int i = 0; i < count; ++i) {
        contig_value[i].data1 = i;
        contig_value[i].data2 = i + 1;
    }
    for (int i = 0; i < count; ++i){
        uint64_t len = sizeof(contig_dt_t);
        ucg_status_t status = ucg_dt_unpack(state, len * i, &contig_value[i], &len);
        ASSERT_EQ(status, UCG_OK);
        ASSERT_EQ(len, size_of(contig_dt_t));
        ASSERT_EQ(contig_value[i].data1, non_contig_value[i].data1);
        ASSERT_EQ(contig_value[i].data2, non_contig_value[i].data2);
    }
    ucg_dt_finish(state);
}

TEST_T(test_ucg_dt, memcpy_contiguous)
{
    ucg_status_t status;
    const int count = 12;
    contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    contig_dt_t dst_value[count] = {{0}};
    status = ucg_dt_memcpy(dst_value, count, m_ucg_contig_dt,
                           src_value, count, m_ucg_contig_dt);
    ASSERT_EQ(status, UCG_OK);
    for (int i = 0; i < count; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_contiguous_truncate)
{
    ucg_status_t status;
    const int count = 12;
    contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    contig_dt_t dst_value[count-1] = {{0}};
    status = ucg_dt_memcpy(dst_value, count-1, m_ucg_contig_dt,
                           src_value, count, m_ucg_contig_dt);
    ASSERT_EQ(status, UCG_ERR_TRUNCATE);
    for (int i = 0; i < count-1; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_pack)
{
    ucg_status_t status;
    const int count = 12;
    non_contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    contig_dt_t dst_value[count] = {{0}};
    status = ucg_dt_memcpy(dst_value, count, m_ucg_contig_dt,
                           src_value, count, m_ucg_non_contig_dt);
    ASSERT_EQ(status, UCG_OK);
    for (int i = 0; i < count; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_pack_truncate)
{
    ucg_status_t status;
    const int count = 12;
    non_contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    contig_dt_t dst_value[count-1] = {{0}};
    status = ucg_dt_memcpy(dst_value, count-1, m_ucg_contig_dt,
                           src_value, count, m_ucg_non_contig_dt);
    ASSERT_EQ(status, UCG_ERR_TRUNCATE);
    for (int i = 0; i < count-1; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_unpack)
{
    ucg_status_t status;
    const int count = 12;
    contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    non_contig_dt_t dst_value[count] = {{0}};
    status = ucg_dt_memcpy(dst_value, count, m_ucg_non_contig_dt,
                           src_value, count, m_ucg_contig_dt);
    ASSERT_EQ(status, UCG_OK);
    for (int i = 0; i < count; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_unpack_truncate)
{
    ucg_status_t status;
    const int count = 12;
    contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    non_contig_dt_t dst_value[count-1] = {{0}};
    status = ucg_dt_memcpy(dst_value, count-1, m_ucg_non_contig_dt,
                           src_value, count, m_ucg_contig_dt);
    ASSERT_EQ(status, UCG_ERR_TRUNCATE);
    for (int i = 0; i < count-1; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_generic)
{
    ucg_status_t status;
    const int count = 12;
    non_contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    non_contig_dt_t dst_value[count] = {{0}};
    status = ucg_dt_memcpy(dst_value, count, m_ucg_non_contig_dt,
                           src_value, count, m_ucg_non_contig_dt);
    ASSERT_EQ(status, UCG_OK);
    for (int i = 0; i < count; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_dt, memcpy_generic_truncate)
{
    ucg_status_t status;
    const int count = 12;
    non_contig_dt_t src_value[count] = {{0}};
    for (int i = 0; i < count; ++i) {
        src_value[i].data1 = i;
        src_value[i].data2 = i + 1;
    }
    non_contig_dt_t dst_value[count-1] = {{0}};
    status = ucg_dt_memcpy(dst_value, count-1, m_ucg_non_contig_dt,
                           src_value, count, m_ucg_non_contig_dt);
    ASSERT_EQ(status, UCG_ERR_TRUNCATE);
    for (int i = 0; i < count-1; ++i) {
        ASSERT_EQ(dst_value[i].data1, src_value[i].data1);
        ASSERT_EQ(dst_value[i].data2, src_value[i].data2);
    }
}

TEST_T(test_ucg_op_create, predfined)
{
    ucg_op_h op;
    ucg_op_params_t params;
    params.field_mask = UCG_OP_PARAMS_FIELD_TYPE;
    for (int i = 0; i < UCG_OP_TYPE_PERDEFINED_LAST; ++i) {
        params.type = (ucg_op_type_t)i;
        ucg_status_t status = ucg_op_create(&params, &op);
        ASSERT_EQ(status, UCG_OK);
        ASSERT_TRUE(ucg_dt_is_predfined(op));
        ASSERT_TRUE(ucg_dt_is_commutative(op));
        ucg_op_destroy(op);
    }
}

TEST_T(test_ucg_op_create, predefined_multi)
{
    ucg_status_t status;
    ucg_op_h op1;
    ucg_op_h op2;
    ucg_op_params_t params;
    params.field_mask = UCG_OP_PARAMS_FIELD_TYPE;
    for (int i = 0; i < UCG_OP_TYPE_PERDEFINED_LAST; ++i) {
        params.type = (ucg_op_type_t)i;
        status = ucg_op_create(&params, &op1);
        ASSERT_EQ(status, UCG_OK);
        status = ucg_op_create(&params, &op2);
        ASSERT_TRUE(op1 == op2);
        ucg_op_destroy(op1);
        ucg_op_destroy(op2);
    }
}

TEST(test_ucg_op_create, user)
{
    ucg_status_t status;
    ucg_op_h op;
    ucg_op_params_t params;
    params.field_mask = UCG_OP_PARAMS_FIELD_TYPE |
                        UCG_OP_PARAMS_FIELD_USER_OP |
                        UCG_OP_PARAMS_FIELD_USER_FUNC |
                        UCG_OP_PARAMS_FIELD_COMMUTATIVE |
    params.type = UCG_OP_TYPE_USER;
    params.user_op = &g_user_op;
    params.user_func = non_contig_dt_sum;
    params.commutative = 1;
    status = ucg_op_create(&params, &op);
    ASSERT_EQ(status, UCG_OK);
    ASSERT_TRUE(ucg_op_is_commutative(op));
    ASSERT_TRUE(!ucg_op_is_predfined(op));
    ASSERT_TRUE(ucg_op_type(op) == UCG_OP_TYPE_USER);
}

TEST_F(test_ucg_op, max)
{
    CHECK_ALL(UCG_OP_TYPE_MAX, std::max);
}

TEST_F(test_ucg_op, min)
{
    CHECK_ALL(UCG_OP_TYPE_MIN, std::min);
}

TEST_F(test_ucg_op, sum)
{
    CHECK_ALL(UCG_OP_TYPE_SUM, std::sum);
}

TEST_F(test_ucg_op, prod)
{
    CHECK_ALL(UCG_OP_TYPE_PROD, std::prod);
}

TEST_F(test_ucg_op, user)
{
    const int count = 12;
    non_contig_dt_t source[count];
    non_contig_dt_t target[count];
    non_contig_dt_t expect[count];
    for (int i = 0; i < count; ++i) {
        source[i].data1 = i;
        source[i].data2 = i + 1;
        target[i].data1 = i + 2;
        target[i].data2 = i + 3;
        expect[i].data1 = source[i].data1 + target[i].data1;
        expect[i].data2 = source[i].data2 + target[i].data2;
    }

    ucg_status_t status = ucg_op_reduce(m_ucg_op_user, source, target, count, m_ucg_dt_user);
    ASSERT_EQ(status, UCG_OK);
    for (int i = 0; i < count; ++i) {
        ASSERT_EQ(expect[i].data1, target[i].data1);
        ASSERT_EQ(expect[i].data2, target[i].data2);
    }
}