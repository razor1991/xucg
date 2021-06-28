/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_H_
#define UCG_H_

#include <ucg/api/ucg_def.h>
#include <ucg/api/ucg_version.h>

#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include <ucs/type/thread_mode.h>
#include <ucs/type/cpu_set.h>
#include <ucs/config/types.h>
#include <ucs/sys/compiler_def.h>
#include <stdio.h>
#include <sys/types.h>

BEGIN_C_DECLS

/**
 * @defgroup UCG_API Unified Communication Protocol (UCG) API
 * @{
 * This section describes UCG API.
 * @}
 */
#define ucg_context_h               ucp_context_h
#define ucg_config_t                ucp_config_t
#define ucg_address_t               ucp_address_t
#define ucg_worker_h                ucp_worker_h
#define ucg_params_t                ucp_params_t
#define ucg_context_attr_t          ucp_context_attr_t
#define ucg_worker_attr_t           ucp_worker_attr_t
#define ucg_worker_params_t         ucp_worker_params_t

#define ucg_config_read             ucp_config_read
#define ucg_config_release          ucp_config_release
#define ucg_config_modify           ucp_config_modify
#define ucg_config_print            ucp_config_print
#define ucg_get_version             ucp_get_version
#define ucg_get_version_string      ucp_get_version_string
#define ucg_cleanup                 ucp_cleanup
#define ucg_context_query           ucp_context_query
#define ucg_context_print_info      ucp_context_print_info
#define ucg_worker_destroy          ucp_worker_destroy
#define ucg_worker_query            ucp_worker_query
#define ucg_worker_print_info       ucp_worker_print_info
#define ucg_worker_get_address      ucp_worker_get_address
#define ucg_worker_release_address  ucp_worker_release_address

/**
 * @ingroup UCG_GROUP
 * @brief UCG group collective operation description.
 *
 * The enumeration allows specifying modifiers to describe the requested
 * collective operation, as part of @ref ucg_collective_params_t
 * passed to @ref ucg_collective_start .
 */
enum ucg_collective_modifiers {
    /* Network Pattern Considerations */
    UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE      = UCS_BIT(0), /* otherwise from all */
    UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION = UCS_BIT(1), /* otherwise to all */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE          = UCS_BIT(2), /* otherwise gather */
    UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST          = UCS_BIT(3), /* otherwise scatter */
    UCG_GROUP_COLLECTIVE_MODIFIER_VARIABLE_LENGTH    = UCS_BIT(4), /* otherwise fixed length */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_PARTIAL  = UCS_BIT(5), /* MPI_Scan */
    UCG_GROUP_COLLECTIVE_MODIFIER_NEIGHBOR           = UCS_BIT(6), /* Neighbor collectives */

    /* Buffer/Data Management Considerations */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_STABLE   = UCS_BIT(7), /* stable reduction */
    UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_EXCLUDE  = UCS_BIT(8), /* MPI_Exscan */
    UCG_GROUP_COLLECTIVE_MODIFIER_IN_PLACE           = UCS_BIT(9), /* otherwise two buffers */
    UCG_GROUP_COLLECTIVE_MODIFIER_VARIABLE_DATATYPE  = UCS_BIT(10), /* otherwise fixed data-type */
    UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT         = UCS_BIT(11), /* otherwise destroy coll_h */
    UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER            = UCS_BIT(12), /* preven others from starting */

    UCG_GROUP_COLLECTIVE_MODIFIER_ALLTOALL           = UCS_BIT(13), /* MPI_ALLTOALL */
    UCG_GROUP_COLLECTIVE_MODIFIER_ALLGATHER          = UCS_BIT(14), /* MPI_ALLGATHER */

    UCG_GROUP_COLLECTIVE_MODIFIER_MASK               = UCS_MASK(16)
};

typedef struct ucg_collective_type {
    enum ucg_collective_modifiers modifiers :16;
    ucg_group_member_index_t      root :48;
} ucg_collective_type_t;

enum UCS_S_PACKED ucg_group_member_distance {
    UCG_GROUP_MEMBER_DISTANCE_SELF = 0,
    UCG_GROUP_MEMBER_DISTANCE_L3CACHE,
    UCG_GROUP_MEMBER_DISTANCE_SOCKET,
    UCG_GROUP_MEMBER_DISTANCE_HOST,
    UCG_GROUP_MEMBER_DISTANCE_NET,
    UCG_GROUP_MEMBER_DISTANCE_LAST
};

enum UCS_S_PACKED ucg_group_hierarchy_level {
    UCG_GROUP_HIERARCHY_LEVEL_NODE = 0,
    UCG_GROUP_HIERARCHY_LEVEL_SOCKET,
    UCG_GROUP_HIERARCHY_LEVEL_L3CACHE
};

typedef int (*dt_convert_f)(void *dt_ext, ucp_datatype_t *ucp_datatype);
typedef ptrdiff_t (*dt_span_f)(void *dt_ext, int count, ptrdiff_t *gap);

typedef struct ucg_group_params {
    ucg_group_member_index_t member_count; /* number of group members */
    uint32_t cid;                          /* Assign value to group_id */

    char **topo_map; /* Global topology map, topo_map[i][j] means Distance between rank i and rank j. */

    /*
     * This array contains information about the process placement of different
     * group members, which is used to select the best topology for collectives.
     *
     *
     * For example, for 2 nodes, 3 sockets each, 4 cores per socket, each member
     * should be passed the distance array contents as follows:
     *   1st group member distance array:  0111222222223333333333333333
     *   2nd group member distance array:  1011222222223333333333333333
     *   3rd group member distance array:  1101222222223333333333333333
     *   4th group member distance array:  1110222222223333333333333333
     *   5th group member distance array:  2222011122223333333333333333
     *   6th group member distance array:  2222101122223333333333333333
     *   7th group member distance array:  2222110122223333333333333333
     *   8th group member distance array:  2222111022223333333333333333
     *    ...
     *   12th group member distance array: 3333333333333333011122222222
     *   13th group member distance array: 3333333333333333101122222222
     *    ...
     */
    enum ucg_group_member_distance *distance;

    /* node index */
    uint16_t *node_index;

    /* bind-to none flag */
    unsigned is_bind_to_none;

    /* MPI passes its own reduction function, used for complex data-types */
    void (*mpi_reduce_f)(void *mpi_op, char *src, char *dst, unsigned count, void *mpi_dtype);

    /* Callback function for connection establishment */
    ucs_status_t (*resolve_address_f)(void *cb_group_obj, ucg_group_member_index_t index,
                                      ucg_address_t **addr, size_t *addr_len);
    void         (*release_address_f)(ucg_address_t *addr);

    void *cb_group_obj;  /* external group object for call-backs (MPI_Comm) */

    /* Callback function for MPI_OP */
    int (*op_is_commute_f)(void *mpi_op);

    dt_convert_f mpi_dt_convert;

    /* Callback function for get rank in MPI_COMM_WORLD */
    ucg_group_member_index_t (*mpi_global_idx_f) (void *cb_group_obj, ucg_group_member_index_t index);

    dt_span_f mpi_datatype_span;
    char is_socket_balance;
} ucg_group_params_t;

typedef struct ucg_collective {
    ucg_collective_type_t     type;    /* the type (and root) of the collective */
    ucg_hash_index_t          plan_cache_index; /* the index of collective type in plan cache. */

    struct {
        void                 *buf;     /* buffer location to use */
        union {
            int               count;   /* item count */
            int              *counts;  /* item count array */
        };
        union {
            size_t            dt_len;  /* external datatype length */
            size_t           *dts_len; /* external datatype length array */
        };
        union {
            void             *dt_ext;  /* external datatype context */
            void             *dts_ext; /* external datatype context array */
        };
        union {
            int              *displs;  /* item displacement array */
            void             *op_ext;  /* external reduce operation handle */
        };
    } send, recv;

    ucg_collective_callback_t comp_cb; /* completion callback */

} ucg_collective_params_t;


/**
 * @ingroup UCG_GROUP
 * @brief Create a group object.
 *
 * This routine allocates and initializes a @ref ucg_group_h "group" object.
 * This routine is a "collective operation", meaning it has to be called for
 * each worker participating in the group - before the first call on the group
 * is invoked on any of those workers. The call does not contain a barrier,
 * meaning a call on one worker can complete regardless of call on others.
 *
 * @note The group object is allocated within context of the calling thread
 *
 * @param [in] worker      Worker to create a group on top of.
 * @param [in] params      User defined @ref ucg_group_params_t configurations for the
 *                         @ref ucg_group_h "UCG group".
 * @param [out] group_p    A pointer to the group object allocated by the
 *                         UCG library
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_group_create(ucg_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p);


/**
 * @ingroup UCG_GROUP
 * @brief Destroy a group object.
 *
 * This routine releases the resources associated with a @ref ucg_group_h
 * "UCG group". This routine is also a "collective operation", similarly to
 * @ref ucg_group_create, meaning it must be called on each worker participating
 * in the group.
 *
 * @warning Once the UCG group handle is destroyed, it cannot be used with any
 * UCG routine.
 *
 * The destroy process releases and shuts down all resources associated with
 * the @ref ucg_group_h "group".
 *
 * @param [in]  group       Group object to destroy.
 */
void ucg_group_destroy(ucg_group_h group);


/**
 * @ingroup UCG_GROUP
 * @brief Progresses a Group object.
 *
 * @param [in]  group       Group object to progress.
 */
unsigned ucg_group_progress(ucg_group_h group);


/**
 * @ingroup UCG_GROUP
 * @brief Progresses a Worker object with the groups (UCG) extension.
 *
 * @param [in]  group       Group object to progress.
 */
unsigned ucg_worker_progress(ucg_worker_h worker);


/**
 * @ingroup UCG_GROUP
 * @brief Exposes the parameters used to create the Group object.
 *
 * @param [in]  group       Group object to query.
 */
const ucg_group_params_t* ucg_group_get_params(ucg_group_h group);


/**
 * @ingroup UCG_GROUP
 * @brief Creates a collective operation on a group object.
 * The parameters are intentionally non-constant, to allow UCG to write-back some
 * information and avoid redundant actions on the next call. For example, memory
 * registration handles are written back to the parameters pointer passed to the
 * function, and are re-used in subsequent calls.
 *
 * @param [in]  group       Group object to use.
 * @param [in]  params      Collective operation parameters.
 * @param [out] coll        Collective operation handle.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_collective_create(ucg_group_h group,
                                   ucg_collective_params_t *params,
                                   ucg_coll_h *coll);


/**
 * @ingroup UCG_GROUP
 * @brief Starts a collective operation.
 *
 * @param [in]  coll        Collective operation handle.
 *
 * @return UCS_OK           - The collective operation was completed immediately.
 * @return UCS_PTR_IS_ERR(_ptr) - The collective operation failed.
 * @return otherwise        - Operation was scheduled for send and can be
 *                          completed in any point in time. The request handle
 *                          is returned to the application in order to track
 *                          progress of the message.
 */
ucs_status_ptr_t ucg_collective_start_nb(ucg_coll_h coll);


/**
 * @ingroup UCG_GROUP
 * @brief Starts a collective operation.
 *
 * @param [in]  coll        Collective operation handle.
 * @param [in]  req         Request handle allocated by the user. There should
 *                          be at least UCG request size bytes of available
 *                          space before the @a req. The size of UCG request
 *                          can be obtained by @ref ucg_context_query function.
 *
 * @return UCS_OK           - The collective operation was completed immediately.
 * @return UCS_INPROGRESS   - The collective was not completed and is in progress.
 *                            @ref ucg_request_check_status() should be used to
 *                            monitor @a req status.
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_collective_start_nbr(ucg_coll_h coll, void *req);


/**
 * @ingroup UCG_GROUP
 * @brief Destroys a collective operation handle.
 *
 * This is only required for persistent collectives, where the flag
 * UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT is passed when calling
 * @ref ucg_collective_create. Otherwise, the handle is
 * destroyed when the collective operation is completed.
 *
 * @param [in]  coll         Collective operation handle.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
void ucg_collective_destroy(ucg_coll_h coll);


/**
 * @ingroup UCG_GROUP
 * @brief Check the status of non-blocking request.
 *
 * This routine checks the state of the request and returns its current status.
 * Any value different from UCS_INPROGRESS means that request is in a completed
 * state.
 *
 * @param [in]  request     Non-blocking request to check.
 *
 * @return Error code as defined by @ref ucs_status_t
 */
ucs_status_t ucg_request_check_status(void *request);

void ucg_request_cancel(ucg_worker_h worker, void *request);

void ucg_request_free(void *request);


ucs_status_t ucg_init_version(unsigned api_major_version,
                              unsigned api_minor_version,
                              const ucp_params_t *params,
                              const ucp_config_t *config,
                              ucp_context_h *context_p);

ucs_status_t ucg_init(const ucp_params_t *params,
                      const ucp_config_t *config,
                      ucp_context_h *context_p);

ucs_status_t ucg_worker_create(ucp_context_h context,
                               const ucp_worker_params_t *params,
                               ucp_worker_h *worker_p);

END_C_DECLS

#endif
