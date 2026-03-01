#pragma once

#include <cstdint>

namespace onebase {

// Fundamental type aliases
using page_id_t = int32_t;
using frame_id_t = int32_t;
using slot_offset_t = uint16_t;
using txn_id_t = int64_t;
using lsn_t = int64_t;
using oid_t = uint32_t;
using column_id_t = uint32_t;
using table_oid_t = uint32_t;
using index_oid_t = uint32_t;

// Sentinel values
static constexpr page_id_t INVALID_PAGE_ID = -1;
static constexpr frame_id_t INVALID_FRAME_ID = -1;
static constexpr txn_id_t INVALID_TXN_ID = -1;
static constexpr lsn_t INVALID_LSN = -1;
static constexpr table_oid_t INVALID_TABLE_OID = 0;
static constexpr index_oid_t INVALID_INDEX_OID = 0;

}  // namespace onebase
