// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <fmt/format.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/sink/autoinc_buffer.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

// Row binlog op type.
// NOTE: The value is persisted into row binlog data, so keep it stable.
static constexpr int64_t ROW_BINLOG_APPEND = 0;
static constexpr int64_t ROW_BINLOG_UPDATE = 1;
static constexpr int64_t ROW_BINLOG_DELETE = 2;

constexpr std::string_view kBinlogPrefix = "binlog_";
constexpr std::string_view kBinlogMetaPrefix = "binlog_meta_";
constexpr std::string_view kBinlogDataPrefix = "binlog_data_";
constexpr std::string_view kRowBinlogPrefix = "binlog_row_";
// used in file directory
constexpr std::string_view FDRowBinlogSuffix = "_row_binlog";

inline auto make_row_binlog_meta_key_prefix(const TabletUid& tablet_uid) {
    return fmt::format("{}{}_", kRowBinlogPrefix, tablet_uid.to_string());
}

inline auto make_row_binlog_meta_key(const TabletUid& tablet_uid, int64_t version,
                                     const RowsetId& rowset_id) {
    // version is formatted to 20 bytes to avoid the problem of sorting
    return fmt::format("{}{}_{:020d}_{}", kRowBinlogPrefix, tablet_uid.to_string(), version,
                       rowset_id.to_string());
}

inline auto make_binlog_meta_key(const std::string_view tablet, int64_t version,
                                 const std::string_view rowset) {
    return fmt::format("{}meta_{}_{:020d}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_meta_key(const std::string_view tablet, const std::string_view version_str,
                                 const std::string_view rowset) {
    // TODO(Drogon): use fmt::format not convert to version_num, only string with length prefix '0'
    int64_t version = std::atoll(version_str.data());
    return make_binlog_meta_key(tablet, version, rowset);
}

inline auto make_binlog_meta_key(const TabletUid& tablet_uid, int64_t version,
                                 const RowsetId& rowset_id) {
    return make_binlog_meta_key(tablet_uid.to_string(), version, rowset_id.to_string());
}

inline auto make_binlog_meta_key_prefix(const TabletUid& tablet_uid) {
    return fmt::format("{}meta_{}_", kBinlogPrefix, tablet_uid.to_string());
}

inline auto make_binlog_meta_key_prefix(const TabletUid& tablet_uid, int64_t version) {
    return fmt::format("{}meta_{}_{:020d}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline auto make_binlog_data_key(const std::string_view tablet, int64_t version,
                                 const std::string_view rowset) {
    return fmt::format("{}data_{}_{:020d}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_data_key(const std::string_view tablet, const std::string_view version,
                                 const std::string_view rowset) {
    return fmt::format("{}data_{}_{:0>20}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_data_key(const TabletUid& tablet_uid, int64_t version,
                                 const RowsetId& rowset_id) {
    return make_binlog_data_key(tablet_uid.to_string(), version, rowset_id.to_string());
}

inline auto make_binlog_data_key(const TabletUid& tablet_uid, int64_t version,
                                 const std::string_view rowset_id) {
    return make_binlog_data_key(tablet_uid.to_string(), version, rowset_id);
}

inline auto make_binlog_data_key_prefix(const TabletUid& tablet_uid, int64_t version) {
    return fmt::format("{}data_{}_{:020d}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline auto make_binlog_filename_key(const TabletUid& tablet_uid, const std::string_view version) {
    return fmt::format("{}meta_{}_{:0>20}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline bool starts_with_binlog_meta(const std::string_view str) {
    auto prefix = kBinlogMetaPrefix;
    if (prefix.length() > str.length()) {
        return false;
    }

    return str.compare(0, prefix.length(), prefix) == 0;
}

inline std::string get_binlog_data_key_from_meta_key(const std::string_view meta_key) {
    // like "binlog_meta_6943f1585fe834b5-e542c2b83a21d0b7" => "binlog_data-6943f1585fe834b5-e542c2b83a21d0b7"
    return fmt::format("{}data_{}", kBinlogPrefix, meta_key.substr(kBinlogMetaPrefix.length()));
}

// Allocate per-row LSNs for row binlog writing.
//
// NOTE:
// - The allocator lives in FE (AutoIncrementGenerator) and is fetched via GlobalAutoIncBuffers.
// - Binlog LSN is NOT a user-visible AUTO_INCREMENT column, but it still reuses the allocator.
// - Returns `nullptr` when `num_rows == 0`.
inline Status allocate_row_binlog_lsn_ids(int64_t db_id, int64_t table_id,
                                         const TabletSchemaSPtr& row_binlog_schema,
                                         size_t num_rows,
                                         std::shared_ptr<std::vector<int128_t>>* lsn_ids) {
    if (lsn_ids == nullptr) {
        return Status::InternalError<false>("lsn_ids output is null");
    }
    if (num_rows == 0) {
        *lsn_ids = nullptr;
        return Status::OK();
    }
    if (row_binlog_schema == nullptr) {
        return Status::InternalError<false>("row binlog schema is null");
    }

    static constexpr const char* kLsnColName = "__DORIS_BINLOG_LSN__";
    int32_t lsn_cid = row_binlog_schema->field_index(kLsnColName);
    if (lsn_cid < 0) {
        return Status::InternalError<false>(
                fmt::format("row binlog schema missing {}", kLsnColName));
    }
    int64_t lsn_col_uid = row_binlog_schema->column(lsn_cid).unique_id();

    auto buffer = GlobalAutoIncBuffers::GetInstance()->get_auto_inc_buffer(db_id, table_id, lsn_col_uid);
    std::vector<std::pair<int64_t, size_t>> ranges;
    auto st = buffer->sync_request_ids(num_rows, &ranges);
    if (!st.ok()) {
        return st;
    }

    auto ids = std::make_shared<std::vector<int128_t>>();
    ids->reserve(num_rows);
    for (const auto& [start, length] : ranges) {
        for (size_t i = 0; i < length; ++i) {
            ids->push_back(static_cast<int128_t>(start + static_cast<int64_t>(i)));
        }
    }
    if (ids->size() != num_rows) {
        return Status::InternalError<false>(
                fmt::format("allocate row binlog lsn size mismatch: {} vs {}", ids->size(),
                            num_rows));
    }

    *lsn_ids = std::move(ids);
    return Status::OK();
}
} // namespace doris
