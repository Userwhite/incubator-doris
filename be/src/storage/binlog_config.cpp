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

#include "storage/binlog_config.h"

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>

#include "common/logging.h"
#include "exec/sink/autoinc_buffer.h" // AutoIncIDBuffer
#include "storage/binlog.h"           // allocate_binlog_lsn

namespace doris {

Status allocate_binlog_lsn(const std::shared_ptr<AutoIncIDBuffer>& lsn_buffer, size_t num_rows,
                           std::shared_ptr<std::vector<int128_t>>* lsn_ids) {
    if (lsn_buffer == nullptr) {
        return Status::InternalError("binlog<row> try to get lsn buffer, but null");
    }
    DCHECK(lsn_ids != nullptr);
    DCHECK(num_rows > 0);

    std::vector<std::pair<int64_t, size_t>> ranges;
    RETURN_IF_ERROR(lsn_buffer->sync_request_ids(num_rows, &ranges));

    auto ids = std::make_shared<std::vector<int128_t>>();
    ids->reserve(num_rows);
    for (const auto& [start, length] : ranges) {
        for (size_t i = 0; i < length; ++i) {
            ids->push_back(static_cast<int128_t>(start + static_cast<int64_t>(i)));
        }
    }
    DCHECK_EQ(ids->size(), num_rows);
    *lsn_ids = std::move(ids);
    return Status::OK();
}

BinlogConfig& BinlogConfig::operator=(const TBinlogConfig& config) {
    if (config.__isset.enable) {
        _enable = config.enable;
    }
    if (config.__isset.ttl_seconds) {
        _ttl_seconds = config.ttl_seconds;
    }
    if (config.__isset.max_bytes) {
        _max_bytes = config.max_bytes;
    }
    if (config.__isset.max_history_nums) {
        _max_history_nums = config.max_history_nums;
    }
    if (config.__isset.binlog_format) {
        if (config.binlog_format == TBinlogFormat::ROW) {
            _binlog_format = BinlogFormatPB::ROW;
        } else if (config.binlog_format == TBinlogFormat::STATEMENT_AND_SNAPSHOT) {
            _binlog_format = BinlogFormatPB::STATEMENT_AND_SNAPSHOT;
        } else {
            DCHECK(false) << "can not identify the binlog format " << config.binlog_format;
        }
    }
    if (config.__isset.need_historical_value) {
        _need_historical_value = config.need_historical_value;
    }
    return *this;
}

BinlogConfig& BinlogConfig::operator=(const BinlogConfigPB& config) {
    if (config.has_enable()) {
        _enable = config.enable();
    }
    if (config.has_ttl_seconds()) {
        _ttl_seconds = config.ttl_seconds();
    }
    if (config.has_max_bytes()) {
        _max_bytes = config.max_bytes();
    }
    if (config.has_max_history_nums()) {
        _max_history_nums = config.max_history_nums();
    }
    if (config.has_binlog_format()) {
        _binlog_format = config.binlog_format();
    }
    if (config.has_need_historical_value()) {
        _need_historical_value = config.need_historical_value();
    }
    return *this;
}

void BinlogConfig::to_pb(BinlogConfigPB* config_pb) const {
    config_pb->set_enable(_enable);
    config_pb->set_ttl_seconds(_ttl_seconds);
    config_pb->set_max_bytes(_max_bytes);
    config_pb->set_max_history_nums(_max_history_nums);
    config_pb->set_binlog_format(_binlog_format);
    config_pb->set_need_historical_value(_need_historical_value);
}

std::string BinlogConfig::to_string() const {
    return fmt::format(
            "BinlogConfig enable: {}, ttl_seconds: {}, max_bytes: {}, max_history_nums: {}, "
            "binlog_format: {}, need_historical_value: {}",
            _enable, _ttl_seconds, _max_bytes, _max_history_nums, _binlog_format,
            _need_historical_value);
}

} // namespace doris
