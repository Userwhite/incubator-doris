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

#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/segment/segment_writer.h"

namespace doris {

void GroupRowsetWriter::set_data_writer(const RowsetWriterSharedPtr& txn_rowset_writer) {
    _txn_rowset_writer = txn_rowset_writer;
}

void GroupRowsetWriter::set_row_binlog_writer(const RowsetWriterSharedPtr& row_binlog_rowset_writer) {
    _row_binlog_rowset_writer = row_binlog_rowset_writer;
}

Status GroupRowsetWriter::flush_rowsets() {
    RETURN_IF_ERROR(_txn_rowset_writer->flush());
    if (_row_binlog_rowset_writer) {
        RETURN_IF_ERROR(_row_binlog_rowset_writer->flush());
    }
    return Status::OK();
}

Status GroupRowsetWriter::build_rowsets(std::vector<RowsetSharedPtr>& rowsets) {
    RETURN_IF_ERROR(_txn_rowset_writer->build(rowsets.at(0)));
    if (_row_binlog_rowset_writer) {
        RETURN_IF_ERROR(_row_binlog_rowset_writer->build(rowsets.at(1)));
    }
    return Status::OK();
}

Status GroupRowsetWriter::flush_memtable(Block* block, int32_t segment_id, int64_t* flush_size) {
    if (!_row_binlog_rowset_writer) {
        return _txn_rowset_writer->flush_memtable(block, segment_id, flush_size);
    }

    RETURN_IF_ERROR(_txn_rowset_writer->flush_memtable(block, segment_id, flush_size));
    // Keep legacy behavior: the last flush overwrites `flush_size`.
    return _row_binlog_rowset_writer->flush_memtable(block, segment_id, flush_size);
}

Status GroupRowsetWriter::flush_single_block(const Block* block) {
    RETURN_IF_ERROR(_txn_rowset_writer->flush_single_block(block));
    if (_row_binlog_rowset_writer) {
        RETURN_IF_ERROR(_row_binlog_rowset_writer->flush_single_block(block));
    }
    return Status::OK();
}

} // namespace doris
