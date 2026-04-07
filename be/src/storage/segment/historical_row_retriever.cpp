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

#include "storage/segment/historical_row_retriever.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "service/point_query_executor.h"
#include "storage/binlog.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/tablet/tablet.h"
#include "storage/key_coder.h"
#include "storage/column_utils.h"
#include "storage/rowset/rowset.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/columns_number.h"
#include "core/data_type/data_type.h"
#include "core/string_ref.h"
#include "core/block/column_with_type_and_name.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "runtime/exec_env.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/segment_v2/segment.h"
#include "storage/cache/row_cache.h"

namespace doris {
namespace segment_v2 {

using namespace ErrorCode;

Status PrimaryKeyModelRowRetriever::init(const HistoricalRowRetrieverContext& context) {
    _context = context;
    _key_columns.resize(_context.tablet_schema->num_key_columns());
    auto& tablet_schema = _context.tablet_schema;
    for (size_t cid = 0; cid < tablet_schema->num_key_columns(); ++cid) {
        const auto& column = tablet_schema->column(cid);
        _key_coders.push_back(get_key_coder(column.type()));
    }
    // encode the sequence id into the primary key index
    if (tablet_schema->has_sequence_col()) {
        const auto& column = tablet_schema->column(tablet_schema->sequence_col_idx());
        _seq_coder = const_cast<KeyCoder*>(get_key_coder(column.type()));
    }
    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::retrieve_historical_row(const Int8* delete_sign_column_data,
                                                            size_t row_pos, size_t num_rows) {
    auto* tablet = static_cast<Tablet*>(_context.tablet.get());
    auto& tablet_schema = _context.tablet_schema;

    DCHECK(_context.partial_update_info);

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(_context.tablet->get_header_lock());
        specified_rowsets = _mow_context->rowset_ptrs;
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
        // After converting to olap column, [0, num_rows) in the result column is corresponding to
        // [row_pos, row_pos + num_rows) in the original block
        size_t delta_pos = block_pos - row_pos;
        std::string key = _full_encode_keys(_key_columns, delta_pos);

        _maybe_invalid_row_cache(key);
        if (_seq_column != nullptr) {
            _encode_seq_column(_seq_column, delta_pos, &key);
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign =
                (delete_sign_column_data != nullptr && delete_sign_column_data[block_pos] != 0);

        RowLocation loc;
        // save rowset shared ptr so this rowset wouldn't delete
        RowsetSharedPtr rowset;
        auto st = tablet->lookup_row_key(key, tablet->tablet_schema().get(), _seq_column != nullptr, specified_rowsets, &loc,
                                         _mow_context->max_version, segment_caches, &rowset);
        if (st.is<KEY_NOT_FOUND>()) {
            // it's an insert row
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_APPEND);
            continue;
        }
        if (!st.ok() && !st.is<KEY_ALREADY_EXISTS>()) {
            LOG(WARNING) << "failed to lookup row key, error: " << st;
            return st;
        }

        // 1. if the delete sign is marked, it means that the value columns of the row will not
        //    be read. So we don't need to read the missing values from the previous rows.
        // 2. the one exception is when there are sequence columns in the table, we need to read
        //    the sequence columns, otherwise it may cause the merge-on-read based compaction
        //    policy to produce incorrect results
        if (have_delete_sign && !tablet_schema->has_sequence_col()) {
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(ROW_BINLOG_DELETE);
        } else {    
            // partial update should not contain invisible columns
            _use_default_or_null_flag.emplace_back(false);
            _rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
            // currently we think row_pos must be zero, so we won't consider row_pos > 0
            CHECK(row_pos == 0);
            tablet->prepare_to_read(loc, delta_pos, &_rssid_to_rid);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_UPDATE);
        }
    }

    CHECK_EQ(_use_default_or_null_flag.size(), num_rows);

    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::build_after_block(Block* block,
                                                      size_t row_pos,
                                                      size_t num_rows) {
    DCHECK_EQ(_use_default_or_null_flag.size(), num_rows);
    auto mutable_full_columns = block->mutate_columns();
    RETURN_IF_ERROR(_fill_missing_columns(mutable_full_columns, _use_default_or_null_flag,
                                         _has_default_or_nullable, row_pos, block));
    block->set_columns(std::move(mutable_full_columns));
    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::build_before_block(Block* before_block,
                                                       const std::vector<uint32_t>& value_cids,
                                                       size_t /*row_pos*/, size_t num_rows) {
    if constexpr (!std::is_same_v<ExecEnv::Engine, StorageEngine>) {
        // TODO(plat1ko): cloud mode
        return Status::NotSupported("fill_before_columns");
    }

    auto tablet = static_cast<Tablet*>(_context.tablet.get());
    auto& tablet_schema = _context.tablet_schema;

    if (num_rows == 0 || value_cids.empty()) {
        return Status::OK();
    }

    // Create block to hold historical values for value columns.
    Block old_value_block = tablet_schema->create_block_by_cids(value_cids);
    CHECK_EQ(value_cids.size(), old_value_block.columns());

    bool has_row_column = tablet_schema->has_row_store_for_all_columns();
    // key: logical row index in current batch; value: index in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    size_t read_idx = 0;

    for (auto rs_it : _rssid_to_rid) {
        auto rowset = _rsid_to_rowset[rs_it.first];
        CHECK(rowset);
        for (auto seg_it : rs_it.second) {
            std::vector<uint32_t> rids;
            rids.reserve(seg_it.second.size());
            for (auto id_and_pos : seg_it.second) {
                rids.emplace_back(id_and_pos.rid);
                read_index[id_and_pos.pos] = read_idx++;
            }

            if (has_row_column) {
                auto st = tablet->fetch_value_through_row_column(rowset, *tablet_schema,
                                                                  seg_it.first, rids, value_cids,
                                                                  old_value_block);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch before values through row column";
                    return st;
                }
                continue;
            }

            auto mutable_old_columns = old_value_block.mutate_columns();
            for (size_t cid_index = 0; cid_index < value_cids.size(); ++cid_index) {
                TabletColumn tablet_column = tablet_schema->column(value_cids[cid_index]);
                auto st = tablet->fetch_value_by_rowids(rowset, seg_it.first, rids,
                                                        tablet_column,
                                                        mutable_old_columns[cid_index]);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch before values by rowids";
                    return st;
                }
            }
            old_value_block.set_columns(std::move(mutable_old_columns));
        }
    }

    auto mutable_before_columns = before_block->mutate_columns();
    // Fill each row in before_block.
    for (size_t idx = 0; idx < num_rows; ++idx) {
        auto it = read_index.find(idx);
        if (it == read_index.end()) {
            // No historical row, fill BEFORE with NULL.
            for (size_t i = 0; i < value_cids.size(); ++i) {
                auto* nullable_column = assert_cast<ColumnNullable*>(
                        mutable_before_columns[i].get());
                nullable_column->insert_null_elements(1);
            }
            continue;
        }

        uint32_t pos_in_old_block = it->second;
        for (size_t i = 0; i < value_cids.size(); ++i) {
            if(old_value_block.get_columns_with_type_and_name()[i].column->is_nullable()) {
                assert_cast<ColumnNullable*>(mutable_before_columns[i].get())->insert_from(
                        *old_value_block.get_columns_with_type_and_name()[i].column.get(),
                        pos_in_old_block);
            } else {
                assert_cast<ColumnNullable*>(mutable_before_columns[i].get())->insert_from_not_nullable(
                        *old_value_block.get_columns_with_type_and_name()[i].column.get(),
                        pos_in_old_block);
            }
        }
    }

    before_block->set_columns(std::move(mutable_before_columns));
    return Status::OK();
}

std::string PrimaryKeyModelRowRetriever::_full_encode_keys(
        const std::vector<IOlapColumnDataAccessor*>& key_columns, size_t pos,
        bool null_first) {
    return _full_encode_keys(_key_coders, key_columns, pos, null_first);
}

std::string PrimaryKeyModelRowRetriever::_full_encode_keys(
        const std::vector<const KeyCoder*>& key_coders,
        const std::vector<IOlapColumnDataAccessor*>& key_columns, size_t pos,
        bool null_first) {
    assert(key_columns.size() == key_coders.size());

    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            if (null_first) {
                encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            } else {
                encoded_keys.push_back(KEY_NORMAL_MARKER);
            }
            ++cid;
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        DCHECK(key_coders[cid] != nullptr);
        key_coders[cid]->full_encode_ascending(field, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

void PrimaryKeyModelRowRetriever::_encode_seq_column(const IOlapColumnDataAccessor* seq_column,
                                       size_t pos, std::string* encoded_keys) {
    auto field = seq_column->get_data_at(pos);
    // To facilitate the use of the primary key index, encode the seq column
    // to the minimum value of the corresponding length when the seq column
    // is null
    if (UNLIKELY(!field)) {
        auto& tablet_schema = _context.tablet_schema;
        encoded_keys->push_back(KEY_NULL_FIRST_MARKER);
        size_t seq_col_length = tablet_schema->column(tablet_schema->sequence_col_idx()).length();
        encoded_keys->append(seq_col_length, KEY_MINIMAL_MARKER);
        return;
    }
    encoded_keys->push_back(KEY_NORMAL_MARKER);
    _seq_coder->full_encode_ascending(field, encoded_keys);
}

void PrimaryKeyModelRowRetriever::_maybe_invalid_row_cache(const std::string& key) {
    // Just invalid row cache for simplicity, since the rowset is not visible at present.
    // If we update/insert cache, if load failed rowset will not be visible but cached data
    // will be visible, and lead to inconsistency.
    if (!config::disable_storage_row_cache &&
        _context.tablet_schema->has_row_store_for_all_columns() &&
        _context.write_type == DataWriteType::TYPE_DIRECT) {
        // invalidate cache
        RowCache::instance()->erase({static_cast<int64_t>(_context.tablet->tablet_id()), key});
    }
}

Status PrimaryKeyModelRowRetriever::_fill_missing_columns(MutableColumns& mutable_full_columns,
                                                         const std::vector<bool>& use_default_or_null_flag,
                                                         bool has_default_or_nullable,
                                                         const size_t& segment_start_pos,
                                                         const Block* block) {
    if constexpr (!std::is_same_v<ExecEnv::Engine, StorageEngine>) {
        // TODO(plat1ko): cloud mode
        return Status::NotSupported("fill_missing_columns");
    }
    auto tablet = static_cast<Tablet*>(_context.tablet.get());
    auto& tablet_schema = _context.tablet_schema;
    // create old value columns
    const auto& cids_missing = _context.partial_update_info->missing_cids;
    Block old_value_block = tablet_schema->create_block_by_cids(cids_missing);
    CHECK_EQ(cids_missing.size(), old_value_block.columns());
    bool has_row_column = tablet_schema->has_row_store_for_all_columns();
    // record real pos, key is input line num, value is old_block line num
    std::map<uint32_t, uint32_t> read_index;
    size_t read_idx = 0;
    for (auto rs_it : _rssid_to_rid) {
        for (auto seg_it : rs_it.second) {
            auto rowset = _rsid_to_rowset[rs_it.first];
            CHECK(rowset);
            std::vector<uint32_t> rids;
            for (auto id_and_pos : seg_it.second) {
                rids.emplace_back(id_and_pos.rid);
                read_index[id_and_pos.pos] = read_idx++;
            }
            if (has_row_column) {
                auto st = tablet->fetch_value_through_row_column(
                        rowset, *tablet_schema, seg_it.first, rids, cids_missing, old_value_block);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value through row column";
                    return st;
                }
                continue;
            }
            auto mutable_old_columns = old_value_block.mutate_columns();
            for (size_t cid = 0; cid < mutable_old_columns.size(); ++cid) {
                TabletColumn tablet_column = tablet_schema->column(cids_missing[cid]);
                auto st = tablet->fetch_value_by_rowids(rowset, seg_it.first, rids, tablet_column,
                                                        mutable_old_columns[cid]);
                // set read value to output block
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value by rowids";
                    return st;
                }
            }
            old_value_block.set_columns(std::move(mutable_old_columns));
        }
    }
    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    auto mutable_default_value_columns = default_value_block.mutate_columns();

    const Int8* delete_sign_column_data = nullptr;
    if (const ColumnWithTypeAndName* delete_sign_column =
                old_value_block.try_get_by_name(DELETE_SIGN);
        delete_sign_column != nullptr) {
        auto& delete_sign_col =
                reinterpret_cast<const ColumnInt8&>(*(delete_sign_column->column));
        delete_sign_column_data = delete_sign_col.get_data().data();
    }

    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        for (auto i = 0; i < cids_missing.size(); ++i) {
            const auto& column = tablet_schema->column(cids_missing[i]);
            if (column.has_default_value()) {
                const auto& default_value =
                        _context.partial_update_info->default_values[i];
                StringRef str(default_value.data(), default_value.size());
                RETURN_IF_ERROR(old_value_block.get_by_position(i).type->get_serde()->default_from_string(
                        str, *mutable_default_value_columns[i]));
            }
        }
    }

    // fill all missing value from mutable_old_columns, need to consider default value and null value
    for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
        // `use_default_or_null_flag[idx] == true` doesn't mean that we should read values from the old row
        // for the missing columns. For example, if a table has sequence column, the rows with DELETE_SIGN column
        // marked will not be marked in delete bitmap(see https://github.com/apache/doris/pull/24011), so it will
        // be found in Tablet::lookup_row_key() and `use_default_or_null_flag[idx]` will be false. But we should not
        // read values from old rows for missing values in this occasion. So we should read the DELETE_SIGN column
        // to check if a row REALLY exists in the table.
        if (use_default_or_null_flag[idx] ||
            (delete_sign_column_data != nullptr &&
             delete_sign_column_data[read_index[idx + segment_start_pos]] != 0)) {
            for (auto i = 0; i < cids_missing.size(); ++i) {
                // if the column has default value, fill it with default value
                // otherwise, if the column is nullable, fill it with null value
                const auto& tablet_column = tablet_schema->column(cids_missing[i]);
                if (tablet_column.has_default_value()) {
                    mutable_full_columns[cids_missing[i]]->insert_from(
                            *mutable_default_value_columns[i].get(), 0);
                } else if (tablet_column.is_nullable()) {
                    auto nullable_column = assert_cast<ColumnNullable*>(
                            mutable_full_columns[cids_missing[i]].get());
                    nullable_column->insert_null_elements(1);
                } else if (tablet_schema->auto_increment_column() == tablet_column.name()) {
                    DCHECK(_context.tablet_schema->column(tablet_column.name()).type() ==
                           FieldType::OLAP_FIELD_TYPE_BIGINT);
                    auto auto_inc_column = assert_cast<ColumnInt64*>(
                            mutable_full_columns[cids_missing[i]].get());
                    auto_inc_column->insert(
                            (assert_cast<const ColumnInt64*>(
                                     block->get_by_name("__PARTIAL_UPDATE_AUTO_INC_COLUMN__")
                                             .column.get()))
                                    ->get_element(idx));
                } else {
                    // If the control flow reaches this branch, the column neither has default value
                    // nor is nullable. It means that the row's delete sign is marked, and the value
                    // columns are useless and won't be read. So we can just put arbitary values in the cells
                    mutable_full_columns[cids_missing[i]]->insert_default();
                }
            }
            continue;
        }
        auto pos_in_old_block = read_index[idx + segment_start_pos];
        for (auto i = 0; i < cids_missing.size(); ++i) {
            mutable_full_columns[cids_missing[i]]->insert_from(
                    *old_value_block.get_columns_with_type_and_name()[i].column.get(),
                    pos_in_old_block);
        }
    }
    return Status::OK();
}
} // namespace segment_v2
} // namespace doris
