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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "storage/segment/row_binlog_segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);
    EXPECT_EQ(system("rm -rf ./data_test"), 0);
    static_cast<void>(io::global_local_filesystem()->delete_directory(
            std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
}

static void create_tablet_request(int64_t tablet_id, int32_t schema_hash, TCreateTabletReq* request,
                                  bool with_row_binlog) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->partition_id = 10001;
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 1;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->__set_storage_format(TStorageFormat::V2);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k1);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::INT;
    v1.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v1);

    if (!with_row_binlog) {
        return;
    }

    TBinlogConfig binlog_config;
    binlog_config.__set_enable(true);
    binlog_config.__set_binlog_format(TBinlogFormat::ROW);
    request->__set_binlog_config(binlog_config);

    TTabletSchema row_binlog_schema = request->tablet_schema;
    row_binlog_schema.schema_hash = schema_hash + 1;
    row_binlog_schema.keys_type = TKeysType::DUP_KEYS;

    for (auto& col : row_binlog_schema.columns) {
        if (!col.is_key) {
            col.__set_aggregation_type(TAggregationType::NONE);
        }
    }

    TColumn lsn_col;
    lsn_col.column_name = BINLOG_LSN_COL;
    lsn_col.__set_is_key(false);
    lsn_col.column_type.type = TPrimitiveType::LARGEINT;
    lsn_col.__set_aggregation_type(TAggregationType::NONE);
    row_binlog_schema.columns.push_back(lsn_col);

    TColumn op_col;
    op_col.column_name = "__DORIS_BINLOG_OP__";
    op_col.__set_is_key(false);
    op_col.column_type.type = TPrimitiveType::BIGINT;
    op_col.__set_aggregation_type(TAggregationType::NONE);
    row_binlog_schema.columns.push_back(op_col);

    TColumn ts_col;
    ts_col.column_name = "__DORIS_BINLOG_TIMESTAMP__";
    ts_col.__set_is_key(false);
    ts_col.column_type.type = TPrimitiveType::BIGINT;
    ts_col.__set_aggregation_type(TAggregationType::NONE);
    row_binlog_schema.columns.push_back(ts_col);

    request->__set_row_binlog_schema(row_binlog_schema);
}

class RowBinlogSegmentWriterTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { set_up(); }
    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(RowBinlogSegmentWriterTest, initAndAppendDirect) {
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("RowBinlogSegmentWriterTest");
    TCreateTabletReq request;
    create_tablet_request(10020, 270068500, &request, true);
    Status res = engine_ref->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok()) << res;

    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(request.tablet_id);
    ASSERT_TRUE(tablet != nullptr);

    // The actual file path is irrelevant to the writer logic, as long as it is writable.
    std::string ut_dir = config::storage_root_path + "/row_binlog_writer_ut";
    auto st = io::global_local_filesystem()->create_directory(ut_dir);
    ASSERT_TRUE(st.ok()) << st;

    io::FileWriterPtr file_writer;
    st = io::global_local_filesystem()->create_file(ut_dir + "/seg_0.dat", &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    segment_v2::SegmentWriteBinlogOptions binlog_opts;
    binlog_opts.source.tablet_schema = tablet->tablet_schema();
    binlog_opts.source.source_write_type = DataWriteType::TYPE_DIRECT;
    auto lsn_ids = std::make_shared<std::vector<int128_t>>();
    lsn_ids->push_back(1);
    lsn_ids->push_back(2);
    binlog_opts.insert_seg_lsn(0, lsn_ids);

    segment_v2::SegmentWriterOptions opts;
    opts.write_type = DataWriteType::TYPE_DIRECT;

    segment_v2::RowBinlogSegmentWriter writer(file_writer.get(), 0, tablet->row_binlog_tablet_schema(),
                                              tablet, tablet->data_dir(), opts, binlog_opts);
    ASSERT_TRUE(writer.init().ok());

    Block block = tablet->tablet_schema()->create_block();
    MutableColumns cols = block.mutate_columns();
    ASSERT_EQ(cols.size(), 2);
    cols[0]->insert(Field::create_field<TYPE_INT>(1));
    cols[1]->insert(Field::create_field<TYPE_INT>(10));
    cols[0]->insert(Field::create_field<TYPE_INT>(2));
    cols[1]->insert(Field::create_field<TYPE_INT>(20));
    block.set_columns(std::move(cols));

    ASSERT_TRUE(writer.append_block(&block, 0, 2).ok());
    uint64_t seg_file_size = 0;
    uint64_t index_size = 0;
    ASSERT_TRUE(writer.finalize(&seg_file_size, &index_size).ok());
    ASSERT_GT(seg_file_size, 0);

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok()) << res;
}

} // namespace doris
