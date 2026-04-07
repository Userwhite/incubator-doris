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

#include "load/memtable/memtable_flush_executor.h"

#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "common/metrics/system_metrics.h"
#include "common/signal_handler.h"
#include "load/memtable/memtable.h"
#include "runtime/thread_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/storage_engine.h"
#include "util/debug_points.h"
#include "util/pretty_printer.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

struct GroupFlushContext {
    std::shared_ptr<MemTable> memtable;
    int32_t segment_id = 0;

    std::once_flag block_once;
    Status block_status;
    std::shared_ptr<Block> block;

    std::mutex mu;
    Status data_st;
    Status binlog_st;
    int64_t data_flush_size = 0;
    int64_t binlog_flush_size = 0;
    uint64_t max_wait_time_ns = 0;
    int64_t max_flush_time_ns = 0;
    size_t memtable_memory_usage = 0;

    std::atomic<int> finished_task_count {0};
};

bvar::Adder<int64_t> g_flush_task_num("memtable_flush_task_num");

class MemtableFlushTask final : public Runnable {
    ENABLE_FACTORY_CREATOR(MemtableFlushTask);

public:
    MemtableFlushTask(std::shared_ptr<FlushToken> flush_token, std::shared_ptr<MemTable> memtable,
                      int32_t segment_id, int64_t submit_task_time)
            : _flush_token(flush_token),
              _memtable(memtable),
              _segment_id(segment_id),
              _submit_task_time(submit_task_time) {
        g_flush_task_num << 1;
    }

    MemtableFlushTask(std::shared_ptr<FlushToken> flush_token, std::shared_ptr<GroupFlushContext> ctx,
                      RowsetWriterSharedPtr flush_writer, bool is_data_task,
                      int64_t submit_task_time)
            : _flush_token(flush_token),
              _group_ctx(std::move(ctx)),
              _flush_writer(std::move(flush_writer)),
              _is_data_task(is_data_task),
              _submit_task_time(submit_task_time) {
        g_flush_task_num << 1;
    }

    ~MemtableFlushTask() override { g_flush_task_num << -1; }

    void run() override {
        auto token = _flush_token.lock();
        if (token) {
            if (_group_ctx != nullptr) {
                token->_flush_memtable(_group_ctx, _flush_writer, _is_data_task, _submit_task_time);
            } else {
                token->_flush_memtable(_memtable, _segment_id, _submit_task_time);
            }
        } else {
            LOG(WARNING) << "flush token is deconstructed, ignore the flush task";
        }
    }

private:
    std::weak_ptr<FlushToken> _flush_token;
    std::shared_ptr<MemTable> _memtable;
    int32_t _segment_id;

    // Group flush context (optional)
    std::shared_ptr<GroupFlushContext> _group_ctx;
    RowsetWriterSharedPtr _flush_writer;
    bool _is_data_task = true;
    int64_t _submit_task_time;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / NANOS_PER_MILLIS
       << ", flush wait time(ms)=" << stat.flush_wait_time_ns / NANOS_PER_MILLIS
       << ", flush submit count=" << stat.flush_submit_count
       << ", running flush count=" << stat.flush_running_count
       << ", finish flush count=" << stat.flush_finish_count
       << ", flush bytes: " << stat.flush_size_bytes
       << ", flush disk bytes: " << stat.flush_disk_size_bytes << ")";
    return os;
}

Status FlushToken::submit(std::shared_ptr<MemTable> mem_table,
                          std::shared_ptr<std::vector<int128_t>> lsn_ids) {
    {
        std::shared_lock rdlk(_flush_status_lock);
        DBUG_EXECUTE_IF("FlushToken.submit_flush_error", {
            _flush_status = Status::IOError<false>("dbug_be_memtable_submit_flush_error");
        });
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }

    if (mem_table == nullptr || mem_table->empty()) {
        return Status::OK();
    }
    int64_t submit_task_time = MonotonicNanos();
    int32_t allocate_segment_id;

    std::shared_ptr<GroupFlushContext> group_ctx;
    RowsetWriterSharedPtr data_writer;
    RowsetWriterSharedPtr binlog_writer;
    bool is_group_writer = typeid_cast<GroupRowsetWriter*>(_rowset_writer.get()) != nullptr;
    if (is_group_writer) {
        auto group_rowset_writer = std::static_pointer_cast<GroupRowsetWriter>(_rowset_writer);
        DCHECK(group_rowset_writer != nullptr);
        DCHECK(lsn_ids != nullptr && !lsn_ids->empty());

        data_writer = group_rowset_writer->data_writer();
        binlog_writer = group_rowset_writer->row_binlog_writer();
        DCHECK(data_writer != nullptr);
        DCHECK(binlog_writer != nullptr);

        allocate_segment_id = data_writer->allocate_segment_id();
        const_cast<RowsetWriterContext&>(binlog_writer->context())
                .write_binlog_opt()
                .write_binlog_config()
                .insert_seg_lsn(allocate_segment_id, lsn_ids);

        group_ctx = std::make_shared<GroupFlushContext>();
        group_ctx->memtable = mem_table;
        group_ctx->segment_id = allocate_segment_id;
        group_ctx->memtable_memory_usage = mem_table->memory_usage();
    } else {
        allocate_segment_id = _rowset_writer->allocate_segment_id();
    }
    // NOTE: we should guarantee WorkloadGroup is not deconstructed when submit memtable flush task.
    // because currently WorkloadGroup's can only be destroyed when all queries in the group is finished,
    // but not consider whether load channel is finish.
    std::shared_ptr<WorkloadGroup> wg_sptr = _wg_wptr.lock();
    ThreadPool* wg_thread_pool = nullptr;
    if (wg_sptr) {
        wg_thread_pool = wg_sptr->get_memtable_flush_pool();
    }
    ThreadPool* pool = wg_thread_pool ? wg_thread_pool : _thread_pool;

    if (!is_group_writer) {
        auto task = MemtableFlushTask::create_shared(shared_from_this(), mem_table,
                                                     allocate_segment_id, submit_task_time);
        Status ret = pool->submit(std::move(task));
        if (ret.ok()) {
            _stats.flush_submit_count++;
        }
        return ret;
    }

    // Submit data flush and binlog flush as two independent tasks.
    auto data_task = MemtableFlushTask::create_shared(shared_from_this(), group_ctx, data_writer,
                                                      true, submit_task_time);
    auto binlog_task = MemtableFlushTask::create_shared(shared_from_this(), group_ctx,
                                                        binlog_writer, false, submit_task_time);

    Status ret1 = pool->submit(std::move(data_task));
    if (ret1.ok()) {
        _stats.flush_submit_count++;
    }
    Status ret2 = pool->submit(std::move(binlog_task));
    if (ret2.ok()) {
        _stats.flush_submit_count++;
    }
    if (!ret1.ok() || !ret2.ok()) {
        std::lock_guard wrlk(_flush_status_lock);
        _flush_status = !ret1.ok() ? ret1 : ret2;
        _shutdown_flush_token();
        return _flush_status;
    }
    return Status::OK();
}

void FlushToken::_flush_memtable(std::shared_ptr<GroupFlushContext> ctx,
                                 const RowsetWriterSharedPtr& flush_writer, bool is_data_task,
                                 int64_t submit_task_time) {
    DCHECK(ctx != nullptr);
    DCHECK(ctx->memtable != nullptr);
    DCHECK(flush_writer != nullptr);

    signal::set_signal_task_id(flush_writer->load_id());
    signal::tablet_id = ctx->memtable->tablet_id();

    _stats.flush_running_count++;
    Defer defer {[&]() {
        std::lock_guard<std::mutex> lock(_mutex);
        _stats.flush_submit_count--;
        if (_stats.flush_submit_count == 0) {
            _submit_task_finish_cond.notify_one();
        }
        _stats.flush_running_count--;
        if (_stats.flush_running_count == 0) {
            _running_task_finish_cond.notify_one();
        }
    }};

    if (_is_shutdown()) {
        return;
    }
    if (_is_shutdown()) {
        return;
    }

    uint64_t flush_wait_time_ns = MonotonicNanos() - submit_task_time;
    {
        std::lock_guard<std::mutex> l(ctx->mu);
        ctx->max_wait_time_ns = std::max<uint64_t>(ctx->max_wait_time_ns, flush_wait_time_ns);
    }

    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }

    std::call_once(ctx->block_once, [&]() {
        ctx->memtable->update_mem_type(MemType::FLUSH);
        SCOPED_ATTACH_TASK(ctx->memtable->resource_ctx());
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                ctx->memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
        SCOPED_CONSUME_MEM_TRACKER(ctx->memtable->mem_tracker());
        std::unique_ptr<Block> block;
        ctx->block_status = ctx->memtable->to_block(&block);
        if (ctx->block_status.ok()) {
            ctx->block.reset(block.release());
        }
    });

    MonotonicStopWatch timer;
    timer.start();
    int64_t flush_size = 0;
    Status s;
    if (ctx->block_status.ok()) {
        SCOPED_ATTACH_TASK(ctx->memtable->resource_ctx());
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                ctx->memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
        SCOPED_CONSUME_MEM_TRACKER(ctx->memtable->mem_tracker());
        s = flush_writer->flush_memtable(ctx->block.get(), ctx->segment_id, &flush_size);
    } else {
        s = ctx->block_status;
    }

    {
        std::lock_guard<std::mutex> l(ctx->mu);
        if (is_data_task) {
            ctx->data_st = s;
            ctx->data_flush_size = flush_size;
        } else {
            ctx->binlog_st = s;
            ctx->binlog_flush_size = flush_size;
        }
        ctx->max_flush_time_ns = std::max<int64_t>(ctx->max_flush_time_ns, timer.elapsed_time());
    }

    if (!s.ok()) {
        std::lock_guard wrlk(_flush_status_lock);
        if (_flush_status.ok()) {
            LOG(WARNING) << "Flush memtable failed with res = " << s
                         << ", load_id: " << print_id(flush_writer->load_id());
            _flush_status = s;
        }
        _shutdown_flush_token();
    }

    int finished = ctx->finished_task_count.fetch_add(1) + 1;
    if (finished != 2) {
        return;
    }

    // Finalize (only once) after both tasks finished.
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }
    {
        std::lock_guard<std::mutex> l(ctx->mu);
        if (!ctx->data_st.ok() || !ctx->binlog_st.ok()) {
            std::lock_guard wrlk(_flush_status_lock);
            if (_flush_status.ok()) {
                _flush_status = !ctx->data_st.ok() ? ctx->data_st : ctx->binlog_st;
            }
            _shutdown_flush_token();
            return;
        }
    }

    ctx->memtable->set_flush_success();
    _memtable_stat += ctx->memtable->stat();
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(ctx->max_flush_time_ns / 1000);
    _stats.flush_wait_time_ns += ctx->max_wait_time_ns;
    _stats.flush_time_ns += ctx->max_flush_time_ns;
    _stats.flush_finish_count++;
    _stats.flush_size_bytes += ctx->memtable_memory_usage;
    _stats.flush_disk_size_bytes += (ctx->data_flush_size + ctx->binlog_flush_size);

    VLOG_CRITICAL << "flush(group) memtable wait time: "
                  << PrettyPrinter::print(ctx->max_wait_time_ns, TUnit::TIME_NS)
                  << ", flush memtable cost: "
                  << PrettyPrinter::print(ctx->max_flush_time_ns, TUnit::TIME_NS)
                  << ", submit count: " << _stats.flush_submit_count
                  << ", running count: " << _stats.flush_running_count
                  << ", finish count: " << _stats.flush_finish_count
                  << ", mem size: " << PrettyPrinter::print_bytes(ctx->memtable_memory_usage)
                  << ", disk size(data/binlog): "
                  << PrettyPrinter::print_bytes(ctx->data_flush_size) << "/"
                  << PrettyPrinter::print_bytes(ctx->binlog_flush_size);
}

// NOTE: FlushToken's submit/cancel/wait run in one thread,
// so we don't need to make them mutually exclusive, std::atomic is enough.
void FlushToken::_wait_submit_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _submit_task_finish_cond.wait(lock, [&]() { return _stats.flush_submit_count.load() == 0; });
}

void FlushToken::_wait_running_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _running_task_finish_cond.wait(lock, [&]() { return _stats.flush_running_count.load() == 0; });
}

void FlushToken::cancel() {
    _shutdown_flush_token();
    _wait_running_task_finish();
}

Status FlushToken::wait() {
    _wait_submit_task_finish();
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }
    return Status::OK();
}

Status FlushToken::_try_reserve_memory(const std::shared_ptr<ResourceContext>& resource_context,
                                       int64_t size) {
    auto* thread_context = doris::thread_context();
    auto* memtable_flush_executor =
            ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    Status st;
    int32_t max_waiting_time = config::memtable_wait_for_memory_sleep_time_s;
    do {
        // only try to reserve process memory
        st = thread_context->thread_mem_tracker_mgr->try_reserve(
                size, ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
        if (st.ok()) {
            memtable_flush_executor->inc_flushing_task();
            break;
        }
        if (_is_shutdown() || resource_context->task_controller()->is_cancelled()) {
            st = Status::Cancelled("flush memtable already cancelled");
            break;
        }
        // Make sure at least one memtable is flushing even reserve memory failed.
        if (memtable_flush_executor->check_and_inc_has_any_flushing_task()) {
            // If there are already any flushing task, Wait for some time and retry.
            LOG_EVERY_T(INFO, 60) << fmt::format(
                    "Failed to reserve memory {} for flush memtable, retry after 100ms",
                    PrettyPrinter::print_bytes(size));
            std::this_thread::sleep_for(std::chrono::seconds(1));
            max_waiting_time -= 1;
        } else {
            st = Status::OK();
            break;
        }
    } while (max_waiting_time > 0);
    return st;
}

Status FlushToken::_do_flush_memtable(MemTable* memtable, int32_t segment_id, int64_t* flush_size) {
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << memtable->tablet_id()
                  << ", memsize: " << PrettyPrinter::print_bytes(memtable->memory_usage())
                  << ", rows: " << memtable->stat().raw_rows;
    memtable->update_mem_type(MemType::FLUSH);
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        SCOPED_ATTACH_TASK(memtable->resource_ctx());
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
        SCOPED_CONSUME_MEM_TRACKER(memtable->mem_tracker());

        // DEFER_RELEASE_RESERVED();

        // auto reserve_size = memtable->get_flush_reserve_memory_size();
        // if (memtable->resource_ctx()->task_controller()->is_enable_reserve_memory() &&
        //     reserve_size > 0) {
        //     RETURN_IF_ERROR(_try_reserve_memory(memtable->resource_ctx(), reserve_size));
        // }

        // Defer defer {[&]() {
        //     ExecEnv::GetInstance()->storage_engine().memtable_flush_executor()->dec_flushing_task();
        // }};
        std::unique_ptr<Block> block;
        RETURN_IF_ERROR(memtable->to_block(&block));
        RETURN_IF_ERROR(_rowset_writer->flush_memtable(block.get(), segment_id, flush_size));
        memtable->set_flush_success();
    }
    _memtable_stat += memtable->stat();
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << memtable->tablet_id()
                  << ", flushsize: " << PrettyPrinter::print_bytes(*flush_size);
    return Status::OK();
}

void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable_ptr, int32_t segment_id,
                                 int64_t submit_task_time) {
    signal::set_signal_task_id(_rowset_writer->load_id());
    signal::tablet_id = memtable_ptr->tablet_id();
    // Count the task as running before registering the deferred cleanup so
    // cancel/shutdown paths keep flush_running_count symmetric on every exit.
    _stats.flush_running_count++;
    Defer defer {[&]() {
        std::lock_guard<std::mutex> lock(_mutex);
        _stats.flush_submit_count--;
        if (_stats.flush_submit_count == 0) {
            _submit_task_finish_cond.notify_one();
        }
        _stats.flush_running_count--;
        if (_stats.flush_running_count == 0) {
            _running_task_finish_cond.notify_one();
        }
    }};
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_before_first_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    if (_is_shutdown()) {
        return;
    }
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_after_first_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    // double check if shutdown to avoid wait running task finish count not accurate
    if (_is_shutdown()) {
        return;
    }
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_after_second_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    uint64_t flush_wait_time_ns = MonotonicNanos() - submit_task_time;
    _stats.flush_wait_time_ns += flush_wait_time_ns;
    // If previous flush has failed, return directly
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }

    MonotonicStopWatch timer;
    timer.start();
    size_t memory_usage = memtable_ptr->memory_usage();

    int64_t flush_size;
    Status s = _do_flush_memtable(memtable_ptr.get(), segment_id, &flush_size);

    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }
    if (!s.ok()) {
        std::lock_guard wrlk(_flush_status_lock);
        LOG(WARNING) << "Flush memtable failed with res = " << s
                     << ", load_id: " << print_id(_rowset_writer->load_id());
        _flush_status = s;
        return;
    }

    VLOG_CRITICAL << "flush memtable wait time: "
                  << PrettyPrinter::print(flush_wait_time_ns, TUnit::TIME_NS)
                  << ", flush memtable cost: "
                  << PrettyPrinter::print(timer.elapsed_time(), TUnit::TIME_NS)
                  << ", submit count: " << _stats.flush_submit_count
                  << ", running count: " << _stats.flush_running_count
                  << ", finish count: " << _stats.flush_finish_count
                  << ", mem size: " << PrettyPrinter::print_bytes(memory_usage)
                  << ", disk size: " << PrettyPrinter::print_bytes(flush_size);
    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_finish_count++;
    _stats.flush_size_bytes += memtable_ptr->memory_usage();
    _stats.flush_disk_size_bytes += flush_size;
}

std::pair<int, int> MemTableFlushExecutor::calc_flush_thread_count(int num_cpus, int num_disk,
                                                                   int thread_num_per_store) {
    if (config::enable_adaptive_flush_threads && num_cpus > 0) {
        int min = std::max(1, (int)(num_cpus * config::min_flush_thread_num_per_cpu));
        int max = std::max(min, num_cpus * config::max_flush_thread_num_per_cpu);
        return {min, max};
    }
    int min = std::max(1, thread_num_per_store);
    int max = num_cpus == 0
                      ? num_disk * min
                      : std::min(num_disk * min, num_cpus * config::max_flush_thread_num_per_cpu);
    return {min, max};
}

void MemTableFlushExecutor::init(int num_disk) {
    _num_disk = std::max(1, num_disk);
    int num_cpus = std::thread::hardware_concurrency();

    auto [min_threads, max_threads] =
            calc_flush_thread_count(num_cpus, _num_disk, config::flush_thread_num_per_store);
    static_cast<void>(ThreadPoolBuilder("MemTableFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_flush_pool));

    auto [hi_min, hi_max] = calc_flush_thread_count(
            num_cpus, _num_disk, config::high_priority_flush_thread_num_per_store);
    static_cast<void>(ThreadPoolBuilder("MemTableHighPriorityFlushThreadPool")
                              .set_min_threads(hi_min)
                              .set_max_threads(hi_max)
                              .build(&_high_prio_flush_pool));
}

void MemTableFlushExecutor::update_memtable_flush_threads() {
    int num_cpus = std::thread::hardware_concurrency();

    auto [min_threads, max_threads] =
            calc_flush_thread_count(num_cpus, _num_disk, config::flush_thread_num_per_store);
    // Update max_threads first to avoid constraint violation when increasing min_threads
    static_cast<void>(_flush_pool->set_max_threads(max_threads));
    static_cast<void>(_flush_pool->set_min_threads(min_threads));

    auto [hi_min, hi_max] = calc_flush_thread_count(
            num_cpus, _num_disk, config::high_priority_flush_thread_num_per_store);
    // Update max_threads first to avoid constraint violation when increasing min_threads
    static_cast<void>(_high_prio_flush_pool->set_max_threads(hi_max));
    static_cast<void>(_high_prio_flush_pool->set_min_threads(hi_min));
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                                                 std::shared_ptr<RowsetWriter> rowset_writer,
                                                 bool is_high_priority,
                                                 std::shared_ptr<WorkloadGroup> wg_sptr) {
    switch (rowset_writer->type()) {
    case ALPHA_ROWSET:
        // alpha rowset do not support flush in CONCURRENT.  and not support alpha rowset now.
        return Status::InternalError<false>("not support alpha rowset load now.");
    case BETA_ROWSET: {
        // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
        ThreadPool* pool = is_high_priority ? _high_prio_flush_pool.get() : _flush_pool.get();
        flush_token = FlushToken::create_shared(pool, wg_sptr);
        flush_token->set_rowset_writer(rowset_writer);
        return Status::OK();
    }
    default:
        return Status::InternalError<false>("unknown rowset type.");
    }
}

} // namespace doris
