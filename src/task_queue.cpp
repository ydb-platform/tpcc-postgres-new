#include "task_queue.h"

#include "timer_queue.h"
#include "spsc_circular_queue.h"
#include "log.h"

#include <chrono>
#include <thread>

namespace NTPCC {

namespace {

//-----------------------------------------------------------------------------

thread_local int TaskQueueThreadId = -1;

//-----------------------------------------------------------------------------

struct THandleWithTs {
    std::coroutine_handle<> Handle;
    Clock::time_point CreatedAt = Clock::now();

    double ElapsedMs() const {
        auto now = Clock::now();
        return std::chrono::duration<double, std::milli>(now - CreatedAt).count();
    }
};

struct alignas(64) TPerThreadContext {
    TPerThreadContext() = default;

    TPerThreadContext(const TPerThreadContext&) = delete;
    TPerThreadContext& operator=(const TPerThreadContext&) = delete;
    TPerThreadContext(TPerThreadContext&&) = delete;
    TPerThreadContext& operator=(TPerThreadContext&&) = delete;

    TBinnedTimerQueue<std::coroutine_handle<>> SleepingTasks;
    TSpscCircularQueue<THandleWithTs> ReadyTasksInternal;
    TSpscCircularQueue<THandleWithTs> InflightWaitingTasksInternal;

    TSpinLock ReadyTasksLock;
    TSpscCircularQueue<THandleWithTs> ReadyTasksExternal;

    ITaskQueue::TThreadStats Stats;
};

//-----------------------------------------------------------------------------

class TTaskQueue : public ITaskQueue {
public:
    TTaskQueue(size_t threadCount,
               size_t maxRunningInternal,
               size_t maxReadyInternal,
               size_t maxReadyExternal);

    ~TTaskQueue() override {
        Join();
    }

    void Run() override;
    void Join() override;
    void WakeupAndNeverSleep() override;

    void TaskReady(std::coroutine_handle<>, size_t threadHint) override;
    void AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::microseconds delay) override;
    bool IncInflight(std::coroutine_handle<> handle, size_t threadHint) override;
    void DecInflight() override;

    void TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) override;

    bool CheckCurrentThread() const override;

    void CollectStats(size_t threadIndex, TThreadStats& dst) override;

    size_t GetRunningCount() const override {
        return RunningInternalCount.load(std::memory_order_relaxed);
    }

private:
    void RunThread(size_t threadId);
    void ProcessSleepingTasks(size_t threadId, TPerThreadContext& context, Clock::time_point now);
    void ProcessInflightQueue(size_t threadId, TPerThreadContext& context, std::optional<uint64_t>& internalInflightWaitTimeMs);

    void HandleQueueFull(const char* queueType);

private:
    size_t ThreadCount;
    size_t MaxRunningInternal;
    size_t MaxReadyInternal;
    size_t MaxReadyExternal;

    std::atomic<size_t> RunningInternalCount{0};

    std::stop_source ThreadsStopSource;
    std::atomic_flag WakeupAll;
    std::vector<std::thread> Threads;
    std::vector<std::unique_ptr<TPerThreadContext>> PerThreadContext;
};

TTaskQueue::TTaskQueue(size_t threadCount,
            size_t maxRunningInternal,
            size_t maxReadyInternal,
            size_t maxReadyExternal)
    : ThreadCount(threadCount)
    , MaxRunningInternal(maxRunningInternal)
    , MaxReadyInternal(maxReadyInternal)
    , MaxReadyExternal(maxReadyExternal)
{
    (void)MaxRunningInternal;
    if (ThreadCount == 0) {
        LOG_E("Zero TaskQueue threads");
        throw std::invalid_argument("Thread count must be greater than zero");
    }

    const size_t maxSleepingInternal = MaxReadyInternal;
    constexpr size_t timerBucketSize = 100;
    const size_t timerBucketCount = (maxSleepingInternal + timerBucketSize - 1) / timerBucketSize;

    PerThreadContext.resize(ThreadCount);
    for (auto& context : PerThreadContext) {
        context = std::make_unique<TPerThreadContext>();
        context->SleepingTasks.Resize(timerBucketCount, timerBucketSize);
        context->ReadyTasksInternal.Resize(MaxReadyInternal);
        context->InflightWaitingTasksInternal.Resize(MaxReadyInternal);
        context->ReadyTasksExternal.Resize(MaxReadyExternal);
    }
}

void TTaskQueue::Run() {
    Threads.reserve(ThreadCount);
    for (size_t i = 0; i < ThreadCount; ++i) {
        Threads.emplace_back([this, i]() {
            RunThread(i);
        });
    }
}

void TTaskQueue::Join() {
    if (ThreadsStopSource.stop_requested()) {
        return;
    }

    ThreadsStopSource.request_stop();
    for (auto& thread: Threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TTaskQueue::WakeupAndNeverSleep() {
    WakeupAll.test_and_set(std::memory_order_relaxed);
}

void TTaskQueue::HandleQueueFull(const char* queueType) {
    LOG_E("Failed to push ready {}, queue is full", queueType);
    throw std::runtime_error(std::string("Task queue is full: ") + queueType);
}

void TTaskQueue::TaskReady(std::coroutine_handle<> handle, size_t threadHint) {
    auto contextIndex = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[contextIndex];

    if (!context.ReadyTasksInternal.TryPush({std::move(handle), Clock::now()})) {
        HandleQueueFull("internal");
    }
}

void TTaskQueue::AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::microseconds delay) {
    auto contextIndex = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[contextIndex];
    context.SleepingTasks.Add(delay, std::move(handle));
}

bool TTaskQueue::IncInflight(std::coroutine_handle<> handle, size_t threadHint) {
    auto prevRunningCount = RunningInternalCount.fetch_add(1, std::memory_order_relaxed);

    if (MaxRunningInternal == 0) {
        return false;
    }

    if (prevRunningCount < MaxRunningInternal) {
        return false;
    }

    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);

    auto index = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[index];
    if (!context.InflightWaitingTasksInternal.TryPush({std::move(handle), Clock::now()})) {
        HandleQueueFull("inflight-waiting");
    }

    return true;
}

void TTaskQueue::DecInflight() {
    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);
}

void TTaskQueue::TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) {
    auto index = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[index];

    std::lock_guard guard(context.ReadyTasksLock);
    if (!context.ReadyTasksExternal.TryPush({std::move(handle), Clock::now()})) {
        HandleQueueFull("external");
    }
}

void TTaskQueue::ProcessSleepingTasks(size_t, TPerThreadContext& context, Clock::time_point now) {
    while (!context.SleepingTasks.Empty() &&
            (context.SleepingTasks.GetNextDeadline() <= now || WakeupAll.test(std::memory_order_relaxed))) {
        auto handle = context.SleepingTasks.PopFront().Value;
        if (!context.ReadyTasksInternal.TryPush({std::move(handle), Clock::now()})) {
            HandleQueueFull("internal (awakened)");
        }
    }
}

void TTaskQueue::ProcessInflightQueue(
    size_t threadId,
    TPerThreadContext& context,
    std::optional<uint64_t>& internalInflightWaitTimeMs)
{
    if (MaxRunningInternal == 0) {
        return;
    }

    while (!context.InflightWaitingTasksInternal.Empty()
            && RunningInternalCount.load(std::memory_order_relaxed) < MaxRunningInternal) {
        auto runningCount = RunningInternalCount.fetch_add(1, std::memory_order_relaxed);
        if (runningCount >= MaxRunningInternal) {
            RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);
            break;
        }

        THandleWithTs internalTask;
        if (context.InflightWaitingTasksInternal.TryPop(internalTask)) {
            if (internalTask.Handle && !internalTask.Handle.done()) {
                LOG_D("Thread {} marked ready task waited for inflight (internal)", threadId);
                internalInflightWaitTimeMs = static_cast<uint64_t>(internalTask.ElapsedMs());
                context.ReadyTasksInternal.TryPush(std::move(internalTask));
            }
        }
    }
}

void TTaskQueue::RunThread(size_t threadId) {
    TaskQueueThreadId = static_cast<int>(threadId);

    auto& context = *PerThreadContext[threadId];
    auto& stats = context.Stats;

    auto totalStart = Clock::now();
    while (!ThreadsStopSource.stop_requested()) {
        double executingTime = 0;

        std::optional<uint64_t> internalInflightWaitTimeMs;
        std::optional<uint64_t> internalQueueTimeMs;

        std::vector<uint64_t> externalQueueTimeLatencies;

        auto now = Clock::now();

        std::vector<THandleWithTs> externalTasks;
        {
            std::lock_guard guard(context.ReadyTasksLock);
            THandleWithTs task;
            while (context.ReadyTasksExternal.TryPop(task)) {
                externalTasks.emplace_back(std::move(task));
            }
        }
        stats.ExternalTasksReady.store(externalTasks.size(), std::memory_order_relaxed);

        for (auto& handleWithTs: externalTasks) {
            if (ThreadsStopSource.stop_requested()) {
                break;
            }
            if (handleWithTs.Handle && !handleWithTs.Handle.done()) {
                LOG_T("Thread {} resumed task (external)", threadId);
                stats.ExternalTasksResumed.fetch_add(1, std::memory_order_relaxed);
                externalQueueTimeLatencies.emplace_back(static_cast<uint64_t>(handleWithTs.ElapsedMs()));

                auto execStart = Clock::now();
                handleWithTs.Handle.resume();
                auto execEnd = Clock::now();
                executingTime += std::chrono::duration<double>(execEnd - execStart).count();
            }
        }

        ProcessInflightQueue(threadId, context, internalInflightWaitTimeMs);
        ProcessSleepingTasks(threadId, context, now);

        THandleWithTs internalTask;
        if (context.ReadyTasksInternal.TryPop(internalTask)) {
            if (internalTask.Handle && !internalTask.Handle.done()) {
                LOG_D("Thread {} resumed task (internal)", threadId);
                stats.InternalTasksResumed.fetch_add(1, std::memory_order_relaxed);
                internalQueueTimeMs = static_cast<uint64_t>(internalTask.ElapsedMs());
                auto execStart = Clock::now();
                internalTask.Handle.resume();
                auto execEnd = Clock::now();
                executingTime += std::chrono::duration<double>(execEnd - execStart).count();
            }
        }

        stats.InternalTasksSleeping.store(context.SleepingTasks.Size(), std::memory_order_relaxed);
        stats.InternalTasksWaitingInflight.store(context.InflightWaitingTasksInternal.Size(), std::memory_order_relaxed);
        stats.InternalTasksReady.store(context.ReadyTasksInternal.Size(), std::memory_order_relaxed);
        stats.ExecutingTime.fetch_add(executingTime, std::memory_order_relaxed);
        auto totalNow = Clock::now();
        stats.TotalTime.fetch_add(std::chrono::duration<double>(totalNow - totalStart).count());
        totalStart = totalNow;
        {
            std::lock_guard guard(stats.HistLock);
            if (internalInflightWaitTimeMs) {
                stats.InternalInflightWaitTimeMs.RecordValue(*internalInflightWaitTimeMs);
            }
            if (internalQueueTimeMs) {
                stats.InternalQueueTimeMs.RecordValue(*internalQueueTimeMs);
            }

            for (auto latency: externalQueueTimeLatencies) {
                stats.ExternalQueueTimeMs.RecordValue(latency);
            }
        }
    }
}

bool TTaskQueue::CheckCurrentThread() const {
    return TaskQueueThreadId >= 0;
}

void TTaskQueue::CollectStats(size_t threadIndex, TThreadStats& dst) {
    if (threadIndex >= PerThreadContext.size()) {
        throw std::runtime_error("Invalid thread index in stats collection");
    }

    auto& context = *PerThreadContext[threadIndex];
    auto& srcStats = context.Stats;
    srcStats.Collect(dst);
}

} // anonymous

//-----------------------------------------------------------------------------

std::unique_ptr<ITaskQueue> CreateTaskQueue(
    size_t threadCount,
    size_t maxRunningInternal,
    size_t maxReadyInternal,
    size_t maxReadyExternal)
{
    return std::make_unique<TTaskQueue>(
        threadCount,
        maxRunningInternal,
        maxReadyInternal,
        maxReadyExternal);
}

} // namespace NTPCC
