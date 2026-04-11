#include "workload.h"

#include "task_queue.h"
#include "coro_traits.h"

#include <gflags/gflags.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

DEFINE_int32(from, 0, "Starting concurrency count");
DEFINE_int32(to, 0, "Ending concurrency count");
DEFINE_int32(pool_size, 1, "Number of threads in the task queue (coroutine mode only)");
DEFINE_bool(coro, false, "Use coroutines instead of threads");
DEFINE_bool(per_thread_data, false, "Use separate data/pattern buffers per worker");
DEFINE_int32(slice_us, 100, "Time slice per yield in microseconds");
DEFINE_string(format, "human", "Output format: human, csv, json, all");
DEFINE_string(json_res_file, "", "If set, write JSON results to this file (independent of --format)");

using SteadyClock = std::chrono::steady_clock;

namespace {

constexpr auto MEASUREMENT_DURATION = std::chrono::seconds(5);
constexpr auto WARMUP_DURATION = std::chrono::seconds(1);

// Encodes the measurement deadline as nanoseconds since steady_clock epoch.
// 0 means "not started yet" (warmup phase).
std::atomic<uint64_t> Deadline{0};
std::atomic<uint64_t> GlobalOps{0};
std::atomic<uint64_t> GlobalMemoryScanned{0};
std::atomic<uint64_t> GlobalSink{0};
std::atomic<uint32_t> ActiveWorkers{0};

struct alignas(64) TWorkerData {
    alignas(64) uint8_t Pattern[NCoroVsThreads::DATA_SIZE];
    alignas(64) uint64_t Data[NCoroVsThreads::DATA_SIZE];

    TWorkerData() {
        NCoroVsThreads::PreparePattern(Pattern, NCoroVsThreads::DATA_SIZE);
        NCoroVsThreads::PrepareData(Data, NCoroVsThreads::DATA_SIZE);
    }
};

std::vector<TWorkerData> MakeWorkerDataSet(int concurrency, bool perThreadData) {
    const int dataSetSize = perThreadData ? concurrency : 1;
    return std::vector<TWorkerData>(static_cast<size_t>(dataSetSize));
}

uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            SteadyClock::now().time_since_epoch())
            .count());
}

void SetDeadline(SteadyClock::time_point tp) {
    Deadline.store(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                tp.time_since_epoch())
                .count()),
        std::memory_order_release);
}

// ---- Thread mode ------------------------------------------------------------

void ThreadWorker(const TWorkerData& workerData, std::chrono::microseconds sliceDuration) {
    ActiveWorkers.fetch_add(1, std::memory_order_relaxed);

    uint64_t localOps = 0;
    uint64_t localMemoryScanned = 0;
    uint64_t sink = 0;

    while (true) {
        uint64_t deadline = Deadline.load(std::memory_order_relaxed);
        if (deadline != 0 && NowNs() >= deadline)
            break;

        bool counting = (deadline != 0);

        auto sliceStart = SteadyClock::now();
        do {
            const auto opResult = NCoroVsThreads::PredictionFriendlyOp(
                workerData.Pattern, workerData.Data, NCoroVsThreads::DATA_SIZE);
            sink ^= opResult.Acc;
            if (counting)
                ++localOps;
            if (counting)
                localMemoryScanned += opResult.MemoryScannedBytes;
        } while (SteadyClock::now() - sliceStart < sliceDuration);

        std::this_thread::yield();
    }

    GlobalOps.fetch_add(localOps, std::memory_order_relaxed);
    GlobalMemoryScanned.fetch_add(localMemoryScanned, std::memory_order_relaxed);
    GlobalSink.fetch_add(sink, std::memory_order_relaxed);
    ActiveWorkers.fetch_sub(1, std::memory_order_relaxed);
}

struct TRunStats {
    uint64_t Ops = 0;
    uint64_t MemoryScannedBytes = 0;
};

TRunStats RunThreads(int concurrency, std::chrono::microseconds sliceDuration) {
    GlobalOps.store(0);
    GlobalMemoryScanned.store(0);
    Deadline.store(0);
    GlobalSink.store(0);
    ActiveWorkers.store(0);

    const auto workerDataSet = MakeWorkerDataSet(concurrency, FLAGS_per_thread_data);
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(concurrency));
    for (int i = 0; i < concurrency; ++i)
        threads.emplace_back(
            ThreadWorker,
            std::cref(workerDataSet[FLAGS_per_thread_data ? static_cast<size_t>(i) : 0]),
            sliceDuration);

    std::this_thread::sleep_for(WARMUP_DURATION);

    auto deadlineTime = SteadyClock::now() + MEASUREMENT_DURATION;
    SetDeadline(deadlineTime);

    std::this_thread::sleep_until(deadlineTime);

    for (auto& t : threads)
        t.join();

    return {
        .Ops = GlobalOps.load(std::memory_order_relaxed),
        .MemoryScannedBytes = GlobalMemoryScanned.load(std::memory_order_relaxed),
    };
}

// ---- Coroutine mode ---------------------------------------------------------

TFuture<void> CoroWorker(
    NTPCC::ITaskQueue& taskQueue,
    const TWorkerData& workerData,
    size_t threadHint,
    std::chrono::microseconds sliceDuration) {
    co_await NTPCC::TTaskReady(taskQueue, threadHint);

    ActiveWorkers.fetch_add(1, std::memory_order_relaxed);

    uint64_t localOps = 0;
    uint64_t localMemoryScanned = 0;
    uint64_t sink = 0;

    while (true) {
        uint64_t deadline = Deadline.load(std::memory_order_relaxed);
        if (deadline != 0 && NowNs() >= deadline)
            break;

        bool counting = (deadline != 0);

        auto sliceStart = SteadyClock::now();
        do {
            const auto opResult = NCoroVsThreads::PredictionFriendlyOp(
                workerData.Pattern, workerData.Data, NCoroVsThreads::DATA_SIZE);
            sink ^= opResult.Acc;
            if (counting)
                ++localOps;
            if (counting)
                localMemoryScanned += opResult.MemoryScannedBytes;
        } while (SteadyClock::now() - sliceStart < sliceDuration);

        co_await NTPCC::TYield(taskQueue, threadHint);
    }

    GlobalOps.fetch_add(localOps, std::memory_order_relaxed);
    GlobalMemoryScanned.fetch_add(localMemoryScanned, std::memory_order_relaxed);
    GlobalSink.fetch_add(sink, std::memory_order_relaxed);
    ActiveWorkers.fetch_sub(1, std::memory_order_relaxed);
    co_return;
}

TRunStats RunCoroutines(int concurrency, int poolSize,
                        std::chrono::microseconds sliceDuration) {
    GlobalOps.store(0);
    GlobalMemoryScanned.store(0);
    Deadline.store(0);
    GlobalSink.store(0);
    ActiveWorkers.store(0);

    const auto workerDataSet = MakeWorkerDataSet(concurrency, FLAGS_per_thread_data);

    auto taskQueue = NTPCC::CreateTaskQueue(
        static_cast<size_t>(poolSize),
        0, // maxRunningInternal=0 disables inflight limiting
        static_cast<size_t>(concurrency) * 4,
        static_cast<size_t>(concurrency) * 4);

    taskQueue->Run();

    for (int i = 0; i < concurrency; ++i) {
        CoroWorker(
            *taskQueue,
            workerDataSet[FLAGS_per_thread_data ? static_cast<size_t>(i) : 0],
            static_cast<size_t>(i) % static_cast<size_t>(poolSize),
            sliceDuration);
    }

    std::this_thread::sleep_for(WARMUP_DURATION);

    auto deadlineTime = SteadyClock::now() + MEASUREMENT_DURATION;
    SetDeadline(deadlineTime);

    std::this_thread::sleep_until(deadlineTime);

    taskQueue->WakeupAndNeverSleep();

    // Wait for coroutines to notice the deadline and exit
    for (int i = 0; i < 500 && ActiveWorkers.load(std::memory_order_relaxed) != 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    taskQueue->Join();

    return {
        .Ops = GlobalOps.load(std::memory_order_relaxed),
        .MemoryScannedBytes = GlobalMemoryScanned.load(std::memory_order_relaxed),
    };
}

// ---- Output -----------------------------------------------------------------

struct TResult {
    int Inflight;
    double OpsPerSec;
    double MemoryScannedPerSec;
    uint64_t MemoryScannedBytes;
    double WallTimeS;
};

const TResult* FindMaxOps(const std::vector<TResult>& results) {
    if (results.empty())
        return nullptr;
    return &*std::max_element(
        results.begin(), results.end(),
        [](const TResult& a, const TResult& b) {
            return a.OpsPerSec < b.OpsPerSec;
        });
}

void PrintResultsHuman(const std::vector<TResult>& results, bool coro, int poolSize) {
    if (coro)
        fmt::print(stdout, "Mode: coroutines (pool_size={})\n", poolSize);
    else
        fmt::print(stdout, "Mode: threads\n");

    fmt::print(stdout, "{:>10} | {:>15} | {:>15} | {:>10}\n", "Inflight", "Ops/s", "Scanned MB/s", "Wall (s)");
    fmt::print(stdout, "{:-^10}-+-{:-^15}-+-{:-^15}-+-{:-^10}\n", "", "", "", "");
    for (const auto& r : results)
        fmt::print(stdout, "{:>10} | {:>15.0f} | {:>15.2f} | {:>10.1f}\n",
                   r.Inflight, r.OpsPerSec, r.MemoryScannedPerSec / (1024.0 * 1024.0), r.WallTimeS);

    if (const auto* maxRow = FindMaxOps(results))
        fmt::print(stdout, "\nMax ops/s: {:.0f} (inflight={}, scanned={:.2f} MB/s, wall={:.1f}s)\n",
                   maxRow->OpsPerSec,
                   maxRow->Inflight,
                   maxRow->MemoryScannedPerSec / (1024.0 * 1024.0),
                   maxRow->WallTimeS);
}

void PrintResultsCsv(const std::vector<TResult>& results) {
    fmt::print(stdout, "inflight,ops_per_sec,memory_scanned_bytes,memory_scanned_bytes_per_sec,wall_time_s\n");
    for (const auto& r : results)
        fmt::print(stdout, "{},{:.0f},{},{:.0f},{:.2f}\n",
                   r.Inflight, r.OpsPerSec, r.MemoryScannedBytes, r.MemoryScannedPerSec, r.WallTimeS);
    if (const auto* maxRow = FindMaxOps(results))
        fmt::print(stdout,
                   "max_inflight,max_ops_per_sec,max_memory_scanned_bytes,max_memory_scanned_bytes_per_sec,max_wall_time_s\n"
                   "{},{:.0f},{},{:.0f},{:.2f}\n",
                   maxRow->Inflight,
                   maxRow->OpsPerSec,
                   maxRow->MemoryScannedBytes,
                   maxRow->MemoryScannedPerSec,
                   maxRow->WallTimeS);
}

void PrintResultsJsonTo(
    FILE* out,
    const std::vector<TResult>& results,
    bool coro,
    int poolSize,
    int sliceUs,
    int measSeconds,
    int warmupSeconds) {
    fmt::print(out, "{{\n");
    fmt::print(out, "  \"mode\": \"{}\",\n", coro ? "coroutines" : "threads");
    fmt::print(out, "  \"pool_size\": {},\n", poolSize);
    fmt::print(out, "  \"slice_us\": {},\n", sliceUs);
    fmt::print(out, "  \"measurement_s\": {},\n", measSeconds);
    fmt::print(out, "  \"warmup_s\": {},\n", warmupSeconds);
    fmt::print(out, "  \"results\": [\n");
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        fmt::print(out,
                   "    {{ \"inflight\": {}, \"ops_per_sec\": {:.0f}, \"memory_scanned_bytes\": {}, \"memory_scanned_bytes_per_sec\": {:.0f}, \"wall_time_s\": {:.2f} }}",
                   r.Inflight, r.OpsPerSec, r.MemoryScannedBytes, r.MemoryScannedPerSec, r.WallTimeS);
        fmt::print(out, "{}\n", i + 1 < results.size() ? "," : "");
    }
    if (const auto* maxRow = FindMaxOps(results)) {
        fmt::print(out,
                   "  ],\n  \"max\": {{ \"inflight\": {}, \"ops_per_sec\": {:.0f}, \"memory_scanned_bytes\": {}, \"memory_scanned_bytes_per_sec\": {:.0f}, \"wall_time_s\": {:.2f} }}\n",
                   maxRow->Inflight,
                   maxRow->OpsPerSec,
                   maxRow->MemoryScannedBytes,
                   maxRow->MemoryScannedPerSec,
                   maxRow->WallTimeS);
    } else {
        fmt::print(out, "  ]\n");
    }
    fmt::print(out, "}}\n");
}

void PrintResultsJson(
    const std::vector<TResult>& results,
    bool coro,
    int poolSize,
    int sliceUs,
    int measSeconds,
    int warmupSeconds) {
    PrintResultsJsonTo(stdout, results, coro, poolSize, sliceUs, measSeconds, warmupSeconds);
}

void PrintResultsAll(
    const std::vector<TResult>& results,
    bool coro,
    int poolSize,
    int sliceUs,
    int measSeconds,
    int warmupSeconds) {
    fmt::print(stdout, "--- human ---\n");
    PrintResultsHuman(results, coro, poolSize);
    fmt::print(stdout, "\n--- csv ---\n");
    PrintResultsCsv(results);
    fmt::print(stdout, "\n--- json ---\n");
    PrintResultsJson(results, coro, poolSize, sliceUs, measSeconds, warmupSeconds);
}

void PrintResults(
    const std::string& format,
    const std::vector<TResult>& results,
    bool coro,
    int poolSize,
    int sliceUs,
    int measSeconds,
    int warmupSeconds) {
    if (format == "human")
        PrintResultsHuman(results, coro, poolSize);
    else if (format == "csv")
        PrintResultsCsv(results);
    else if (format == "json")
        PrintResultsJson(results, coro, poolSize, sliceUs, measSeconds, warmupSeconds);
    else if (format == "all")
        PrintResultsAll(results, coro, poolSize, sliceUs, measSeconds, warmupSeconds);
}

} // anonymous namespace

void PrintHelp() {
    std::cout <<
        "coroutines_vs_threads - context-switch overhead: threads vs coroutines\n"
        "\n"
        "Usage: coroutines_vs_threads --from N --to N [options]\n"
        "\n"
        "Options:\n"
        "  --from N        Starting concurrency count (required)\n"
        "  --to N          Ending concurrency count, step *2 (required)\n"
        "  --coro          Use coroutines instead of threads (default: false)\n"
        "  --per_thread_data Use per-worker data/pattern buffers (default: false)\n"
        "  --pool_size P   Task-queue thread count, coroutine mode (default: 1)\n"
        "  --slice_us US   Microseconds a worker runs before yielding (default: 1)\n"
        "  --format F      Output: human (default), csv, json, all (result -> stdout only)\n"
        "  --json_res_file PATH  Write JSON results to file (independent of --format)\n";
}

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--help" || arg == "-help" || arg == "-h") {
            PrintHelp();
            return 0;
        }
    }

    gflags::SetUsageMessage(
        "coroutines_vs_threads --from N --to N [--pool_size P] [--coro] [--slice_us US]");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_from <= 0 || FLAGS_to <= 0 || FLAGS_from > FLAGS_to) {
        fmt::print(stderr, "Error: --from and --to must be positive with from <= to\n");
        return 1;
    }
    if (FLAGS_slice_us <= 0) {
        fmt::print(stderr, "Error: --slice_us must be positive\n");
        return 1;
    }
    if (FLAGS_coro && FLAGS_pool_size <= 0) {
        fmt::print(stderr, "Error: --pool_size must be positive\n");
        return 1;
    }

    const std::string& format = FLAGS_format;
    if (format != "human" && format != "csv" && format != "json" && format != "all") {
        fmt::print(stderr, "Error: --format must be one of: human, csv, json, all\n");
        return 1;
    }

    const auto sliceDuration = std::chrono::microseconds(FLAGS_slice_us);
    const double measSeconds =
        std::chrono::duration<double>(MEASUREMENT_DURATION).count();
    const int measSecondsInt = static_cast<int>(measSeconds);
    const int warmupSecondsInt = static_cast<int>(
        std::chrono::duration_cast<std::chrono::seconds>(WARMUP_DURATION).count());

    fmt::print(stderr, "slice_us={}, measurement={}s, warmup={}s, format={}\n",
               FLAGS_slice_us,
               measSecondsInt,
               warmupSecondsInt,
               format);
    fmt::print(stderr, "per_thread_data={}\n", FLAGS_per_thread_data);

    std::vector<TResult> results;
    for (int n = FLAGS_from; n <= FLAGS_to; n *= 2) {
        fmt::print(stderr, "Running with inflight={} ...\n", n);

        auto runStart = SteadyClock::now();

        TRunStats stats;
        if (FLAGS_coro)
            stats = RunCoroutines(n, FLAGS_pool_size, sliceDuration);
        else
            stats = RunThreads(n, sliceDuration);

        double wallTimeS = std::chrono::duration<double>(SteadyClock::now() - runStart).count();

        double opsPerSec = static_cast<double>(stats.Ops) / measSeconds;
        double memoryScannedPerSec = static_cast<double>(stats.MemoryScannedBytes) / measSeconds;
        results.push_back({n, opsPerSec, memoryScannedPerSec, stats.MemoryScannedBytes, wallTimeS});
    }

    PrintResults(
        format,
        results,
        FLAGS_coro,
        FLAGS_pool_size,
        FLAGS_slice_us,
        measSecondsInt,
        warmupSecondsInt);

    if (!FLAGS_json_res_file.empty()) {
        FILE* f = fopen(FLAGS_json_res_file.c_str(), "w");
        if (!f) {
            fmt::print(stderr, "Error: cannot open '{}' for writing\n", FLAGS_json_res_file);
            return 1;
        }
        PrintResultsJsonTo(f, results, FLAGS_coro, FLAGS_pool_size,
                           FLAGS_slice_us, measSecondsInt, warmupSecondsInt);
        fclose(f);
        fmt::print(stderr, "JSON results written to {}\n", FLAGS_json_res_file);
    }

    if (GlobalSink.load() == 0xDEADBEEFULL)
        fmt::print(stderr, "sink\n");

    return 0;
}
