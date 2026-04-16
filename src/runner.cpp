#include "runner.h"

#include "constants.h"
#include "log.h"
#include "log_backend.h"
#include "pg_connection_pool.h"
#include "runner_display_data.h"
#include "task_queue.h"
#include "terminal.h"
#include "transactions.h"
#include "util.h"

#ifdef TPCC_HAS_TUI
#include "runner_tui.h"
#endif

#include <fmt/format.h>

#include <chrono>
#include <csignal>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

namespace NTPCC {

namespace {

void InterruptHandler(int) {
    GetGlobalInterruptSource().request_stop();
}

const char* TransactionTypeName(ETransactionType type) {
    switch (type) {
        case ETransactionType::NewOrder: return "NewOrder";
        case ETransactionType::Delivery: return "Delivery";
        case ETransactionType::OrderStatus: return "OrderStatus";
        case ETransactionType::Payment: return "Payment";
        case ETransactionType::StockLevel: return "StockLevel";
        default: return "Unknown";
    }
}

#ifdef TPCC_HAS_TUI
std::shared_ptr<TRunDisplayData> CollectDisplayData(
    const TRunConfig& config,
    size_t threadCount,
    size_t terminalCount,
    ITaskQueue& taskQueue,
    const std::vector<std::shared_ptr<TTerminalStats>>& perThreadStats,
    Clock::time_point startTs,
    Clock::time_point warmupEnd,
    Clock::time_point runEnd,
    bool warmupDone)
{
    auto now = Clock::now();
    auto data = std::make_shared<TRunDisplayData>(threadCount, now, config.HighResHistogram);
    data->WarehouseCount = config.WarehouseCount;

    for (size_t i = 0; i < threadCount; ++i) {
        taskQueue.CollectStats(i, *data->Statistics.StatVec[i].TaskThreadStats);
        perThreadStats[i]->Collect(*data->Statistics.StatVec[i].TerminalStats);
    }

    auto& status = data->StatusData;
    auto totalElapsed = std::chrono::duration<double>(now - startTs);
    auto remaining = std::chrono::duration<double>(runEnd - now);
    auto totalDuration = std::chrono::duration<double>(runEnd - startTs);

    int elapsedSec = static_cast<int>(totalElapsed.count());
    int remainSec = std::max(0, static_cast<int>(remaining.count()));

    status.ElapsedMinutesTotal = elapsedSec / 60;
    status.ElapsedSecondsTotal = elapsedSec % 60;
    status.RemainingMinutesTotal = remainSec / 60;
    status.RemainingSecondsTotal = remainSec % 60;

    double totalSec = totalDuration.count();
    status.ProgressPercentTotal = totalSec > 0 ? std::min(100.0, totalElapsed.count() / totalSec * 100.0) : 100.0;

    status.Phase = warmupDone ? "Measuring" : "Warmup";
    status.RunningTerminals = terminalCount;
    status.RunningTransactions = TransactionsInflight.load(std::memory_order_relaxed);

    size_t totalNewOrderOK = 0;
    for (auto& stats : perThreadStats) {
        totalNewOrderOK += stats->GetStats(ETransactionType::NewOrder).OK.load(std::memory_order_relaxed);
    }

    double measureSeconds = warmupDone ? std::chrono::duration<double>(now - warmupEnd).count() : 0.0;
    status.Tpmc = measureSeconds > 0 ? (totalNewOrderOK / measureSeconds * 60.0) : 0.0;
    status.Efficiency = config.WarehouseCount > 0
        ? (status.Tpmc / (MAX_TPMC_PER_WAREHOUSE * config.WarehouseCount) * 100.0) : 0.0;

    return data;
}
#endif

void PrintConsoleStats(
    const TRunConfig& config,
    const std::vector<std::shared_ptr<TTerminalStats>>& perThreadStats,
    Clock::time_point measureStart,
    Clock::time_point runEnd)
{
    auto now = Clock::now();
    auto elapsed = std::chrono::duration<double>(now - measureStart).count();
    auto remaining = std::chrono::duration<double>(runEnd - now).count();

    size_t totalOK = 0;
    size_t totalFailed = 0;
    size_t totalNewOrderOK = 0;
    TTerminalStats aggregated(config.HighResHistogram);

    for (auto& stats : perThreadStats) {
        stats->Collect(aggregated);
        for (size_t i = 0; i < TRANSACTION_TYPE_COUNT; ++i) {
            totalOK += stats->GetStats(static_cast<ETransactionType>(i)).OK.load(std::memory_order_relaxed);
            totalFailed += stats->GetStats(static_cast<ETransactionType>(i)).Failed.load(std::memory_order_relaxed);
        }
        totalNewOrderOK += stats->GetStats(ETransactionType::NewOrder).OK.load(std::memory_order_relaxed);
    }

    double tpmc = elapsed > 0 ? (totalNewOrderOK / elapsed * 60.0) : 0.0;
    double efficiency = config.WarehouseCount > 0
        ? (tpmc / (MAX_TPMC_PER_WAREHOUSE * config.WarehouseCount) * 100.0) : 0.0;

    std::string latencies;
    for (size_t i = 0; i < TRANSACTION_TYPE_COUNT; ++i) {
        auto type = static_cast<ETransactionType>(i);
        const auto& s = aggregated.GetStats(type);
        auto p50 = s.LatencyHistogramFullMs.GetValueAtPercentile(50);
        auto p99 = s.LatencyHistogramFullMs.GetValueAtPercentile(99);
        auto ok = s.OK.load(std::memory_order_relaxed);
        if (ok > 0) {
            latencies += fmt::format("  {}:{}(p50={} p99={})",
                TransactionTypeName(type), ok, p50, p99);
        }
    }

    if (config.NoDelays) {
        LOG_I("{:.0f}s/{:.0f}s | tpmC:{:.0f} | OK:{} Fail:{} Inflight:{} |{}",
              elapsed, elapsed + remaining, tpmc,
              totalOK, totalFailed,
              TransactionsInflight.load(std::memory_order_relaxed),
              latencies);
    } else {
        LOG_I("{:.0f}s/{:.0f}s | tpmC:{:.0f} eff:{:.1f}% | OK:{} Fail:{} Inflight:{} |{}",
              elapsed, elapsed + remaining, tpmc, efficiency,
              totalOK, totalFailed,
              TransactionsInflight.load(std::memory_order_relaxed),
              latencies);
    }
}

void PrintFinalResults(
    const TRunConfig& config,
    const std::vector<std::shared_ptr<TTerminalStats>>& perThreadStats)
{
    TTerminalStats aggregated(config.HighResHistogram);
    size_t totalFailed = 0;

    for (auto& stats : perThreadStats) {
        stats->Collect(aggregated);
        for (size_t i = 0; i < TRANSACTION_TYPE_COUNT; ++i) {
            totalFailed += stats->GetStats(static_cast<ETransactionType>(i)).Failed.load(std::memory_order_relaxed);
        }
    }

    size_t totalNewOrderOK = aggregated.GetStats(ETransactionType::NewOrder).OK.load(std::memory_order_relaxed);
    double measureDuration = config.RunDuration.count();
    double tpmc = measureDuration > 0 ? (totalNewOrderOK / measureDuration * 60.0) : 0.0;
    double efficiency = config.WarehouseCount > 0
        ? (tpmc / (MAX_TPMC_PER_WAREHOUSE * config.WarehouseCount) * 100.0) : 0.0;

    LOG_I("=== TPC-C Results ===");
    LOG_I("  New-Order Throughput: {:.2f} tpmC", tpmc);
    if (!config.NoDelays) {
        LOG_I("  Efficiency: {:.1f}%", efficiency);
    }
    LOG_I("  Total Failed: {}", totalFailed);

    for (size_t i = 0; i < TRANSACTION_TYPE_COUNT; ++i) {
        auto type = static_cast<ETransactionType>(i);
        const auto& s = aggregated.GetStats(type);
        auto ok = s.OK.load(std::memory_order_relaxed);
        auto failed = s.Failed.load(std::memory_order_relaxed);
        if (ok == 0 && failed == 0) continue;

        LOG_I("  {}: OK={} Failed={} p50={}ms p90={}ms p99={}ms",
              TransactionTypeName(type), ok, failed,
              s.LatencyHistogramFullMs.GetValueAtPercentile(50),
              s.LatencyHistogramFullMs.GetValueAtPercentile(90),
              s.LatencyHistogramFullMs.GetValueAtPercentile(99));
    }
}

} // anonymous

void RunSync(const TRunConfig& config) {
    signal(SIGINT, InterruptHandler);
    signal(SIGTERM, InterruptHandler);

    const size_t warehouseCount = config.WarehouseCount;
    const size_t terminalCount = warehouseCount * TERMINALS_PER_WAREHOUSE;

    size_t threadCount = config.ThreadCount;
    if (threadCount == 0) {
        threadCount = std::min(NumberOfMyCpus(), terminalCount);
    }
    threadCount = std::max(threadCount, size_t(1));

    size_t maxInflight = config.MaxInflight;
    if (maxInflight == 0) {
        maxInflight = terminalCount;
    }

    size_t poolSize = std::min(terminalCount, maxInflight);

    if (config.IsSimulationMode()) {
        if (config.SimulateTransactionMs > 0) {
            LOG_I("SIMULATION MODE: sleep {}ms per transaction (no DB queries)", config.SimulateTransactionMs);
        } else {
            LOG_I("SIMULATION MODE: {} SELECT 1 queries per transaction", config.SimulateTransactionSelect1);
        }
    }

    const bool needsConnections = !config.IsSimulationMode() || config.SimulateTransactionSelect1 > 0;

    LOG_I("Starting TPC-C benchmark: {} warehouses, {} terminals, {} threads, {} connections, {} max inflight",
          warehouseCount, terminalCount, threadCount,
          needsConnections ? poolSize : 0, maxInflight);

    std::unique_ptr<PgConnectionPool> connectionPool;
    if (needsConnections) {
        size_t ioThreads = std::max(config.IOThreads, poolSize);
        if (ioThreads != config.IOThreads) {
            LOG_I("Raising IO threads from {} to {} (must be >= connection pool size to avoid deadlocks)",
                  config.IOThreads, ioThreads);
        }
        connectionPool = std::make_unique<PgConnectionPool>(
            config.ConnectionString, poolSize, ioThreads, config.Path);
    }

    auto taskQueue = CreateTaskQueue(threadCount, maxInflight, terminalCount, terminalCount);

    auto stopToken = GetGlobalInterruptSource().get_token();
    std::atomic<bool> stopWarmup{false};

    std::vector<std::shared_ptr<TTerminalStats>> perThreadStats;
    perThreadStats.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        perThreadStats.push_back(std::make_shared<TTerminalStats>(config.HighResHistogram));
    }

    std::vector<std::unique_ptr<TTerminal>> terminals;
    terminals.reserve(terminalCount);

    for (size_t wh = 1; wh <= warehouseCount; ++wh) {
        for (size_t t = 0; t < TERMINALS_PER_WAREHOUSE; ++t) {
            size_t terminalID = (wh - 1) * TERMINALS_PER_WAREHOUSE + t;
            size_t threadIndex = terminalID % threadCount;

            terminals.push_back(std::make_unique<TTerminal>(
                terminalID,
                wh,
                warehouseCount,
                *taskQueue,
                connectionPool.get(),
                config.NoDelays,
                stopToken,
                stopWarmup,
                perThreadStats[threadIndex],
                config.SimulateTransactionMs,
                config.SimulateTransactionSelect1));
        }
    }

    taskQueue->Run();

    constexpr auto MinWarmupPerTerminalMs = std::chrono::milliseconds(1);
    uint32_t minWarmupSeconds =
        static_cast<uint32_t>(terminalCount * MinWarmupPerTerminalMs.count() / 1000 + 1);

    bool forcedWarmup = false;
    uint32_t warmupSeconds;
    if (config.WarmupDuration.count() == 0) {
        // adaptive warmup
        if (warehouseCount <= 10) {
            warmupSeconds = 30;
        } else if (warehouseCount <= 100) {
            warmupSeconds = 5 * 60;
        } else if (warehouseCount <= 1000) {
            warmupSeconds = 10 * 60;
        } else {
            warmupSeconds = 30 * 60;
        }
        warmupSeconds = std::max(warmupSeconds, minWarmupSeconds);
    } else {
        warmupSeconds = static_cast<uint32_t>(config.WarmupDuration.count());
        if (warmupSeconds < minWarmupSeconds) {
            forcedWarmup = true;
            warmupSeconds = minWarmupSeconds;
        }
    }

    auto startTs = Clock::now();
    auto warmupEnd = startTs + std::chrono::seconds(warmupSeconds);
    auto runEnd = warmupEnd + config.RunDuration;

#ifdef TPCC_HAS_TUI
    TLogCapture logCapture(TUI_LOG_LINES);
    std::unique_ptr<TRunnerTui> tui;
    if (config.UseTui) {
        StartLogCapture(logCapture);
        auto initData = CollectDisplayData(
            config, threadCount, terminalCount, *taskQueue,
            perThreadStats, startTs, warmupEnd, runEnd, false);
        tui = std::make_unique<TRunnerTui>(logCapture, initData);
    }
#endif

    if (forcedWarmup) {
        LOG_I("Forced minimal warmup: {}s (requested {}s, need at least {}s for {} terminals)",
              warmupSeconds, config.WarmupDuration.count(), minWarmupSeconds, terminalCount);
    }

    LOG_I("Benchmark running (warmup: {}s, measure: {}s)...",
          warmupSeconds, config.RunDuration.count());

    // Stagger terminal starts to avoid overwhelming the task queue
    size_t startedTerminalId = 0;
    for (; startedTerminalId < terminals.size() && !stopToken.stop_requested(); ++startedTerminalId) {
        terminals[startedTerminalId]->Start();
        std::this_thread::sleep_for(MinWarmupPerTerminalMs);
    }

    bool warmupDone = false;
    Clock::time_point lastDisplayUpdate = startTs;
    std::shared_ptr<TRunDisplayData> prevData;

    while (!stopToken.stop_requested()) {
        auto now = Clock::now();

        if (!warmupDone && now >= warmupEnd) {
            LOG_I("Warmup complete, starting measurement");
            stopWarmup.store(true);
            warmupDone = true;
            warmupEnd = now;
            runEnd = now + config.RunDuration;
        }

        if (warmupDone && now >= runEnd) {
            LOG_I("Benchmark duration reached, stopping...");
            break;
        }

        auto sinceLast = std::chrono::duration_cast<std::chrono::seconds>(now - lastDisplayUpdate);
        const auto updateInterval = std::chrono::seconds(
#ifdef TPCC_HAS_TUI
            tui ? 1 :
#endif
            5);
        if (sinceLast >= updateInterval) {
#ifdef TPCC_HAS_TUI
            if (tui) {
                auto displayData = CollectDisplayData(
                    config, threadCount, terminalCount, *taskQueue,
                    perThreadStats, startTs, warmupEnd, runEnd, warmupDone);
                if (prevData) {
                    displayData->Statistics.CalculateDerivativeAndTotal(prevData->Statistics);
                }
                tui->Update(displayData);
                prevData = displayData;
            } else
#endif
            {
                auto measureStart = warmupDone ? warmupEnd : startTs;
                PrintConsoleStats(config, perThreadStats, measureStart, runEnd);
            }
            lastDisplayUpdate = now;
        }

        std::this_thread::sleep_for(TRunConfig::SleepMsEveryIterationMainLoop);
    }

#ifdef TPCC_HAS_TUI
    tui.reset();
    StopLogCapture();
#endif

    LOG_I("Stopping terminals...");
    GetGlobalInterruptSource().request_stop();
    if (connectionPool) {
        connectionPool->CancelAll();
    }
    taskQueue->WakeupAndNeverSleep();
    taskQueue->Join();

    PrintFinalResults(config, perThreadStats);

    connectionPool.reset();
}

} // namespace NTPCC
