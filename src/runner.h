#pragma once

#include <chrono>
#include <string>

namespace NTPCC {

constexpr int DEFAULT_WAREHOUSE_COUNT = 10;
constexpr int DEFAULT_THREAD_COUNT = 0;
constexpr int DEFAULT_MAX_INFLIGHT = 0;
constexpr int DEFAULT_IO_THREADS = 4;

struct TRunConfig {
    std::string ConnectionString;
    std::string Path;
    size_t WarehouseCount = DEFAULT_WAREHOUSE_COUNT;
    std::chrono::seconds WarmupDuration{0};
    std::chrono::seconds RunDuration{600};
    size_t ThreadCount = DEFAULT_THREAD_COUNT;
    size_t MaxInflight = DEFAULT_MAX_INFLIGHT;
    size_t IOThreads = DEFAULT_IO_THREADS;
    bool NoDelays = false;
    bool HighResHistogram = false;

    // Simulation mode: replaces real TPC-C transactions with a lightweight
    // SELECT 1 loop or pure sleep. Useful for testing the coroutine/IO stack.
    int SimulateTransactionMs = 0;
    int SimulateTransactionSelect1 = 0;
    bool UseTui = false;

    bool IsSimulationMode() const {
        return SimulateTransactionMs > 0 || SimulateTransactionSelect1 > 0;
    }

    static constexpr auto SleepMsEveryIterationMainLoop = std::chrono::milliseconds(50);
};

void RunSync(const TRunConfig& config);

} // namespace NTPCC
