#pragma once

#include <atomic>
#include <chrono>
#include <stop_token>
#include <string>

namespace NTPCC {

struct TImportConfig {
    std::string ConnectionString;
    std::string Path;
    size_t WarehouseCount = 1;
    size_t LoadThreadCount = 0;
    bool UseTui = true;
};

struct TImportState {
    explicit TImportState(std::stop_token stopToken)
        : StopToken(stopToken)
    {}

    TImportState(const TImportState& other)
        : StopToken()
        , DataSizeLoaded(other.DataSizeLoaded.load(std::memory_order_relaxed))
        , WarehousesLoaded(other.WarehousesLoaded.load(std::memory_order_relaxed))
        , ApproximateDataSize(other.ApproximateDataSize)
    {}

    TImportState& operator=(const TImportState& other) {
        if (this != &other) {
            DataSizeLoaded.store(other.DataSizeLoaded.load(std::memory_order_relaxed), std::memory_order_relaxed);
            WarehousesLoaded.store(other.WarehousesLoaded.load(std::memory_order_relaxed), std::memory_order_relaxed);
            ApproximateDataSize = other.ApproximateDataSize;
        }
        return *this;
    }

    std::stop_token StopToken;
    std::atomic<size_t> DataSizeLoaded{0};
    std::atomic<size_t> WarehousesLoaded{0};
    size_t ApproximateDataSize = 0;
};

void ImportSync(const TImportConfig& config);

} // namespace NTPCC
