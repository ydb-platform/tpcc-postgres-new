#pragma once

#include "pg_session.h"
#include "task_queue.h"
#include "future.h"

#include <atomic>
#include <chrono>
#include <memory>

namespace NTPCC {

//-----------------------------------------------------------------------------

extern std::atomic<size_t> TransactionsInflight;

struct TTransactionInflightGuard {
    TTransactionInflightGuard() {
        TransactionsInflight.fetch_add(1, std::memory_order_relaxed);
    }

    ~TTransactionInflightGuard() {
        TransactionsInflight.fetch_sub(1, std::memory_order_relaxed);
    }
};

//-----------------------------------------------------------------------------

struct TTransactionContext {
    size_t TerminalID;
    size_t WarehouseID;
    size_t WarehouseCount;
    ITaskQueue& TaskQueue;

    // Simulation mode parameters (0 = disabled)
    int SimulateTransactionMs = 0;
    int SimulateTransactionSelect1 = 0;
};

struct TUserAbortedException : public std::runtime_error {
    TUserAbortedException() : std::runtime_error("User aborted transaction (expected rollback)") {}
};

//-----------------------------------------------------------------------------

// Each transaction is a coroutine returning TFuture<bool>.
// Returns true on success, false on retryable failure.
// Throws on fatal errors.
// Latency is measured by the coroutine and stored in the latency output param.

TFuture<bool> GetNewOrderTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session);

TFuture<bool> GetDeliveryTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session);

TFuture<bool> GetOrderStatusTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session);

TFuture<bool> GetPaymentTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session);

TFuture<bool> GetStockLevelTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session);

TFuture<bool> GetSimulationTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session);

} // namespace NTPCC
