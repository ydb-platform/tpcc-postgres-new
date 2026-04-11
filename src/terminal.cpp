#include "terminal.h"
#include "coro_traits.h"
#include "log.h"
#include "transactions.h"
#include "util.h"
#include "constants.h"

#include <array>

namespace NTPCC {

namespace {

struct TTerminalTransaction {
    using TTaskFunc = TFuture<bool> (*)(TTransactionContext&, std::chrono::microseconds&, PgSession&);

    std::string Name;
    double Weight;
    TTaskFunc TaskFunc;
    std::chrono::seconds KeyingTime;
    std::chrono::seconds ThinkTime;
};

static std::array<TTerminalTransaction, TRANSACTION_TYPE_COUNT> CreateTransactions() {
    std::array<TTerminalTransaction, TRANSACTION_TYPE_COUNT> transactions{};

    transactions[static_cast<size_t>(ETransactionType::NewOrder)] =
        {"NewOrder", NEW_ORDER_WEIGHT, &GetNewOrderTask, NEW_ORDER_KEYING_TIME, NEW_ORDER_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::Delivery)] =
        {"Delivery", DELIVERY_WEIGHT, &GetDeliveryTask, DELIVERY_KEYING_TIME, DELIVERY_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::OrderStatus)] =
        {"OrderStatus", ORDER_STATUS_WEIGHT, &GetOrderStatusTask, ORDER_STATUS_KEYING_TIME, ORDER_STATUS_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::Payment)] =
        {"Payment", PAYMENT_WEIGHT, &GetPaymentTask, PAYMENT_KEYING_TIME, PAYMENT_THINK_TIME};
    transactions[static_cast<size_t>(ETransactionType::StockLevel)] =
        {"StockLevel", STOCK_LEVEL_WEIGHT, &GetStockLevelTask, STOCK_LEVEL_KEYING_TIME, STOCK_LEVEL_THINK_TIME};

    return transactions;
}

static std::array<TTerminalTransaction, TRANSACTION_TYPE_COUNT> Transactions = CreateTransactions();

static size_t ChooseRandomTransactionIndex() {
    double totalWeight = 0.0;
    for (const auto& tx : Transactions) {
        totalWeight += tx.Weight;
    }

    double randomValue = RandomNumber(0, static_cast<size_t>(totalWeight * 100)) / 100.0;
    double cumulativeWeight = 0.0;

    for (size_t i = 0; i < Transactions.size(); ++i) {
        cumulativeWeight += Transactions[i].Weight;
        if (randomValue <= cumulativeWeight) {
            return i;
        }
    }

    return Transactions.size() - 1;
}

} // anonymous

TTerminal::TTerminal(size_t terminalID,
                     size_t warehouseID,
                     size_t warehouseCount,
                     ITaskQueue& taskQueue,
                     PgConnectionPool* connectionPool,
                     bool noDelays,
                     std::stop_token stopToken,
                     std::atomic<bool>& stopWarmup,
                     std::shared_ptr<TTerminalStats>& stats,
                     int simulateTransactionMs,
                     int simulateTransactionSelect1)
    : TaskQueue(taskQueue)
    , ConnectionPool(connectionPool)
    , Context{terminalID, warehouseID, warehouseCount, taskQueue,
              simulateTransactionMs, simulateTransactionSelect1}
    , NoDelays(noDelays)
    , StopToken(stopToken)
    , StopWarmup(stopWarmup)
    , Stats(stats)
{}

void TTerminal::Start() {
    if (!Started) {
        Run();
        Started = true;
    }
}

TFuture<void> TTerminal::Run() {
    co_await TTaskReady(TaskQueue, Context.TerminalID);

    LOG_D("Terminal {} started", Context.TerminalID);

    while (!StopToken.stop_requested()) {
        if (!WarmupWasStopped && StopWarmup.load(std::memory_order::relaxed)) {
            Stats->ClearOnce();
            WarmupWasStopped = true;
        }

        const bool simulationMode =
            Context.SimulateTransactionMs > 0 || Context.SimulateTransactionSelect1 > 0;

        size_t txIndex = simulationMode ? 0 : ChooseRandomTransactionIndex();
        const char* txName = simulationMode ? "Simulation" : Transactions[txIndex].Name.c_str();

        if (!NoDelays && !simulationMode) {
            auto& transaction = Transactions[txIndex];
            LOG_T("Terminal {} keying time for {}: {}s",
                Context.TerminalID, transaction.Name, transaction.KeyingTime.count());
            co_await TSuspend(TaskQueue, Context.TerminalID, transaction.KeyingTime);
            if (StopToken.stop_requested()) break;
        }

        auto startTime = std::chrono::steady_clock::now();
        co_await TTaskHasInflight(TaskQueue, Context.TerminalID);
        if (StopToken.stop_requested()) {
            TaskQueue.DecInflight();
            break;
        }

        LOG_T("Terminal {} starting {} transaction", Context.TerminalID, txName);

        auto startTimeTransaction = std::chrono::steady_clock::now();
        std::chrono::microseconds latencyPure{0};
        bool fatal = false;

        try {
            std::optional<PgConnectionPool::SessionGuard> guard;
            if (ConnectionPool) {
                guard.emplace(ConnectionPool->AcquireGuard());
            }

            PgSession dummySession;
            PgSession& session = guard ? **guard : dummySession;

            TFuture<bool> future = simulationMode
                ? GetSimulationTask(Context, latencyPure, session)
                : Transactions[txIndex].TaskFunc(Context, latencyPure, session);
            auto result = co_await TSuspendWithFuture(std::move(future), Context.TaskQueue, Context.TerminalID);

            auto endTime = std::chrono::steady_clock::now();
            auto latencyFull = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
            auto latencyTransaction = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTimeTransaction);

            if (result) {
                Stats->AddOK(static_cast<ETransactionType>(txIndex), latencyTransaction, latencyFull, latencyPure);
                LOG_T("Terminal {} {} succeeded", Context.TerminalID, txName);
            } else {
                Stats->IncFailed(static_cast<ETransactionType>(txIndex));
                LOG_D("Terminal {} {} failed (retryable)", Context.TerminalID, txName);
            }
        } catch (const TUserAbortedException&) {
            Stats->IncUserAborted(static_cast<ETransactionType>(txIndex));
            LOG_T("Terminal {} {} user aborted", Context.TerminalID, txName);
        } catch (const pqxx::serialization_failure&) {
            Stats->IncFailed(static_cast<ETransactionType>(txIndex));
            LOG_D("Terminal {} {} serialization failure", Context.TerminalID, txName);
        } catch (const std::exception& ex) {
            LOG_E("Terminal {} exception in {}: {}", Context.TerminalID, txName, ex.what());
            fatal = true;
        }

        TaskQueue.DecInflight();

        if (fatal) {
            RequestStopWithError();
            Done.store(true, std::memory_order_relaxed);
            co_return;
        }

        if (!NoDelays && !simulationMode) {
            auto& transaction = Transactions[txIndex];
            LOG_T("Terminal {} think time: {}s", Context.TerminalID, transaction.ThinkTime.count());
            co_await TSuspend(TaskQueue, Context.TerminalID, transaction.ThinkTime);
        }
    }

    LOG_D("Terminal {} stopped", Context.TerminalID);
    Done.store(true, std::memory_order_relaxed);
    co_return;
}

} // namespace NTPCC
