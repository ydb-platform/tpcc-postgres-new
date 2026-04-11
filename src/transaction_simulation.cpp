#include "transactions.h"
#include "coro_traits.h"

#include "constants.h"
#include "log.h"
#include "util.h"

namespace NTPCC {

TFuture<bool> GetSimulationTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session)
{
    auto startTs = std::chrono::steady_clock::now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    LOG_T("Terminal {} started simulated transaction", context.TerminalID);

    for (size_t i = 0; i < 10; ++i) {
        RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    }

    if (context.SimulateTransactionMs != 0) {
        std::chrono::milliseconds delay(context.SimulateTransactionMs);
        co_await TSuspend(context.TaskQueue, context.TerminalID, delay);
        auto endTs = std::chrono::steady_clock::now();
        latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);
        co_return true;
    }

    for (int i = 0; i < context.SimulateTransactionSelect1; ++i) {
        auto result = co_await TSuspendWithFuture(
            session.ExecuteQuery("SELECT $1::int", pqxx::params{1}),
            context.TaskQueue, context.TerminalID);
        LOG_T("Terminal {} select1 iteration {}", context.TerminalID, i);
    }

    co_await TSuspendWithFuture(session.Commit(), context.TaskQueue, context.TerminalID);

    auto endTs = std::chrono::steady_clock::now();
    latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);

    co_return true;
}

} // namespace NTPCC
