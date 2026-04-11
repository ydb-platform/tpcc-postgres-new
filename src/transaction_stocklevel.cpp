#include "transactions.h"
#include "coro_traits.h"

#include "constants.h"
#include "log.h"
#include "util.h"

#include <string>

namespace NTPCC {

//-----------------------------------------------------------------------------

TFuture<bool> GetStockLevelTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session)
{
    auto startTs = std::chrono::steady_clock::now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    const int threshold = RandomNumber(10, 20);

    LOG_T("Terminal {} started StockLevel: W={}, D={}", context.TerminalID, warehouseID, districtID);

    // Get next order ID from district
    auto distFuture = session.ExecuteQuery(
        "SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2",
        pqxx::params{warehouseID, districtID});
    auto distResult = co_await TSuspendWithFuture(std::move(distFuture), context.TaskQueue, context.TerminalID);

    if (!distResult.TryNextRow()) {
        LOG_E("Terminal {} district not found: W={}, D={}", context.TerminalID, warehouseID, districtID);
        RequestStopWithError();
        co_return false;
    }
    int nextOrderID = distResult.GetInt32("d_next_o_id");

    // Get stock count below threshold for recent orders
    auto stockFuture = session.ExecuteQuery(
        "SELECT COUNT(DISTINCT s.s_i_id) AS stock_count "
        "FROM order_line AS ol "
        "INNER JOIN stock AS s ON s.s_i_id = ol.ol_i_id "
        "WHERE ol.ol_w_id = $1 AND ol.ol_d_id = $2 "
        "AND ol.ol_o_id < $3 AND ol.ol_o_id >= $4 "
        "AND s.s_w_id = $5 AND s.s_quantity < $6",
        pqxx::params{warehouseID, districtID, nextOrderID, nextOrderID - 20,
                     warehouseID, threshold});
    auto stockResult = co_await TSuspendWithFuture(std::move(stockFuture), context.TaskQueue, context.TerminalID);

    LOG_T("Terminal {} committing StockLevel", context.TerminalID);

    auto commitFuture = session.Commit();
    co_await TSuspendWithFuture(std::move(commitFuture), context.TaskQueue, context.TerminalID);

    auto endTs = std::chrono::steady_clock::now();
    latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);

    co_return true;
}

} // namespace NTPCC
