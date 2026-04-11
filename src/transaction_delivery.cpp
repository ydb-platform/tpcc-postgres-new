#include "transactions.h"
#include "coro_traits.h"

#include "constants.h"
#include "log.h"
#include "util.h"

#include <array>
#include <optional>
#include <string>
#include <vector>

namespace NTPCC {

namespace {

struct TOrderData {
    int OrderID = 0;
    int CustomerId = 0;
    double TotalAmount = 0;
    std::vector<int> OrderLineNumbers;
};

} // anonymous

//-----------------------------------------------------------------------------

TFuture<bool> GetDeliveryTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session)
{
    auto startTs = std::chrono::steady_clock::now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    const int warehouseID = context.WarehouseID;
    const int carrierID = RandomNumber(1, 10);

    LOG_T("Terminal {} started Delivery: W={}", context.TerminalID, warehouseID);

    std::array<std::optional<TOrderData>, DISTRICT_COUNT> orders;

    for (int districtID = DISTRICT_LOW_ID; districtID <= DISTRICT_HIGH_ID; ++districtID) {
        // Get oldest new order
        auto noFuture = session.ExecuteQuery(
            "SELECT no_o_id FROM new_order "
            "WHERE no_d_id = $1 AND no_w_id = $2 "
            "ORDER BY no_o_id ASC LIMIT 1",
            pqxx::params{districtID, warehouseID});
        auto noResult = co_await TSuspendWithFuture(std::move(noFuture), context.TaskQueue, context.TerminalID);

        if (!noResult.TryNextRow()) {
            LOG_T("Terminal {} no new orders for district {}", context.TerminalID, districtID);
            continue;
        }

        auto& order = orders[districtID - DISTRICT_LOW_ID].emplace();
        order.OrderID = noResult.GetInt32("no_o_id");

        // Get customer ID from order
        auto cidFuture = session.ExecuteQuery(
            "SELECT o_c_id FROM oorder WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3",
            pqxx::params{warehouseID, districtID, order.OrderID});
        auto cidResult = co_await TSuspendWithFuture(std::move(cidFuture), context.TaskQueue, context.TerminalID);

        if (!cidResult.TryNextRow()) {
            LOG_E("Terminal {} order not found: W={}, D={}, O={}", context.TerminalID, warehouseID, districtID, order.OrderID);
            RequestStopWithError();
            co_return false;
        }
        order.CustomerId = cidResult.GetInt32("o_c_id");

        // Get order lines
        auto olFuture = session.ExecuteQuery(
            "SELECT ol_number, ol_amount FROM order_line "
            "WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
            pqxx::params{warehouseID, districtID, order.OrderID});
        auto olResult = co_await TSuspendWithFuture(std::move(olFuture), context.TaskQueue, context.TerminalID);

        while (olResult.TryNextRow()) {
            order.OrderLineNumbers.push_back(olResult.GetInt32("ol_number"));
            order.TotalAmount += olResult.GetDouble("ol_amount");
        }

        if (order.OrderLineNumbers.empty()) {
            LOG_E("Terminal {} no order lines: W={}, D={}, O={}", context.TerminalID, warehouseID, districtID, order.OrderID);
            RequestStopWithError();
            co_return false;
        }
    }

    // Now perform the writes for each district
    for (int districtID = DISTRICT_LOW_ID; districtID <= DISTRICT_HIGH_ID; ++districtID) {
        if (!orders[districtID - DISTRICT_LOW_ID]) continue;
        auto& order = *orders[districtID - DISTRICT_LOW_ID];

        // Delete new order
        auto delFuture = session.ExecuteModify(
            "DELETE FROM new_order WHERE no_w_id = $1 AND no_d_id = $2 AND no_o_id = $3",
            pqxx::params{warehouseID, districtID, order.OrderID});
        co_await TSuspendWithFuture(std::move(delFuture), context.TaskQueue, context.TerminalID);

        // Update carrier ID
        auto updFuture = session.ExecuteModify(
            "UPDATE oorder SET o_carrier_id = $1 WHERE o_w_id = $2 AND o_d_id = $3 AND o_id = $4",
            pqxx::params{carrierID, warehouseID, districtID, order.OrderID});
        co_await TSuspendWithFuture(std::move(updFuture), context.TaskQueue, context.TerminalID);

        // Update delivery date on order lines
        auto updOlFuture = session.ExecuteModify(
            "UPDATE order_line SET ol_delivery_d = CURRENT_TIMESTAMP "
            "WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
            pqxx::params{warehouseID, districtID, order.OrderID});
        co_await TSuspendWithFuture(std::move(updOlFuture), context.TaskQueue, context.TerminalID);

        // Update customer balance and delivery count
        auto updCustFuture = session.ExecuteModify(
            "UPDATE customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + 1 "
            "WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4",
            pqxx::params{order.TotalAmount, warehouseID, districtID, order.CustomerId});
        co_await TSuspendWithFuture(std::move(updCustFuture), context.TaskQueue, context.TerminalID);
    }

    LOG_T("Terminal {} committing Delivery", context.TerminalID);

    auto commitFuture = session.Commit();
    co_await TSuspendWithFuture(std::move(commitFuture), context.TaskQueue, context.TerminalID);

    auto endTs = std::chrono::steady_clock::now();
    latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);

    co_return true;
}

} // namespace NTPCC
