#include "transactions.h"
#include "coro_traits.h"

#include "common_queries.h"
#include "constants.h"
#include "log.h"
#include "util.h"

#include <string>

namespace NTPCC {

//-----------------------------------------------------------------------------

TFuture<bool> GetOrderStatusTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session)
{
    auto startTs = std::chrono::steady_clock::now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);

    LOG_T("Terminal {} started OrderStatus: W={}, D={}", context.TerminalID, warehouseID, districtID);

    bool lookupByName = RandomNumber(1, 100) <= 60;

    TCustomer customer;

    if (lookupByName) {
        std::string lastName = GetNonUniformRandomLastNameForRun();

        auto custFuture = GetCustomersByLastName(session, warehouseID, districtID, lastName);
        auto custResult = co_await TSuspendWithFuture(std::move(custFuture), context.TaskQueue, context.TerminalID);

        auto selectedCustomer = SelectCustomerFromResultSet(custResult);
        if (!selectedCustomer) {
            LOG_E("Terminal {} no customer by name: {}", context.TerminalID, lastName);
            RequestStopWithError();
            co_return false;
        }
        customer = std::move(*selectedCustomer);
    } else {
        int customerID = GetRandomCustomerID();

        auto custFuture = GetCustomerById(session, warehouseID, districtID, customerID);
        auto custResult = co_await TSuspendWithFuture(std::move(custFuture), context.TaskQueue, context.TerminalID);

        if (!custResult.TryNextRow()) {
            LOG_E("Terminal {} customer not found: C={}", context.TerminalID, customerID);
            RequestStopWithError();
            co_return false;
        }
        customer = ParseCustomerFromResult(custResult);
        customer.c_id = customerID;
    }

    // Get the newest order for this customer (PostgreSQL uses the idx_order index automatically)
    auto orderFuture = session.ExecuteQuery(
        "SELECT o_id, o_carrier_id, o_entry_d FROM oorder "
        "WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3 "
        "ORDER BY o_id DESC LIMIT 1",
        pqxx::params{warehouseID, districtID, customer.c_id});
    auto orderResult = co_await TSuspendWithFuture(std::move(orderFuture), context.TaskQueue, context.TerminalID);

    if (!orderResult.TryNextRow()) {
        LOG_T("Terminal {} customer has no orders", context.TerminalID);
        auto commitFuture = session.Commit();
        co_await TSuspendWithFuture(std::move(commitFuture), context.TaskQueue, context.TerminalID);
        auto endTs = std::chrono::steady_clock::now();
        latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);
        co_return true;
    }
    int orderID = orderResult.GetInt32("o_id");

    // Get order lines
    auto olFuture = session.ExecuteQuery(
        "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
        "FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
        pqxx::params{warehouseID, districtID, orderID});
    auto olResult = co_await TSuspendWithFuture(std::move(olFuture), context.TaskQueue, context.TerminalID);

    LOG_T("Terminal {} committing OrderStatus: C={}, O={}", context.TerminalID, customer.c_id, orderID);

    auto commitFuture = session.Commit();
    co_await TSuspendWithFuture(std::move(commitFuture), context.TaskQueue, context.TerminalID);

    auto endTs = std::chrono::steady_clock::now();
    latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);

    co_return true;
}

} // namespace NTPCC
