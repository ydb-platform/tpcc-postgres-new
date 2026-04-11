#include "transactions.h"
#include "coro_traits.h"

#include "constants.h"
#include "log.h"
#include "util.h"

#include <string>
#include <unordered_map>
#include <vector>
#include <set>

namespace NTPCC {

std::atomic<size_t> TransactionsInflight{0};

namespace {

//-----------------------------------------------------------------------------

struct Stock {
    int s_w_id;
    int s_i_id;
    int s_quantity;
    double s_ytd;
    int s_order_cnt;
    int s_remote_cnt;
    std::string s_data;
    std::string s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
    std::string s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10;
};

struct TPairHash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& pair) const {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};

std::string GetDistInfo(int districtID, const Stock& stock) {
    switch (districtID) {
        case 1: return stock.s_dist_01;
        case 2: return stock.s_dist_02;
        case 3: return stock.s_dist_03;
        case 4: return stock.s_dist_04;
        case 5: return stock.s_dist_05;
        case 6: return stock.s_dist_06;
        case 7: return stock.s_dist_07;
        case 8: return stock.s_dist_08;
        case 9: return stock.s_dist_09;
        case 10: return stock.s_dist_10;
        default: return {};
    }
}

} // anonymous

//-----------------------------------------------------------------------------

TFuture<bool> GetNewOrderTask(
    TTransactionContext& context,
    std::chrono::microseconds& latency,
    PgSession& session)
{
    auto startTs = std::chrono::steady_clock::now();

    TTransactionInflightGuard guard;
    co_await TTaskReady(context.TaskQueue, context.TerminalID);

    const int warehouseID = context.WarehouseID;
    const int districtID = RandomNumber(DISTRICT_LOW_ID, DISTRICT_HIGH_ID);
    const int customerID = GetRandomCustomerID();

    LOG_T("Terminal {} started NewOrder: W={}, D={}, C={}", context.TerminalID, warehouseID, districtID, customerID);

    const int numItems = RandomNumber(MIN_ITEMS, MAX_ITEMS);

    std::vector<int> itemIDs;
    std::vector<int> supplierWarehouseIDs;
    std::vector<int> orderQuantities;
    itemIDs.reserve(numItems);
    supplierWarehouseIDs.reserve(numItems);
    orderQuantities.reserve(numItems);
    int allLocal = 1;

    for (int i = 0; i < numItems; ++i) {
        itemIDs.push_back(GetRandomItemID());
        if (RandomNumber(1, 100) > 1) {
            supplierWarehouseIDs.push_back(warehouseID);
        } else {
            int supplierID;
            do {
                supplierID = RandomNumber(1, context.WarehouseCount);
            } while (supplierID == warehouseID && context.WarehouseCount > 1);
            supplierWarehouseIDs.push_back(supplierID);
            allLocal = 0;
        }
        orderQuantities.push_back(RandomNumber(1, 10));
    }

    bool hasInvalidItem = false;
    if (RandomNumber(1, 100) == 1) {
        itemIDs[numItems - 1] = INVALID_ITEM_ID;
        hasInvalidItem = true;
    }

    // Get customer discount/credit
    auto custFuture = session.ExecuteQuery(
        "SELECT c_discount, c_last, c_credit FROM customer "
        "WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
        pqxx::params{warehouseID, districtID, customerID});
    auto custResult = co_await TSuspendWithFuture(std::move(custFuture), context.TaskQueue, context.TerminalID);

    // Get warehouse tax
    auto whFuture = session.ExecuteQuery(
        "SELECT w_tax FROM warehouse WHERE w_id = $1",
        pqxx::params{warehouseID});
    auto whResult = co_await TSuspendWithFuture(std::move(whFuture), context.TaskQueue, context.TerminalID);

    // Get district info with FOR UPDATE
    auto distFuture = session.ExecuteQuery(
        "SELECT d_next_o_id, d_tax FROM district "
        "WHERE d_w_id = $1 AND d_id = $2 FOR UPDATE",
        pqxx::params{warehouseID, districtID});
    auto distResult = co_await TSuspendWithFuture(std::move(distFuture), context.TaskQueue, context.TerminalID);

    if (!distResult.TryNextRow()) {
        LOG_E("Terminal {} district not found: W={}, D={}", context.TerminalID, warehouseID, districtID);
        RequestStopWithError();
        co_return false;
    }
    int nextOrderID = distResult.GetInt32("d_next_o_id");

    // Update district next order ID
    auto updDistFuture = session.ExecuteModify(
        "UPDATE district SET d_next_o_id = d_next_o_id + 1 "
        "WHERE d_w_id = $1 AND d_id = $2",
        pqxx::params{warehouseID, districtID});
    co_await TSuspendWithFuture(std::move(updDistFuture), context.TaskQueue, context.TerminalID);

    // Insert into oorder
    auto oorderFuture = session.ExecuteModify(
        "INSERT INTO oorder (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, o_all_local, o_entry_d) "
        "VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)",
        pqxx::params{warehouseID, districtID, nextOrderID, customerID, numItems, allLocal});
    co_await TSuspendWithFuture(std::move(oorderFuture), context.TaskQueue, context.TerminalID);

    // Insert into new_order
    auto noFuture = session.ExecuteModify(
        "INSERT INTO new_order (no_w_id, no_d_id, no_o_id) VALUES ($1, $2, $3)",
        pqxx::params{warehouseID, districtID, nextOrderID});
    co_await TSuspendWithFuture(std::move(noFuture), context.TaskQueue, context.TerminalID);

    // Get items one by one and check for invalid item
    std::unordered_map<int, double> itemPrices;
    for (int i = 0; i < numItems; ++i) {
        if (itemIDs[i] == INVALID_ITEM_ID) continue;

        auto itemFuture = session.ExecuteQuery(
            "SELECT i_price, i_name, i_data FROM item WHERE i_id = $1",
            pqxx::params{itemIDs[i]});
        auto itemResult = co_await TSuspendWithFuture(std::move(itemFuture), context.TaskQueue, context.TerminalID);

        if (itemResult.TryNextRow()) {
            itemPrices[itemIDs[i]] = itemResult.GetDouble("i_price");
        }
    }

    if (hasInvalidItem) {
        auto rbFuture = session.Rollback();
        co_await TSuspendWithFuture(std::move(rbFuture), context.TaskQueue, context.TerminalID);
        throw TUserAbortedException();
    }

    // Get and update stock for each item
    std::unordered_map<std::pair<int, int>, Stock, TPairHash> stocks;
    for (int i = 0; i < numItems; ++i) {
        auto key = std::make_pair(supplierWarehouseIDs[i], itemIDs[i]);
        if (stocks.count(key)) continue;

        auto stockFuture = session.ExecuteQuery(
            "SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt, "
            "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
            "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 "
            "FROM stock WHERE s_w_id = $1 AND s_i_id = $2 FOR UPDATE",
            pqxx::params{supplierWarehouseIDs[i], itemIDs[i]});
        auto stockResult = co_await TSuspendWithFuture(std::move(stockFuture), context.TaskQueue, context.TerminalID);

        if (stockResult.TryNextRow()) {
            Stock s;
            s.s_w_id = supplierWarehouseIDs[i];
            s.s_i_id = itemIDs[i];
            s.s_quantity = stockResult.GetInt32("s_quantity");
            s.s_ytd = stockResult.GetDouble("s_ytd");
            s.s_order_cnt = stockResult.GetInt32("s_order_cnt");
            s.s_remote_cnt = stockResult.GetInt32("s_remote_cnt");
            s.s_dist_01 = stockResult.GetString("s_dist_01");
            s.s_dist_02 = stockResult.GetString("s_dist_02");
            s.s_dist_03 = stockResult.GetString("s_dist_03");
            s.s_dist_04 = stockResult.GetString("s_dist_04");
            s.s_dist_05 = stockResult.GetString("s_dist_05");
            s.s_dist_06 = stockResult.GetString("s_dist_06");
            s.s_dist_07 = stockResult.GetString("s_dist_07");
            s.s_dist_08 = stockResult.GetString("s_dist_08");
            s.s_dist_09 = stockResult.GetString("s_dist_09");
            s.s_dist_10 = stockResult.GetString("s_dist_10");
            stocks[key] = std::move(s);
        }
    }

    // Process order lines, update stock, insert order lines
    for (int olNum = 1; olNum <= numItems; ++olNum) {
        int supWh = supplierWarehouseIDs[olNum - 1];
        int iid = itemIDs[olNum - 1];
        int qty = orderQuantities[olNum - 1];

        double iPrice = itemPrices[iid];
        double olAmount = qty * iPrice;

        auto& stock = stocks[std::make_pair(supWh, iid)];
        if (stock.s_quantity - qty >= 10) {
            stock.s_quantity -= qty;
        } else {
            stock.s_quantity += -qty + 91;
        }

        auto updStockFuture = session.ExecuteModify(
            "UPDATE stock SET s_quantity = $1, s_ytd = s_ytd + $2, "
            "s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + $3 "
            "WHERE s_w_id = $4 AND s_i_id = $5",
            pqxx::params{stock.s_quantity, static_cast<double>(qty),
                         (supWh == warehouseID ? 0 : 1), supWh, iid});
        co_await TSuspendWithFuture(std::move(updStockFuture), context.TaskQueue, context.TerminalID);

        std::string distInfo = GetDistInfo(districtID, stock);

        auto olFuture = session.ExecuteModify(
            "INSERT INTO order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, "
            "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            pqxx::params{warehouseID, districtID, nextOrderID, olNum, iid,
                         olAmount, supWh, static_cast<double>(qty), distInfo});
        co_await TSuspendWithFuture(std::move(olFuture), context.TaskQueue, context.TerminalID);
    }

    LOG_T("Terminal {} committing NewOrder", context.TerminalID);

    auto commitFuture = session.Commit();
    co_await TSuspendWithFuture(std::move(commitFuture), context.TaskQueue, context.TerminalID);

    auto endTs = std::chrono::steady_clock::now();
    latency = std::chrono::duration_cast<std::chrono::microseconds>(endTs - startTs);

    co_return true;
}

} // namespace NTPCC
