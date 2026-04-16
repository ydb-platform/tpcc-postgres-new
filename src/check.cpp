#include "check.h"

#include "constants.h"
#include "log.h"

#include <pqxx/pqxx>

#include <fmt/format.h>
#include <iostream>
#include <string>
#include <vector>

namespace NTPCC {

namespace {

void CheckNoRows(pqxx::nontransaction& txn, const std::string& sql, const std::string& description = {}) {
    auto result = txn.exec(sql);
    if (!result.empty()) {
        throw std::runtime_error(
            description.empty() ? "Unexpected rows returned" : description);
    }
}

void WaitCheck(const std::string& description, auto checkFn) {
    std::cout << "Checking " << description << " " << std::flush;
    try {
        checkFn();
        std::cout << "[OK]" << std::endl;
    } catch (const std::exception& ex) {
        std::cout << "[Failed]: " << ex.what() << std::endl;
        throw;
    }
}

//-----------------------------------------------------------------------------

void BaseCheckWarehouseTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*) AS count, MAX(W_ID) AS max, MIN(W_ID) AS min FROM {}", TABLE_WAREHOUSE)).one_row();

    auto rowCount = r[0].as<int64_t>();
    auto maxWh = r[1].as<int>();
    auto minWh = r[2].as<int>();

    if (rowCount != expectedWhNumber || minWh != 1 || maxWh != expectedWhNumber) {
        throw std::runtime_error(fmt::format(
            "Inconsistent {}: count={}, min={}, max={}, expected={}",
            TABLE_WAREHOUSE, rowCount, minWh, maxWh, expectedWhNumber));
    }
}

void BaseCheckDistrictTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*) AS count, "
        "MAX(D_W_ID) AS max_w_id, MIN(D_W_ID) AS min_w_id, "
        "MAX(D_ID) AS max_d_id, MIN(D_ID) AS min_d_id "
        "FROM {}", TABLE_DISTRICT)).one_row();

    int expectedCount = expectedWhNumber * DISTRICT_COUNT;
    auto rowCount = r[0].as<int64_t>();
    if (rowCount != expectedCount)
        throw std::runtime_error(fmt::format("District count is {} and not {}", rowCount, expectedCount));

    auto maxWh = r[1].as<int>(), minWh = r[2].as<int>();
    auto maxDist = r[3].as<int>(), minDist = r[4].as<int>();

    if (minWh != 1 || maxWh != expectedWhNumber)
        throw std::runtime_error(fmt::format("District warehouse range [{}, {}] instead of [1, {}]", minWh, maxWh, expectedWhNumber));
    if (minDist != DISTRICT_LOW_ID || maxDist != DISTRICT_HIGH_ID)
        throw std::runtime_error(fmt::format("District ID range [{}, {}] instead of [{}, {}]", minDist, maxDist, DISTRICT_LOW_ID, DISTRICT_HIGH_ID));
}

void BaseCheckCustomerTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*) AS count, "
        "MAX(C_W_ID), MIN(C_W_ID), MAX(C_D_ID), MIN(C_D_ID), MAX(C_ID), MIN(C_ID) "
        "FROM {}", TABLE_CUSTOMER)).one_row();

    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_COUNT;
    auto rowCount = r[0].as<int64_t>();
    if (rowCount != expectedCount)
        throw std::runtime_error(fmt::format("Customer count is {} and not {}", rowCount, expectedCount));

    if (r[2].as<int>() != 1 || r[1].as<int>() != expectedWhNumber)
        throw std::runtime_error(fmt::format("Customer warehouse range [{}, {}] instead of [1, {}]", r[2].as<int>(), r[1].as<int>(), expectedWhNumber));
    if (r[4].as<int>() != DISTRICT_LOW_ID || r[3].as<int>() != DISTRICT_HIGH_ID)
        throw std::runtime_error("Customer district range mismatch");
    if (r[6].as<int>() != 1 || r[5].as<int>() != CUSTOMERS_PER_DISTRICT)
        throw std::runtime_error("Customer ID range mismatch");
}

void BaseCheckItemTable(pqxx::nontransaction& txn) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*), MAX(I_ID), MIN(I_ID) FROM {}", TABLE_ITEM)).one_row();

    auto rowCount = r[0].as<int64_t>();
    if (rowCount != ITEM_COUNT)
        throw std::runtime_error(fmt::format("Item count is {} and not {}", rowCount, ITEM_COUNT));
    if (r[2].as<int>() != 1 || r[1].as<int>() != ITEM_COUNT)
        throw std::runtime_error("Item ID range mismatch");
}

void BaseCheckStockTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*), COUNT(DISTINCT S_W_ID), MAX(S_W_ID), MIN(S_W_ID), MAX(S_I_ID), MIN(S_I_ID) "
        "FROM {}", TABLE_STOCK)).one_row();

    int expectedCount = expectedWhNumber * ITEM_COUNT;
    auto rowCount = r[0].as<int64_t>();
    if (rowCount != expectedCount)
        throw std::runtime_error(fmt::format("Stock count is {} and not {}", rowCount, expectedCount));

    auto whCount = r[1].as<int>();
    if (whCount != expectedWhNumber)
        throw std::runtime_error(fmt::format("Stock warehouse count is {} and not {}", whCount, expectedWhNumber));
    if (r[3].as<int>() != 1 || r[2].as<int>() != expectedWhNumber)
        throw std::runtime_error("Stock warehouse range mismatch");
    if (r[5].as<int>() != 1 || r[4].as<int>() != ITEM_COUNT)
        throw std::runtime_error("Stock item range mismatch");
}

void BaseCheckOorderTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*), MAX(O_W_ID), MIN(O_W_ID), MAX(O_D_ID), MIN(O_D_ID), MAX(O_ID), MIN(O_ID) "
        "FROM {}", TABLE_OORDER)).one_row();

    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_COUNT;
    auto rowCount = r[0].as<int64_t>();
    if (rowCount != expectedCount)
        throw std::runtime_error(fmt::format("Order count is {} and not {}", rowCount, expectedCount));

    if (r[2].as<int>() != 1 || r[1].as<int>() != expectedWhNumber)
        throw std::runtime_error("Order warehouse range mismatch");
    if (r[4].as<int>() != DISTRICT_LOW_ID || r[3].as<int>() != DISTRICT_HIGH_ID)
        throw std::runtime_error("Order district range mismatch");
    if (r[6].as<int>() != 1 || r[5].as<int>() != CUSTOMERS_PER_DISTRICT)
        throw std::runtime_error("Order ID range mismatch");
}

void BaseCheckNewOrderTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*), MAX(NO_W_ID), MIN(NO_W_ID), MAX(NO_D_ID), MIN(NO_D_ID), MAX(NO_O_ID), MIN(NO_O_ID) "
        "FROM {}", TABLE_NEW_ORDER)).one_row();

    const auto newOrdersPerDistrict = CUSTOMERS_PER_DISTRICT - FIRST_UNPROCESSED_O_ID + 1;
    int expectedCount = expectedWhNumber * newOrdersPerDistrict * DISTRICT_COUNT;
    auto rowCount = r[0].as<int64_t>();
    if (rowCount != expectedCount)
        throw std::runtime_error(fmt::format("New order count is {} and not {}", rowCount, expectedCount));

    if (r[2].as<int>() != 1 || r[1].as<int>() != expectedWhNumber)
        throw std::runtime_error("New order warehouse range mismatch");
    if (r[4].as<int>() != DISTRICT_LOW_ID || r[3].as<int>() != DISTRICT_HIGH_ID)
        throw std::runtime_error("New order district range mismatch");
    if (r[6].as<int>() < FIRST_UNPROCESSED_O_ID || r[5].as<int>() != CUSTOMERS_PER_DISTRICT)
        throw std::runtime_error("New order ID range mismatch");
}

void BaseCheckOrderLineTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT MIN(order_count) AS min_orders, MAX(order_count) AS max_orders, COUNT(*) AS district_count "
        "FROM ("
        "  SELECT OL_W_ID, OL_D_ID, COUNT(DISTINCT OL_O_ID) AS order_count "
        "  FROM {} GROUP BY OL_W_ID, OL_D_ID"
        ") sub", TABLE_ORDER_LINE)).one_row();

    int expectedDistrictCount = expectedWhNumber * DISTRICT_COUNT;
    auto districtCount = r[2].as<int64_t>();
    if (districtCount != expectedDistrictCount)
        throw std::runtime_error(fmt::format("Order line district count is {} and not {}", districtCount, expectedDistrictCount));

    auto minOrders = r[0].as<int64_t>();
    auto maxOrders = r[1].as<int64_t>();
    if (minOrders != CUSTOMERS_PER_DISTRICT || maxOrders != CUSTOMERS_PER_DISTRICT)
        throw std::runtime_error(fmt::format("Order line orders per district [{}, {}] instead of [{}, {}]",
            minOrders, maxOrders, CUSTOMERS_PER_DISTRICT, CUSTOMERS_PER_DISTRICT));
}

void BaseCheckHistoryTable(pqxx::nontransaction& txn, int expectedWhNumber) {
    auto r = txn.exec(fmt::format(
        "SELECT COUNT(*), MAX(H_C_W_ID), MIN(H_C_W_ID) FROM {}", TABLE_HISTORY)).one_row();

    int expectedCount = expectedWhNumber * CUSTOMERS_PER_DISTRICT * DISTRICT_COUNT;
    auto rowCount = r[0].as<int64_t>();
    if (rowCount != expectedCount)
        throw std::runtime_error(fmt::format("History count is {} and not {}", rowCount, expectedCount));
    if (r[2].as<int>() != 1 || r[1].as<int>() != expectedWhNumber)
        throw std::runtime_error("History warehouse range mismatch");
}

//-----------------------------------------------------------------------------
// Consistency checks based on TPC-C spec section 3.3.2
//-----------------------------------------------------------------------------

void ConsistencyCheck3321(pqxx::nontransaction& txn) {
    // W_YTD = sum(D_YTD) for each warehouse
    std::string sql = fmt::format(
        "SELECT w.W_ID, w.W_YTD, d.sum_d_ytd "
        "FROM {} AS w "
        "JOIN (SELECT D_W_ID, SUM(D_YTD) AS sum_d_ytd FROM {} GROUP BY D_W_ID) AS d "
        "ON w.W_ID = d.D_W_ID "
        "WHERE ABS(w.W_YTD - d.sum_d_ytd) > 1e-3 LIMIT 1",
        TABLE_WAREHOUSE, TABLE_DISTRICT);
    CheckNoRows(txn, sql);
}

void ConsistencyCheck3322(pqxx::nontransaction& txn) {
    // D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
    std::string sql = fmt::format(
        "SELECT d.D_W_ID, d.D_ID, d.D_NEXT_O_ID, o.max_o_id, n.max_no_o_id "
        "FROM {} AS d "
        "LEFT JOIN (SELECT O_W_ID, O_D_ID, MAX(O_ID) AS max_o_id FROM {} GROUP BY O_W_ID, O_D_ID) AS o "
        "  ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID "
        "LEFT JOIN (SELECT NO_W_ID, NO_D_ID, MAX(NO_O_ID) AS max_no_o_id FROM {} GROUP BY NO_W_ID, NO_D_ID) AS n "
        "  ON d.D_W_ID = n.NO_W_ID AND d.D_ID = n.NO_D_ID "
        "WHERE (d.D_NEXT_O_ID - 1) != o.max_o_id OR o.max_o_id != n.max_no_o_id "
        "LIMIT 1",
        TABLE_DISTRICT, TABLE_OORDER, TABLE_NEW_ORDER);
    CheckNoRows(txn, sql);
}

void ConsistencyCheck3323(pqxx::nontransaction& txn) {
    // max(NO_O_ID) - min(NO_O_ID) + 1 = count(*) for each warehouse/district
    std::string sql = fmt::format(
        "SELECT NO_W_ID, NO_D_ID, COUNT(*) - (MAX(NO_O_ID) - MIN(NO_O_ID) + 1) AS delta "
        "FROM {} GROUP BY NO_W_ID, NO_D_ID "
        "HAVING COUNT(*) - (MAX(NO_O_ID) - MIN(NO_O_ID) + 1) != 0 LIMIT 1",
        TABLE_NEW_ORDER);
    CheckNoRows(txn, sql);
}

void ConsistencyCheck3324(pqxx::nontransaction& txn, int warehouseCount) {
    // sum(O_OL_CNT) = count of order_lines per district
    const int RANGE_SIZE = 50;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT o.O_W_ID, o.O_D_ID, o.sum_ol_cnt, ol.ol_count "
            "FROM (SELECT O_W_ID, O_D_ID, SUM(O_OL_CNT) AS sum_ol_cnt "
            "      FROM {} WHERE O_W_ID >= {} AND O_W_ID <= {} GROUP BY O_W_ID, O_D_ID) AS o "
            "FULL JOIN (SELECT OL_W_ID, OL_D_ID, COUNT(*) AS ol_count "
            "           FROM {} WHERE OL_W_ID >= {} AND OL_W_ID <= {} GROUP BY OL_W_ID, OL_D_ID) AS ol "
            "  ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID "
            "WHERE o.sum_ol_cnt != ol.ol_count LIMIT 1",
            TABLE_OORDER, startWh, endWh, TABLE_ORDER_LINE, startWh, endWh);
        CheckNoRows(txn, sql, fmt::format("3.3.2.4 w_id [{},{}]", startWh, endWh));
    }
}

void ConsistencyCheck3325(pqxx::nontransaction& txn, int warehouseCount) {
    // For every new_order row, the order must exist and have no carrier (O_CARRIER_ID IS NULL or 0).
    // For every order with no carrier, a new_order row must exist.
    const int RANGE_SIZE = 50;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT * FROM ("
            "  SELECT no.NO_W_ID, no.NO_D_ID, no.NO_O_ID "
            "  FROM {} AS no "
            "  LEFT JOIN {} AS o "
            "    ON no.NO_W_ID = o.O_W_ID AND no.NO_D_ID = o.O_D_ID AND no.NO_O_ID = o.O_ID "
            "  WHERE no.NO_W_ID >= {} AND no.NO_W_ID <= {} "
            "    AND (o.O_W_ID IS NULL OR (o.O_CARRIER_ID IS NOT NULL AND o.O_CARRIER_ID != 0)) "
            "  UNION ALL "
            "  SELECT o2.O_W_ID, o2.O_D_ID, o2.O_ID "
            "  FROM {} AS o2 "
            "  LEFT JOIN {} AS no2 "
            "    ON o2.O_W_ID = no2.NO_W_ID AND o2.O_D_ID = no2.NO_D_ID AND o2.O_ID = no2.NO_O_ID "
            "  WHERE o2.O_W_ID >= {} AND o2.O_W_ID <= {} "
            "    AND (o2.O_CARRIER_ID IS NULL OR o2.O_CARRIER_ID = 0) AND no2.NO_W_ID IS NULL"
            ") sub LIMIT 1",
            TABLE_NEW_ORDER, TABLE_OORDER, startWh, endWh,
            TABLE_OORDER, TABLE_NEW_ORDER, startWh, endWh);
        CheckNoRows(txn, sql, fmt::format("3.3.2.5 w_id [{},{}]", startWh, endWh));
    }
}

void ConsistencyCheck3326(pqxx::nontransaction& txn, int warehouseCount) {
    // For each order: O_OL_CNT must equal count of order_lines, and every order_line must have a parent order
    const int RANGE_SIZE = 50;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT * FROM ("
            "  SELECT o.O_W_ID, o.O_D_ID, o.O_ID "
            "  FROM {} AS o "
            "  LEFT JOIN (SELECT OL_W_ID, OL_D_ID, OL_O_ID, COUNT(*) AS cnt "
            "             FROM {} WHERE OL_W_ID >= {} AND OL_W_ID <= {} "
            "             GROUP BY OL_W_ID, OL_D_ID, OL_O_ID) AS l "
            "    ON o.O_W_ID = l.OL_W_ID AND o.O_D_ID = l.OL_D_ID AND o.O_ID = l.OL_O_ID "
            "  WHERE o.O_W_ID >= {} AND o.O_W_ID <= {} AND o.O_OL_CNT != COALESCE(l.cnt, 0) "
            "  UNION ALL "
            "  SELECT l2.OL_W_ID, l2.OL_D_ID, l2.OL_O_ID "
            "  FROM (SELECT DISTINCT OL_W_ID, OL_D_ID, OL_O_ID FROM {} "
            "        WHERE OL_W_ID >= {} AND OL_W_ID <= {}) AS l2 "
            "  LEFT JOIN {} AS o2 "
            "    ON l2.OL_W_ID = o2.O_W_ID AND l2.OL_D_ID = o2.O_D_ID AND l2.OL_O_ID = o2.O_ID "
            "  WHERE o2.O_W_ID IS NULL"
            ") sub LIMIT 1",
            TABLE_OORDER, TABLE_ORDER_LINE, startWh, endWh, startWh, endWh,
            TABLE_ORDER_LINE, startWh, endWh, TABLE_OORDER);
        CheckNoRows(txn, sql, fmt::format("3.3.2.6 w_id [{},{}]", startWh, endWh));
    }
}

void ConsistencyCheck3327(pqxx::nontransaction& txn, int warehouseCount) {
    // O_CARRIER_ID set => all OL_DELIVERY_D set; O_CARRIER_ID null => all OL_DELIVERY_D null
    const int RANGE_SIZE = 10;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT l.OL_W_ID, l.OL_D_ID, l.OL_O_ID "
            "FROM ("
            "  SELECT OL_W_ID, OL_D_ID, OL_O_ID, "
            "    BOOL_AND(OL_DELIVERY_D IS NOT NULL) AS all_delivered, "
            "    BOOL_OR(OL_DELIVERY_D IS NULL) AS some_null "
            "  FROM {} WHERE OL_W_ID >= {} AND OL_W_ID <= {} "
            "  GROUP BY OL_W_ID, OL_D_ID, OL_O_ID"
            ") AS l "
            "JOIN {} AS o ON l.OL_W_ID = o.O_W_ID AND l.OL_D_ID = o.O_D_ID AND l.OL_O_ID = o.O_ID "
            "WHERE (o.O_CARRIER_ID IS NULL AND l.all_delivered = true) "
            "   OR (o.O_CARRIER_ID IS NOT NULL AND l.some_null = true) "
            "LIMIT 1",
            TABLE_ORDER_LINE, startWh, endWh, TABLE_OORDER);
        CheckNoRows(txn, sql, fmt::format("3.3.2.7 w_id [{},{}]", startWh, endWh));
    }
}

void ConsistencyCheck3328(pqxx::nontransaction& txn) {
    // W_YTD = sum(H_AMOUNT) grouped by warehouse
    std::string sql = fmt::format(
        "SELECT w.W_ID, w.W_YTD, h.sum_h "
        "FROM {} AS w "
        "JOIN (SELECT H_W_ID, SUM(H_AMOUNT) AS sum_h FROM {} GROUP BY H_W_ID) AS h "
        "  ON w.W_ID = h.H_W_ID "
        "WHERE ABS(w.W_YTD - h.sum_h) > 1e-3 LIMIT 1",
        TABLE_WAREHOUSE, TABLE_HISTORY);
    CheckNoRows(txn, sql);
}

void ConsistencyCheck3329(pqxx::nontransaction& txn) {
    // D_YTD = sum(H_AMOUNT) grouped by warehouse+district
    std::string sql = fmt::format(
        "SELECT d.D_W_ID, d.D_ID, d.D_YTD, h.sum_h "
        "FROM {} AS d "
        "JOIN (SELECT H_W_ID, H_D_ID, SUM(H_AMOUNT) AS sum_h FROM {} GROUP BY H_W_ID, H_D_ID) AS h "
        "  ON d.D_W_ID = h.H_W_ID AND d.D_ID = h.H_D_ID "
        "WHERE ABS(d.D_YTD - h.sum_h) > 1e-3 LIMIT 1",
        TABLE_DISTRICT, TABLE_HISTORY);
    CheckNoRows(txn, sql);
}

void ConsistencyCheck33210(pqxx::nontransaction& txn, int warehouseCount) {
    // C_BALANCE = sum(delivered OL_AMOUNTs) - sum(H_AMOUNT) for each customer
    const int RANGE_SIZE = 10;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT c.C_W_ID, c.C_D_ID, c.C_ID, c.C_BALANCE "
            "FROM {} AS c "
            "LEFT JOIN ("
            "  SELECT o.O_W_ID AS W_ID, o.O_D_ID AS D_ID, o.O_C_ID AS C_ID, SUM(ol.OL_AMOUNT) AS ol_sum "
            "  FROM {} AS o "
            "  JOIN {} AS ol ON ol.OL_W_ID = o.O_W_ID AND ol.OL_D_ID = o.O_D_ID AND ol.OL_O_ID = o.O_ID "
            "  WHERE ol.OL_DELIVERY_D IS NOT NULL AND o.O_W_ID >= {} AND o.O_W_ID <= {} "
            "  GROUP BY o.O_W_ID, o.O_D_ID, o.O_C_ID"
            ") AS ols ON c.C_W_ID = ols.W_ID AND c.C_D_ID = ols.D_ID AND c.C_ID = ols.C_ID "
            "LEFT JOIN ("
            "  SELECT H_C_W_ID, H_C_D_ID, H_C_ID, SUM(H_AMOUNT) AS h_sum "
            "  FROM {} WHERE H_C_W_ID >= {} AND H_C_W_ID <= {} "
            "  GROUP BY H_C_W_ID, H_C_D_ID, H_C_ID"
            ") AS hs ON c.C_W_ID = hs.H_C_W_ID AND c.C_D_ID = hs.H_C_D_ID AND c.C_ID = hs.H_C_ID "
            "WHERE c.C_W_ID >= {} AND c.C_W_ID <= {} "
            "  AND ABS(c.C_BALANCE - (COALESCE(ols.ol_sum, 0) - COALESCE(hs.h_sum, 0))) > 1e-3 "
            "LIMIT 1",
            TABLE_CUSTOMER,
            TABLE_OORDER, TABLE_ORDER_LINE, startWh, endWh,
            TABLE_HISTORY, startWh, endWh,
            startWh, endWh);
        CheckNoRows(txn, sql, fmt::format("3.3.2.10 w_id [{},{}]", startWh, endWh));
    }
}

void ConsistencyCheck33211(pqxx::nontransaction& txn, int warehouseCount) {
    const int RANGE_SIZE = 50;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT o.O_W_ID, o.O_D_ID, (o.order_cnt - n.new_order_cnt) AS delta "
            "FROM (SELECT O_W_ID, O_D_ID, COUNT(*) AS order_cnt FROM {} "
            "      WHERE O_W_ID >= {} AND O_W_ID <= {} GROUP BY O_W_ID, O_D_ID) AS o "
            "JOIN (SELECT NO_W_ID, NO_D_ID, COUNT(*) AS new_order_cnt FROM {} "
            "      WHERE NO_W_ID >= {} AND NO_W_ID <= {} GROUP BY NO_W_ID, NO_D_ID) AS n "
            "  ON o.O_W_ID = n.NO_W_ID AND o.O_D_ID = n.NO_D_ID "
            "WHERE (o.order_cnt - n.new_order_cnt) != {} LIMIT 1",
            TABLE_OORDER, startWh, endWh,
            TABLE_NEW_ORDER, startWh, endWh,
            FIRST_UNPROCESSED_O_ID - 1);
        CheckNoRows(txn, sql, fmt::format("3.3.2.11 w_id [{},{}]", startWh, endWh));
    }
}

void ConsistencyCheck33212(pqxx::nontransaction& txn, int warehouseCount) {
    // C_BALANCE + C_YTD_PAYMENT = sum(delivered OL_AMOUNTs) for each customer
    const int RANGE_SIZE = 10;
    for (int startWh = 1; startWh <= warehouseCount; startWh += RANGE_SIZE) {
        int endWh = std::min(startWh + RANGE_SIZE - 1, warehouseCount);
        std::string sql = fmt::format(
            "SELECT c.C_W_ID, c.C_D_ID, c.C_ID "
            "FROM {} AS c "
            "JOIN ("
            "  SELECT o.O_W_ID AS W_ID, o.O_D_ID AS D_ID, o.O_C_ID AS C_ID, SUM(ol.OL_AMOUNT) AS ol_sum "
            "  FROM {} AS o "
            "  JOIN {} AS ol ON ol.OL_W_ID = o.O_W_ID AND ol.OL_D_ID = o.O_D_ID AND ol.OL_O_ID = o.O_ID "
            "  WHERE ol.OL_DELIVERY_D IS NOT NULL AND o.O_W_ID >= {} AND o.O_W_ID <= {} "
            "  GROUP BY o.O_W_ID, o.O_D_ID, o.O_C_ID"
            ") AS l ON c.C_W_ID = l.W_ID AND c.C_D_ID = l.D_ID AND c.C_ID = l.C_ID "
            "WHERE c.C_W_ID >= {} AND c.C_W_ID <= {} "
            "  AND ABS(c.C_BALANCE + c.C_YTD_PAYMENT - l.ol_sum) > 1e-3 "
            "LIMIT 1",
            TABLE_CUSTOMER,
            TABLE_OORDER, TABLE_ORDER_LINE, startWh, endWh,
            startWh, endWh);
        CheckNoRows(txn, sql, fmt::format("3.3.2.12 w_id [{},{}]", startWh, endWh));
    }
}

//-----------------------------------------------------------------------------
// Post-import checks: stricter invariants that hold only on freshly loaded data
//-----------------------------------------------------------------------------

void PostImportCheckNextOrderId(pqxx::nontransaction& txn) {
    std::string sql = fmt::format(
        "SELECT D_W_ID, D_ID, D_NEXT_O_ID FROM {} "
        "WHERE D_NEXT_O_ID != {} LIMIT 1",
        TABLE_DISTRICT, CUSTOMERS_PER_DISTRICT + 1);
    CheckNoRows(txn, sql, fmt::format(
        "D_NEXT_O_ID must be {} for all districts after import", CUSTOMERS_PER_DISTRICT + 1));
}

void PostImportCheckWarehouseYtd(pqxx::nontransaction& txn) {
    double expectedYtd = DISTRICT_INITIAL_YTD * DISTRICT_COUNT;
    std::string sql = fmt::format(
        "SELECT W_ID, W_YTD FROM {} WHERE ABS(W_YTD - {}) > 1e-3 LIMIT 1",
        TABLE_WAREHOUSE, expectedYtd);
    CheckNoRows(txn, sql, fmt::format("W_YTD must be {} after import", expectedYtd));
}

void PostImportCheckDistrictYtd(pqxx::nontransaction& txn) {
    std::string sql = fmt::format(
        "SELECT D_W_ID, D_ID, D_YTD FROM {} WHERE ABS(D_YTD - {}) > 1e-3 LIMIT 1",
        TABLE_DISTRICT, DISTRICT_INITIAL_YTD);
    CheckNoRows(txn, sql, fmt::format("D_YTD must be {} after import", DISTRICT_INITIAL_YTD));
}

void PostImportCheckNoCarriers(pqxx::nontransaction& txn) {
    std::string sql = fmt::format(
        "SELECT O_W_ID, O_D_ID, O_ID, O_CARRIER_ID FROM {} "
        "WHERE O_ID >= {} AND O_CARRIER_ID IS NOT NULL LIMIT 1",
        TABLE_OORDER, FIRST_UNPROCESSED_O_ID);
    CheckNoRows(txn, sql, "Unprocessed orders must have NULL O_CARRIER_ID after import");
}

void PostImportCheckNoDeliveryDates(pqxx::nontransaction& txn) {
    std::string sql = fmt::format(
        "SELECT ol.OL_W_ID, ol.OL_D_ID, ol.OL_O_ID FROM {} AS ol "
        "WHERE ol.OL_O_ID >= {} AND ol.OL_DELIVERY_D IS NOT NULL LIMIT 1",
        TABLE_ORDER_LINE, FIRST_UNPROCESSED_O_ID);
    CheckNoRows(txn, sql, "Unprocessed order lines must have NULL OL_DELIVERY_D after import");
}

//-----------------------------------------------------------------------------

} // anonymous

void CheckSync(const std::string& connectionString, int warehouseCount, bool afterImport,
               const std::string& path) {
    if (warehouseCount <= 0) {
        std::cerr << "Zero warehouses specified, nothing to check" << std::endl;
        return;
    }

    pqxx::connection conn(connectionString);

    if (!path.empty()) {
        pqxx::nontransaction ntx(conn);
        ntx.exec(fmt::format("SET search_path TO {}", conn.quote_name(path)));
    }

    pqxx::nontransaction txn(conn);

    int failedCount = 0;

    auto runCheck = [&](const std::string& name, auto fn) {
        std::cout << "Checking " << name << " " << std::flush;
        try {
            fn(txn);
            std::cout << "[OK]" << std::endl;
        } catch (const std::exception& ex) {
            std::cout << "[Failed]: " << ex.what() << std::endl;
            ++failedCount;
        }
    };

    // Static tables: row counts never change during benchmark
    runCheck(TABLE_WAREHOUSE, [&](auto& t) { BaseCheckWarehouseTable(t, warehouseCount); });
    runCheck(TABLE_DISTRICT, [&](auto& t) { BaseCheckDistrictTable(t, warehouseCount); });
    runCheck(TABLE_CUSTOMER, [&](auto& t) { BaseCheckCustomerTable(t, warehouseCount); });
    runCheck(TABLE_ITEM, [&](auto& t) { BaseCheckItemTable(t); });
    runCheck(TABLE_STOCK, [&](auto& t) { BaseCheckStockTable(t, warehouseCount); });

    // Dynamic tables: row counts change during benchmark (NewOrder adds orders/order_lines,
    // Delivery removes new_orders, Payment adds history). Exact count checks are only
    // valid right after import.
    if (afterImport) {
        runCheck(TABLE_OORDER, [&](auto& t) { BaseCheckOorderTable(t, warehouseCount); });
        runCheck(TABLE_NEW_ORDER, [&](auto& t) { BaseCheckNewOrderTable(t, warehouseCount); });
        runCheck(TABLE_ORDER_LINE, [&](auto& t) { BaseCheckOrderLineTable(t, warehouseCount); });
        runCheck(TABLE_HISTORY, [&](auto& t) { BaseCheckHistoryTable(t, warehouseCount); });
    }

    if (failedCount > 0) {
        std::cout << "Base checks failed, aborting consistency checks!" << std::endl;
        throw std::runtime_error("Base checks failed");
    }

    // Consistency checks
    runCheck("3.3.2.1", [&](auto& t) { ConsistencyCheck3321(t); });
    runCheck("3.3.2.2", [&](auto& t) { ConsistencyCheck3322(t); });
    runCheck("3.3.2.3", [&](auto& t) { ConsistencyCheck3323(t); });
    runCheck("3.3.2.4", [&](auto& t) { ConsistencyCheck3324(t, warehouseCount); });
    runCheck("3.3.2.5", [&](auto& t) { ConsistencyCheck3325(t, warehouseCount); });
    runCheck("3.3.2.6", [&](auto& t) { ConsistencyCheck3326(t, warehouseCount); });
    runCheck("3.3.2.7", [&](auto& t) { ConsistencyCheck3327(t, warehouseCount); });
    runCheck("3.3.2.8", [&](auto& t) { ConsistencyCheck3328(t); });
    runCheck("3.3.2.9", [&](auto& t) { ConsistencyCheck3329(t); });
    runCheck("3.3.2.10", [&](auto& t) { ConsistencyCheck33210(t, warehouseCount); });
    runCheck("3.3.2.12", [&](auto& t) { ConsistencyCheck33212(t, warehouseCount); });

    if (afterImport) {
        runCheck("3.3.2.11", [&](auto& t) { ConsistencyCheck33211(t, warehouseCount); });
        runCheck("post-import: D_NEXT_O_ID", [&](auto& t) { PostImportCheckNextOrderId(t); });
        runCheck("post-import: W_YTD", [&](auto& t) { PostImportCheckWarehouseYtd(t); });
        runCheck("post-import: D_YTD", [&](auto& t) { PostImportCheckDistrictYtd(t); });
        runCheck("post-import: O_CARRIER_ID", [&](auto& t) { PostImportCheckNoCarriers(t); });
        runCheck("post-import: OL_DELIVERY_D", [&](auto& t) { PostImportCheckNoDeliveryDates(t); });
    }

    if (failedCount == 0) {
        std::cout << "Everything is good!" << std::endl;
    } else {
        throw std::runtime_error(fmt::format("{} checks failed", failedCount));
    }
}

} // namespace NTPCC
