#include "common_queries.h"
#include "log.h"

#include <vector>

namespace NTPCC {

//-----------------------------------------------------------------------------

TCustomer ParseCustomerFromResult(QueryResult& result) {
    TCustomer customer;
    customer.c_first = result.GetString("c_first");
    customer.c_middle = result.GetString("c_middle");
    customer.c_street_1 = result.GetString("c_street_1");
    customer.c_street_2 = result.GetString("c_street_2");
    customer.c_city = result.GetString("c_city");
    customer.c_state = result.GetString("c_state");
    customer.c_zip = result.GetString("c_zip");
    customer.c_phone = result.GetString("c_phone");
    customer.c_credit = result.GetString("c_credit");
    customer.c_credit_lim = result.GetDouble("c_credit_lim");
    customer.c_discount = result.GetDouble("c_discount");
    customer.c_balance = result.GetDouble("c_balance");
    customer.c_ytd_payment = result.GetDouble("c_ytd_payment");
    customer.c_payment_cnt = result.GetInt32("c_payment_cnt");
    customer.c_since = result.GetString("c_since");
    return customer;
}

//-----------------------------------------------------------------------------

TFuture<QueryResult> GetCustomerById(
    PgSession& session,
    int warehouseID,
    int districtID,
    int customerID)
{
    static constexpr std::string_view sql =
        "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
        "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
        "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since "
        "FROM customer "
        "WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3";

    return session.ExecuteQuery(sql, pqxx::params{warehouseID, districtID, customerID});
}

//-----------------------------------------------------------------------------

TFuture<QueryResult> GetCustomersByLastName(
    PgSession& session,
    int warehouseID,
    int districtID,
    const std::string& lastName)
{
    static constexpr std::string_view sql =
        "SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
        "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
        "c_balance, c_ytd_payment, c_payment_cnt, c_since "
        "FROM customer "
        "WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 "
        "ORDER BY c_first";

    return session.ExecuteQuery(sql, pqxx::params{warehouseID, districtID, lastName});
}

//-----------------------------------------------------------------------------

std::optional<TCustomer> SelectCustomerFromResultSet(QueryResult& result) {
    std::vector<TCustomer> customers;

    while (result.TryNextRow()) {
        auto customer = ParseCustomerFromResult(result);
        auto optId = result.GetOptionalInt32("c_id");
        if (optId) {
            customer.c_id = *optId;
        }
        auto optLast = result.GetOptionalString("c_last");
        if (optLast) {
            customer.c_last = *optLast;
        }
        customers.push_back(std::move(customer));
    }

    if (customers.empty()) {
        return std::nullopt;
    }

    // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer (1-based)
    size_t index = customers.size() / 2;
    if (customers.size() % 2 == 0 && index > 0) {
        index--;
    }

    return customers[index];
}

} // namespace NTPCC
