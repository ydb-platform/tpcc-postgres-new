#pragma once

#include "constants.h"
#include "pg_session.h"
#include "query_result.h"
#include "future.h"

#include <optional>
#include <string>

namespace NTPCC {

struct TCustomer {
    int c_id = C_INVALID_CUSTOMER_ID;
    std::string c_first;
    std::string c_middle;
    std::string c_last;
    std::string c_street_1;
    std::string c_street_2;
    std::string c_city;
    std::string c_state;
    std::string c_zip;
    std::string c_phone;
    std::string c_credit;
    std::string c_data;
    double c_credit_lim = 0;
    double c_discount = 0;
    double c_balance = 0;
    double c_ytd_payment = 0;
    int c_payment_cnt = 0;
    std::string c_since;
};

TCustomer ParseCustomerFromResult(QueryResult& result);

TFuture<QueryResult> GetCustomerById(
    PgSession& session,
    int warehouseID,
    int districtID,
    int customerID);

TFuture<QueryResult> GetCustomersByLastName(
    PgSession& session,
    int warehouseID,
    int districtID,
    const std::string& lastName);

std::optional<TCustomer> SelectCustomerFromResultSet(QueryResult& result);

} // namespace NTPCC
