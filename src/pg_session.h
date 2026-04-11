#pragma once

#include "query_result.h"
#include "future.h"
#include "thread_pool.h"

#include <pqxx/pqxx>

#include <memory>
#include <string>
#include <string_view>

namespace NTPCC {

class PgSession {
public:
    PgSession() = default;
    PgSession(std::unique_ptr<pqxx::connection> conn, IExecutor* executor);

    PgSession(PgSession&& other) noexcept;
    PgSession& operator=(PgSession&& other) noexcept;

    PgSession(const PgSession&) = delete;
    PgSession& operator=(const PgSession&) = delete;

    ~PgSession();

    // Executes a parameterized query. Lazily begins a transaction on first call.
    TFuture<QueryResult> ExecuteQuery(
        std::string_view sql, const pqxx::params& params = {});

    // Executes a query that doesn't return rows (INSERT/UPDATE/DELETE).
    TFuture<uint64_t> ExecuteModify(
        std::string_view sql, const pqxx::params& params = {});

    TFuture<void> Commit();
    TFuture<void> Rollback();

    // For non-transactional operations (DDL, etc.)
    TFuture<QueryResult> ExecuteNonTx(std::string_view sql);

    // For COPY-based bulk loading
    TFuture<void> ExecuteCopy(
        const std::string& tableName,
        const std::vector<std::string>& columns,
        std::function<void(pqxx::stream_to&)> writer);

    bool HasConnection() const { return conn_ != nullptr; }
    pqxx::connection& GetRawConnection() { return *conn_; }

    // Returns the underlying connection back (for pool return)
    std::unique_ptr<pqxx::connection> ReleaseConnection();

private:
    std::unique_ptr<pqxx::connection> conn_;
    std::unique_ptr<pqxx::work> txn_;
    IExecutor* executor_ = nullptr;
};

} // namespace NTPCC
