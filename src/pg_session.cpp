#include "pg_session.h"
#include "log.h"

namespace NTPCC {

PgSession::PgSession(std::unique_ptr<pqxx::connection> conn, IExecutor* executor)
    : conn_(std::move(conn))
    , executor_(executor)
{}

PgSession::PgSession(PgSession&& other) noexcept
    : conn_(std::move(other.conn_))
    , txn_(std::move(other.txn_))
    , executor_(other.executor_)
{
    other.executor_ = nullptr;
}

PgSession& PgSession::operator=(PgSession&& other) noexcept {
    if (this != &other) {
        conn_ = std::move(other.conn_);
        txn_ = std::move(other.txn_);
        executor_ = other.executor_;
        other.executor_ = nullptr;
    }
    return *this;
}

PgSession::~PgSession() {
    if (txn_) {
        try {
            txn_->abort();
        } catch (...) {
        }
        txn_.reset();
    }
}

TFuture<QueryResult> PgSession::ExecuteQuery(
    std::string_view sql, const pqxx::params& params)
{
    TPromise<QueryResult> promise;
    auto future = promise.GetFuture();
    std::string sqlCopy(sql);

    executor_->Submit([this, sqlCopy = std::move(sqlCopy), params,
                       p = std::move(promise)]() mutable {
        try {
            if (!txn_) {
                txn_ = std::make_unique<pqxx::work>(*conn_);
            }
            auto result = txn_->exec(sqlCopy, params);
            p.SetValue(QueryResult(std::move(result)));
        } catch (...) {
            p.SetException(std::current_exception());
        }
    });

    return future;
}

TFuture<uint64_t> PgSession::ExecuteModify(
    std::string_view sql, const pqxx::params& params)
{
    TPromise<uint64_t> promise;
    auto future = promise.GetFuture();
    std::string sqlCopy(sql);

    executor_->Submit([this, sqlCopy = std::move(sqlCopy), params,
                       p = std::move(promise)]() mutable {
        try {
            if (!txn_) {
                txn_ = std::make_unique<pqxx::work>(*conn_);
            }
            auto result = txn_->exec(sqlCopy, params);
            p.SetValue(result.affected_rows());
        } catch (...) {
            p.SetException(std::current_exception());
        }
    });

    return future;
}

TFuture<void> PgSession::Commit() {
    TPromise<void> promise;
    auto future = promise.GetFuture();

    executor_->Submit([this, p = std::move(promise)]() mutable {
        try {
            if (txn_) {
                txn_->commit();
                txn_.reset();
            }
            p.SetValue();
        } catch (...) {
            txn_.reset();
            p.SetException(std::current_exception());
        }
    });

    return future;
}

TFuture<void> PgSession::Rollback() {
    TPromise<void> promise;
    auto future = promise.GetFuture();

    executor_->Submit([this, p = std::move(promise)]() mutable {
        try {
            if (txn_) {
                txn_->abort();
                txn_.reset();
            }
            p.SetValue();
        } catch (...) {
            txn_.reset();
            p.SetException(std::current_exception());
        }
    });

    return future;
}

TFuture<QueryResult> PgSession::ExecuteNonTx(std::string_view sql) {
    TPromise<QueryResult> promise;
    auto future = promise.GetFuture();
    std::string sqlCopy(sql);

    executor_->Submit([this, sqlCopy = std::move(sqlCopy),
                       p = std::move(promise)]() mutable {
        try {
            pqxx::nontransaction ntx(*conn_);
            auto result = ntx.exec(sqlCopy);
            p.SetValue(QueryResult(std::move(result)));
        } catch (...) {
            p.SetException(std::current_exception());
        }
    });

    return future;
}

TFuture<void> PgSession::ExecuteCopy(
    const std::string& tableName,
    const std::vector<std::string>& columns,
    std::function<void(pqxx::stream_to&)> writer)
{
    TPromise<void> promise;
    auto future = promise.GetFuture();

    executor_->Submit([this, tableName, columns, writer = std::move(writer),
                       p = std::move(promise)]() mutable {
        try {
            if (!txn_) {
                txn_ = std::make_unique<pqxx::work>(*conn_);
            }
            auto stream = pqxx::stream_to::raw_table(
                *txn_, conn_->quote_table(tableName), conn_->quote_columns(columns));
            writer(stream);
            stream.complete();
            p.SetValue();
        } catch (...) {
            p.SetException(std::current_exception());
        }
    });

    return future;
}

std::unique_ptr<pqxx::connection> PgSession::ReleaseConnection() {
    if (txn_) {
        try { txn_->abort(); } catch (...) {}
        txn_.reset();
    }
    return std::move(conn_);
}

} // namespace NTPCC
