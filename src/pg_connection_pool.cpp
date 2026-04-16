#include "pg_connection_pool.h"
#include "log.h"

#include <fmt/format.h>

namespace NTPCC {

PgConnectionPool::PgConnectionPool(const std::string& connectionString,
                                   size_t poolSize,
                                   size_t ioThreads,
                                   const std::string& path)
    : connectionString_(connectionString)
    , poolSize_(poolSize)
    , executor_(std::make_unique<TThreadPool>(ioThreads))
{
    LOG_I("Creating connection pool: {} connections, {} IO threads", poolSize, ioThreads);

    for (size_t i = 0; i < poolSize; ++i) {
        auto conn = std::make_unique<pqxx::connection>(connectionString_);
        if (!path.empty()) {
            pqxx::nontransaction ntx(*conn);
            ntx.exec(fmt::format("SET search_path TO {}", conn->quote_name(path)));
        }
        connections_.push(std::move(conn));
    }

    LOG_I("Connection pool ready");
}

PgConnectionPool::~PgConnectionPool() {
    {
        std::lock_guard lock(mutex_);
        shutdown_ = true;
    }
    cv_.notify_all();

    executor_->Join();

    std::lock_guard lock(mutex_);
    while (!connections_.empty()) {
        connections_.pop();
    }
}

PgSession PgConnectionPool::AcquireSession() {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] { return !connections_.empty() || shutdown_; });

    if (shutdown_ && connections_.empty()) {
        throw std::runtime_error("Connection pool is shutting down");
    }

    auto conn = std::move(connections_.front());
    connections_.pop();
    checkedOut_.push_back(conn.get());
    return PgSession(std::move(conn), executor_.get(), sessionShutdownFlag_);
}

void PgConnectionPool::ReleaseSession(PgSession session) {
    auto conn = session.ReleaseConnection();
    if (!conn) return;

    {
        std::lock_guard lock(mutex_);
        std::erase(checkedOut_, conn.get());
        connections_.push(std::move(conn));
    }
    cv_.notify_one();
}

void PgConnectionPool::CancelAll() {
    sessionShutdownFlag_->store(true, std::memory_order_release);

    std::lock_guard lock(mutex_);
    for (auto* conn : checkedOut_) {
        try {
            conn->cancel_query();
        } catch (...) {
        }
    }
}

PgConnectionPool::SessionGuard PgConnectionPool::AcquireGuard() {
    return SessionGuard(*this, AcquireSession());
}

} // namespace NTPCC
