#include "pg_connection_pool.h"
#include "log.h"

namespace NTPCC {

PgConnectionPool::PgConnectionPool(const std::string& connectionString,
                                   size_t poolSize,
                                   size_t ioThreads)
    : connectionString_(connectionString)
    , poolSize_(poolSize)
    , executor_(std::make_unique<TThreadPool>(ioThreads))
{
    LOG_I("Creating connection pool: {} connections, {} IO threads", poolSize, ioThreads);

    for (size_t i = 0; i < poolSize; ++i) {
        auto conn = std::make_unique<pqxx::connection>(connectionString_);
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
    return PgSession(std::move(conn), executor_.get());
}

void PgConnectionPool::ReleaseSession(PgSession session) {
    auto conn = session.ReleaseConnection();
    if (!conn) return;

    {
        std::lock_guard lock(mutex_);
        connections_.push(std::move(conn));
    }
    cv_.notify_one();
}

PgConnectionPool::SessionGuard PgConnectionPool::AcquireGuard() {
    return SessionGuard(*this, AcquireSession());
}

} // namespace NTPCC
