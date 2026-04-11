#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class IExecutor {
public:
    virtual ~IExecutor() = default;
    virtual void Submit(std::function<void()> task) = 0;
};

class TThreadPool : public IExecutor {
public:
    explicit TThreadPool(size_t threadCount) {
        for (size_t i = 0; i < threadCount; ++i) {
            Threads.emplace_back([this] { WorkerLoop(); });
        }
    }

    ~TThreadPool() override {
        Join();
    }

    TThreadPool(const TThreadPool&) = delete;
    TThreadPool& operator=(const TThreadPool&) = delete;

    void Submit(std::function<void()> task) override {
        {
            std::lock_guard lock(Mutex);
            Tasks.push(std::move(task));
        }
        CV.notify_one();
    }

    void Join() {
        {
            std::lock_guard lock(Mutex);
            if (Stopped) return;
            Stopped = true;
        }
        CV.notify_all();
        for (auto& t : Threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

private:
    void WorkerLoop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock lock(Mutex);
                CV.wait(lock, [this] { return Stopped || !Tasks.empty(); });
                if (Stopped && Tasks.empty()) return;
                task = std::move(Tasks.front());
                Tasks.pop();
            }
            task();
        }
    }

    std::vector<std::thread> Threads;
    std::queue<std::function<void()>> Tasks;
    std::mutex Mutex;
    std::condition_variable CV;
    bool Stopped = false;
};
