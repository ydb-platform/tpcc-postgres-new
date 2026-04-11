#pragma once

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <vector>
#include <memory>

template <typename T>
class TSharedState {
public:
    using Callback = std::function<void()>;

    template <typename TT>
    void SetValue(TT&& value) {
        std::vector<Callback> toCall;
        {
            std::lock_guard lock(Mutex);
            Value = std::forward<TT>(value);
            IsSet = true;
            toCall.swap(Callbacks);
            CV.notify_all();
        }
        for (auto& cb : toCall) {
            cb();
        }
    }

    void SetException(std::exception_ptr ex) {
        std::vector<Callback> toCall;
        {
            std::lock_guard lock(Mutex);
            Exception = std::move(ex);
            IsSet = true;
            toCall.swap(Callbacks);
            CV.notify_all();
        }
        for (auto& cb : toCall) {
            cb();
        }
    }

    void AddCallback(Callback cb) {
        bool ready = false;
        {
            std::lock_guard lock(Mutex);
            if (IsSet) {
                ready = true;
            } else {
                Callbacks.push_back(std::move(cb));
            }
        }
        if (ready) {
            cb();
        }
    }

    bool Ready() const {
        std::lock_guard lock(Mutex);
        return IsSet;
    }

    T Get() {
        std::unique_lock lock(Mutex);
        CV.wait(lock, [this] { return IsSet; });
        if (Exception) {
            std::rethrow_exception(Exception);
        }
        return std::move(*Value);
    }

private:
    mutable std::mutex Mutex;
    std::condition_variable CV;
    std::optional<T> Value;
    std::exception_ptr Exception;
    bool IsSet = false;
    std::vector<Callback> Callbacks;
};

template <>
class TSharedState<void> {
public:
    using Callback = std::function<void()>;

    void SetValue() {
        std::vector<Callback> toCall;
        {
            std::lock_guard lock(Mutex);
            IsSet = true;
            toCall.swap(Callbacks);
            CV.notify_all();
        }
        for (auto& cb : toCall) {
            cb();
        }
    }

    void SetException(std::exception_ptr ex) {
        std::vector<Callback> toCall;
        {
            std::lock_guard lock(Mutex);
            Exception = std::move(ex);
            IsSet = true;
            toCall.swap(Callbacks);
            CV.notify_all();
        }
        for (auto& cb : toCall) {
            cb();
        }
    }

    void AddCallback(Callback cb) {
        bool ready = false;
        {
            std::lock_guard lock(Mutex);
            if (IsSet) {
                ready = true;
            } else {
                Callbacks.push_back(std::move(cb));
            }
        }
        if (ready) {
            cb();
        }
    }

    bool Ready() const {
        std::lock_guard lock(Mutex);
        return IsSet;
    }

    void Get() {
        std::unique_lock lock(Mutex);
        CV.wait(lock, [this] { return IsSet; });
        if (Exception) {
            std::rethrow_exception(Exception);
        }
    }

private:
    mutable std::mutex Mutex;
    std::condition_variable CV;
    std::exception_ptr Exception;
    bool IsSet = false;
    std::vector<Callback> Callbacks;
};

template<typename T>
class TFuture {
public:
    using Callback = typename TSharedState<T>::Callback;

    TFuture(std::shared_ptr<TSharedState<T>> state)
        : State(std::move(state))
    {}

    void Subscribe(Callback callback) {
        if (State) {
            State->AddCallback(std::move(callback));
        }
    }

    bool IsReady() const {
        return State && State->Ready();
    }

    T Get() {
        return State->Get();
    }

private:
    std::shared_ptr<TSharedState<T>> State;
};

template<typename T>
class TPromise {
public:
    TPromise()
        : State(std::make_shared<TSharedState<T>>())
    {}

    TFuture<T> GetFuture() {
        return TFuture<T>(State);
    }

    template<typename TT>
    void SetValue(TT&& value) {
        State->SetValue(std::forward<TT>(value));
    }

    void SetException(std::exception_ptr ex) {
        State->SetException(std::move(ex));
    }

private:
    std::shared_ptr<TSharedState<T>> State;
};

template<>
class TFuture<void> {
public:
    using Callback = typename TSharedState<void>::Callback;

    TFuture(std::shared_ptr<TSharedState<void>> state)
        : State(std::move(state))
    {}

    void Subscribe(Callback callback) {
        if (State) {
            State->AddCallback(std::move(callback));
        }
    }

    bool IsReady() const {
        return State && State->Ready();
    }

    void Get() {
        if (State) {
            State->Get();
        }
    }

private:
    std::shared_ptr<TSharedState<void>> State;
};

template<>
class TPromise<void> {
public:
    TPromise()
        : State(std::make_shared<TSharedState<void>>())
    {}

    TFuture<void> GetFuture() {
        return TFuture<void>(State);
    }

    void SetValue() {
        State->SetValue();
    }

    void SetException(std::exception_ptr ex) {
        State->SetException(std::move(ex));
    }

private:
    std::shared_ptr<TSharedState<void>> State;
};
