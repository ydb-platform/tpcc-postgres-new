#pragma once

#include "future.h"

#include <coroutine>
#include <exception>

// Coroutine promise_type for TFuture<T>
namespace std {

template <typename T, typename... Args>
struct coroutine_traits<TFuture<T>, Args...> {
    struct promise_type {
        TPromise<T> promise;

        TFuture<T> get_return_object() {
            return promise.GetFuture();
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }

        void return_value(T value) {
            promise.SetValue(std::move(value));
        }

        void unhandled_exception() {
            promise.SetException(std::current_exception());
        }
    };
};

template <typename... Args>
struct coroutine_traits<TFuture<void>, Args...> {
    struct promise_type {
        TPromise<void> promise;

        TFuture<void> get_return_object() {
            return promise.GetFuture();
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }

        void return_void() {
            promise.SetValue();
        }

        void unhandled_exception() {
            promise.SetException(std::current_exception());
        }
    };
};

} // namespace std

// Awaiter for co_await on TFuture<T>.
// Resumes the coroutine inline when the future completes.
template <typename T>
struct TFutureAwaiter {
    TFuture<T> Future;

    bool await_ready() {
        return Future.IsReady();
    }

    void await_suspend(std::coroutine_handle<> handle) {
        Future.Subscribe([handle]() { handle.resume(); });
    }

    T await_resume() {
        return Future.Get();
    }
};

template <typename T>
TFutureAwaiter<T> operator co_await(TFuture<T>&& future) {
    return TFutureAwaiter<T>{std::move(future)};
}
