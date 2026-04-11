cmake_minimum_required(VERSION 3.20)
project(tpcc-postgres LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Require clang
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(WARNING "This project is designed for Clang 16+. Current compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# Dependencies

# libpqxx (7.x+, built from source via CMake)
find_package(libpqxx REQUIRED)

find_package(gflags REQUIRED)

# spdlog
find_package(spdlog REQUIRED)

# fmt
find_package(fmt REQUIRED)

# ftxui (optional TUI)
find_package(ftxui QUIET)
if(ftxui_FOUND)
    set(TPCC_HAS_TUI ON)
    message(STATUS "ftxui found: TUI support enabled")
else()
    set(TPCC_HAS_TUI OFF)
    message(STATUS "ftxui not found: TUI support disabled")
endif()

# Sources
set(TPCC_SOURCES
    src/constants.h
    src/util.h
    src/util.cpp
    src/log.h
    src/histogram.h
    src/histogram.cpp
    src/future.h
    src/spinlock.h
    src/thread_pool.h
    src/coro_traits.h
    src/task_queue.h
    src/task_queue.cpp
    src/timer_queue.h
    src/spsc_circular_queue.h
    src/pg_session.h
    src/pg_session.cpp
    src/query_result.h
    src/pg_connection_pool.h
    src/pg_connection_pool.cpp
    src/init.h
    src/init.cpp
    src/import.h
    src/import.cpp
    src/clean.h
    src/clean.cpp
    src/check.h
    src/check.cpp
    src/transactions.h
    src/common_queries.h
    src/common_queries.cpp
    src/transaction_neworder.cpp
    src/transaction_delivery.cpp
    src/transaction_payment.cpp
    src/transaction_orderstatus.cpp
    src/transaction_stocklevel.cpp
    src/transaction_simulation.cpp
    src/terminal.h
    src/terminal.cpp
    src/runner.h
    src/runner.cpp
    src/runner_display_data.h
    src/path_checker.h
    src/path_checker.cpp
    src/log_backend.h
    src/log_backend.cpp
    src/main.cpp
)

if(TPCC_HAS_TUI)
    list(APPEND TPCC_SOURCES
        src/tui_base.h
        src/tui_base.cpp
        src/runner_tui.h
        src/runner_tui.cpp
        src/import_tui.h
        src/import_tui.cpp
        src/import_display_data.h
        src/scroller.h
        src/scroller.cpp
        src/logs_scroller.h
        src/logs_scroller.cpp
    )
endif()

add_executable(tpcc ${TPCC_SOURCES})

target_include_directories(tpcc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_link_libraries(tpcc PRIVATE
    libpqxx::pqxx
    gflags
    spdlog::spdlog
    fmt::fmt
)

if(TPCC_HAS_TUI)
    target_link_libraries(tpcc PRIVATE
        ftxui::screen
        ftxui::dom
        ftxui::component
    )
    target_compile_definitions(tpcc PRIVATE TPCC_HAS_TUI=1)
endif()

target_compile_options(tpcc PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

# GCC needs -fcoroutines for C++20 coroutines, Clang does not
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(tpcc PRIVATE -fcoroutines)
endif()

# --- coroutines_vs_threads benchmark ---
add_executable(coroutines_vs_threads
    src/coroutines_vs_threads/main.cpp
    src/coroutines_vs_threads/workload.cpp
    src/histogram.cpp
    src/task_queue.cpp
    src/log_backend.cpp
    src/util.cpp
)

target_include_directories(coroutines_vs_threads PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/coroutines_vs_threads
)

target_compile_definitions(coroutines_vs_threads PRIVATE TPCC_NO_SPDLOG)

target_link_libraries(coroutines_vs_threads PRIVATE
    gflags
    fmt::fmt
)

target_compile_options(coroutines_vs_threads PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(coroutines_vs_threads PRIVATE -fcoroutines)
endif()

# --- Tests ---
find_package(GTest QUIET)
if(GTest_FOUND)
    enable_testing()

    set(TPCC_TEST_LIB_SOURCES
        src/histogram.cpp
        src/task_queue.cpp
        src/log_backend.cpp
        src/util.cpp
    )

    add_executable(tpcc_tests
        src/ut/spsc_circular_queue_ut.cpp
        src/ut/histogram_ut.cpp
        src/ut/timer_queue_ut.cpp
        src/ut/task_queue_ut.cpp
        src/ut/log_capture_ut.cpp
        ${TPCC_TEST_LIB_SOURCES}
    )

    target_include_directories(tpcc_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_compile_definitions(tpcc_tests PRIVATE TPCC_NO_SPDLOG)

    target_link_libraries(tpcc_tests PRIVATE
        GTest::gtest
        GTest::gtest_main
        fmt::fmt
    )

    target_compile_options(tpcc_tests PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )

    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(tpcc_tests PRIVATE -fcoroutines)
    endif()

    add_test(NAME tpcc_tests COMMAND tpcc_tests)
    message(STATUS "GTest found: test target 'tpcc_tests' enabled")
else()
    message(STATUS "GTest not found: tests disabled")
endif()
