#include "import.h"

#include "constants.h"
#include "init.h"
#include "log.h"
#include "util.h"

#ifdef TPCC_HAS_TUI
#include "import_tui.h"
#include "log_backend.h"
#endif

#include <pqxx/pqxx>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iomanip>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

namespace NTPCC {

namespace {

using Clock = std::chrono::steady_clock;

void SetSearchPath(pqxx::connection& conn, const std::string& path) {
    if (!path.empty()) {
        pqxx::nontransaction ntx(conn);
        ntx.exec(fmt::format("SET search_path TO {}", conn.quote_name(path)));
    }
}

// Rough per-row byte estimates for progress tracking (not exact, but close enough for TUI)
constexpr size_t BYTES_PER_ITEM = 40;
constexpr size_t BYTES_PER_STOCK = 280;
constexpr size_t BYTES_PER_CUSTOMER = 600;
constexpr size_t BYTES_PER_HISTORY = 46;
constexpr size_t BYTES_PER_ORDER = 48;
constexpr size_t BYTES_PER_NEW_ORDER = 12;
constexpr size_t BYTES_PER_ORDER_LINE = 54;
constexpr size_t AVG_ORDER_LINES_PER_ORDER = 10;
constexpr size_t NEW_ORDERS_PER_DISTRICT = CUSTOMERS_PER_DISTRICT - FIRST_UNPROCESSED_O_ID + 1;

size_t EstimateSharedDataSize() {
    return ITEM_COUNT * BYTES_PER_ITEM;
}

size_t EstimatePerWarehouseDataSize() {
    size_t stock = ITEM_COUNT * BYTES_PER_STOCK;
    size_t perDistrict =
        CUSTOMERS_PER_DISTRICT * BYTES_PER_CUSTOMER +
        CUSTOMERS_PER_DISTRICT * BYTES_PER_HISTORY +
        CUSTOMERS_PER_DISTRICT * BYTES_PER_ORDER +
        NEW_ORDERS_PER_DISTRICT * BYTES_PER_NEW_ORDER +
        CUSTOMERS_PER_DISTRICT * AVG_ORDER_LINES_PER_ORDER * BYTES_PER_ORDER_LINE;
    return stock + DISTRICT_COUNT * perDistrict;
}

//-----------------------------------------------------------------------------

std::string RandomStringBenchbase(int strLen, char baseChar = 'a') {
    if (strLen > 1) {
        int actualLength = strLen - 1;
        std::string result;
        result.reserve(actualLength);
        for (int i = 0; i < actualLength; ++i) {
            result += static_cast<char>(baseChar + RandomNumber(0, 25));
        }
        return result;
    }
    return "";
}

std::string RandomAlphaString(int minLength, int maxLength) {
    int length = RandomNumber(minLength, maxLength);
    return RandomStringBenchbase(length, 'a');
}

std::string RandomUpperAlphaString(int minLength, int maxLength) {
    int length = RandomNumber(minLength, maxLength);
    return RandomStringBenchbase(length, 'A');
}

std::string RandomNumericString(int length) {
    std::string result;
    result.reserve(length);
    for (int i = 0; i < length; ++i) {
        result += static_cast<char>('0' + RandomNumber(0, 9));
    }
    return result;
}

size_t HashCustomer(int warehouseId, int districtId, int customerId) {
    return std::hash<int>{}(warehouseId) ^
           (std::hash<int>{}(districtId) << 1) ^
           (std::hash<int>{}(customerId) << 2);
}

int GetRandomCount(int warehouseId, int customerId, int districtId) {
    size_t seed = HashCustomer(warehouseId, districtId, customerId);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist(5, 15);
    return dist(rng);
}

std::string CurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
    gmtime_r(&time_t_now, &tm_buf);
    std::ostringstream ss;
    ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

//-----------------------------------------------------------------------------

void LoadItems(pqxx::connection& conn) {
    LOG_I("Loading {} items...", ITEM_COUNT);

    pqxx::work txn(conn);
    auto stream = pqxx::stream_to::table(txn, {"item"},
        {"i_id", "i_name", "i_price", "i_data", "i_im_id"});

    for (int i = 1; i <= ITEM_COUNT; ++i) {
        std::string data;
        int randPct = RandomNumber(1, 100);
        int len = RandomNumber(26, 50);
        if (randPct > 10) {
            data = RandomStringBenchbase(len);
        } else {
            int startOrig = RandomNumber(2, len - 8);
            data = RandomStringBenchbase(startOrig) + "ORIGINAL" + RandomStringBenchbase(len - startOrig - 8);
        }

        stream.write_values(
            i,
            RandomAlphaString(14, 24),
            RandomNumber(100, 10000) / 100.0,
            data,
            static_cast<int>(RandomNumber(1, 10000))
        );
    }

    stream.complete();
    txn.commit();
    LOG_I("Items loaded");
}

//-----------------------------------------------------------------------------

void LoadWarehouses(pqxx::connection& conn, int startId, int lastId) {
    LOG_I("Loading warehouses {} to {}", startId, lastId);

    pqxx::work txn(conn);
    auto stream = pqxx::stream_to::table(txn, {"warehouse"},
        {"w_id", "w_ytd", "w_tax", "w_name", "w_street_1", "w_street_2",
         "w_city", "w_state", "w_zip"});

    for (int wh = startId; wh <= lastId; ++wh) {
        stream.write_values(
            wh,
            DISTRICT_INITIAL_YTD * DISTRICT_COUNT,
            RandomNumber(0, 2000) / 10000.0,
            RandomAlphaString(6, 10),
            RandomAlphaString(10, 20),
            RandomAlphaString(10, 20),
            RandomAlphaString(10, 20),
            RandomUpperAlphaString(3, 3),
            std::string("123456789")
        );
    }

    stream.complete();
    txn.commit();
}

//-----------------------------------------------------------------------------

void LoadDistricts(pqxx::connection& conn, int startId, int lastId) {
    LOG_I("Loading districts for warehouses {} to {}", startId, lastId);

    pqxx::work txn(conn);
    auto stream = pqxx::stream_to::table(txn, {"district"},
        {"d_w_id", "d_id", "d_ytd", "d_tax", "d_next_o_id", "d_name",
         "d_street_1", "d_street_2", "d_city", "d_state", "d_zip"});

    for (int wh = startId; wh <= lastId; ++wh) {
        for (int d = DISTRICT_LOW_ID; d <= DISTRICT_HIGH_ID; ++d) {
            stream.write_values(
                wh,
                d,
                DISTRICT_INITIAL_YTD,
                RandomNumber(0, 2000) / 10000.0,
                CUSTOMERS_PER_DISTRICT + 1,
                RandomAlphaString(6, 10),
                RandomAlphaString(10, 20),
                RandomAlphaString(10, 20),
                RandomAlphaString(10, 20),
                RandomUpperAlphaString(3, 3),
                std::string("123456789")
            );
        }
    }

    stream.complete();
    txn.commit();
}

//-----------------------------------------------------------------------------

void LoadStock(pqxx::connection& conn, int wh) {
    LOG_D("Loading stock for warehouse {}", wh);

    pqxx::work txn(conn);
    auto stream = pqxx::stream_to::table(txn, {"stock"},
        {"s_w_id", "s_i_id", "s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt",
         "s_data", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
         "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10"});

    for (int itemId = 1; itemId <= ITEM_COUNT; ++itemId) {
        std::string data;
        int randPct = RandomNumber(1, 100);
        int len = RandomNumber(26, 50);
        if (randPct > 10) {
            data = RandomStringBenchbase(len);
        } else {
            int startOrig = RandomNumber(2, len - 8);
            data = RandomStringBenchbase(startOrig) + "ORIGINAL" + RandomStringBenchbase(len - startOrig - 8);
        }

        stream.write_values(
            wh,
            itemId,
            static_cast<int>(RandomNumber(10, 100)),
            0.0,
            0,
            0,
            data,
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24),
            RandomStringBenchbase(24)
        );
    }

    stream.complete();
    txn.commit();
}

//-----------------------------------------------------------------------------

void LoadCustomers(pqxx::connection& conn, int wh, int district) {
    LOG_D("Loading customers for warehouse {} district {}", wh, district);

    pqxx::work txn(conn);
    auto stream = pqxx::stream_to::table(txn, {"customer"},
        {"c_w_id", "c_d_id", "c_id", "c_discount", "c_credit", "c_last", "c_first",
         "c_credit_lim", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt",
         "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone",
         "c_since", "c_middle", "c_data"});

    auto ts = CurrentTimestamp();

    for (int cid = C_FIRST_CUSTOMER_ID; cid <= CUSTOMERS_PER_DISTRICT; ++cid) {
        std::string last;
        if (cid <= 1000) {
            last = GetLastName(cid - 1);
        } else {
            last = GetNonUniformRandomLastNameForLoad();
        }

        std::string credit = RandomNumber(1, 100) <= 10 ? "BC" : "GC";

        stream.write_values(
            wh,
            district,
            cid,
            RandomNumber(1, 5000) / 10000.0,
            credit,
            last,
            RandomAlphaString(8, 16),
            50000.00,
            -10.00,
            10.00,
            1,
            0,
            RandomAlphaString(10, 20),
            RandomAlphaString(10, 20),
            RandomAlphaString(10, 20),
            RandomUpperAlphaString(3, 3),
            RandomNumericString(4) + "11111",
            RandomNumericString(16),
            ts,
            std::string("OE"),
            RandomAlphaString(300, 500)
        );
    }

    stream.complete();
    txn.commit();
}

//-----------------------------------------------------------------------------

void LoadHistory(pqxx::connection& conn, int wh, int district) {
    LOG_D("Loading history for warehouse {} district {}", wh, district);

    pqxx::work txn(conn);
    auto stream = pqxx::stream_to::table(txn, {"history"},
        {"h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"});

    auto ts = CurrentTimestamp();

    for (int cid = C_FIRST_CUSTOMER_ID; cid <= CUSTOMERS_PER_DISTRICT; ++cid) {
        stream.write_values(
            cid,
            district,
            wh,
            district,
            wh,
            ts,
            10.00,
            RandomAlphaString(10, 24)
        );
    }

    stream.complete();
    txn.commit();
}

//-----------------------------------------------------------------------------

void LoadOrders(pqxx::connection& conn, int wh, int district) {
    LOG_D("Loading orders for warehouse {} district {}", wh, district);

    // Generate shuffled customer IDs (TPC-C 4.3.3.1)
    std::vector<int> customerIds;
    customerIds.reserve(CUSTOMERS_PER_DISTRICT);
    for (int i = 1; i <= CUSTOMERS_PER_DISTRICT; ++i) {
        customerIds.push_back(i);
    }
    thread_local std::mt19937 rng(std::random_device{}());
    std::shuffle(customerIds.begin(), customerIds.end(), rng);

    auto ts = CurrentTimestamp();

    {
        pqxx::work txn(conn);
        auto stream = pqxx::stream_to::table(txn, {"oorder"},
            {"o_w_id", "o_d_id", "o_id", "o_c_id", "o_carrier_id", "o_ol_cnt",
             "o_all_local", "o_entry_d"});

        for (int oid = 1; oid <= CUSTOMERS_PER_DISTRICT; ++oid) {
            int cid = customerIds[oid - 1];
            std::optional<int> carrierId;
            if (oid < FIRST_UNPROCESSED_O_ID) {
                carrierId = static_cast<int>(RandomNumber(1, 10));
            }
            int olCnt = GetRandomCount(wh, oid, district);

            stream.write_values(
                wh, district, oid, cid, carrierId, olCnt, 1, ts
            );
        }

        stream.complete();
        txn.commit();
    }

    // New orders
    {
        pqxx::work txn(conn);
        auto stream = pqxx::stream_to::table(txn, {"new_order"},
            {"no_w_id", "no_d_id", "no_o_id"});

        for (int oid = FIRST_UNPROCESSED_O_ID; oid <= CUSTOMERS_PER_DISTRICT; ++oid) {
            stream.write_values(wh, district, oid);
        }

        stream.complete();
        txn.commit();
    }

    // Order lines
    {
        pqxx::work txn(conn);
        auto stream = pqxx::stream_to::table(txn, {"order_line"},
            {"ol_w_id", "ol_d_id", "ol_o_id", "ol_number", "ol_i_id", "ol_delivery_d",
             "ol_amount", "ol_supply_w_id", "ol_quantity", "ol_dist_info"});

        for (int oid = 1; oid <= CUSTOMERS_PER_DISTRICT; ++oid) {
            int olCnt = GetRandomCount(wh, oid, district);
            for (int lineNum = 1; lineNum <= olCnt; ++lineNum) {
                int itemId = static_cast<int>(RandomNumber(1, ITEM_COUNT));

                std::optional<std::string> deliveryDate;
                double amount;
                if (oid < FIRST_UNPROCESSED_O_ID) {
                    deliveryDate = ts;
                    amount = 0.0;
                } else {
                    amount = RandomNumber(1, 999999) / 100.0;
                }

                stream.write_values(
                    wh, district, oid, lineNum, itemId, deliveryDate,
                    amount, wh, 5.0, RandomStringBenchbase(24)
                );
            }
        }

        stream.complete();
        txn.commit();
    }
}

//-----------------------------------------------------------------------------

void LoadWarehouse(pqxx::connection& conn, int wh, TImportState& state) {
    if (state.StopToken.stop_requested()) return;

    LoadStock(conn, wh);
    state.DataSizeLoaded.fetch_add(
        ITEM_COUNT * BYTES_PER_STOCK, std::memory_order_relaxed);

    for (int d = DISTRICT_LOW_ID; d <= DISTRICT_HIGH_ID; ++d) {
        if (state.StopToken.stop_requested()) return;

        LoadCustomers(conn, wh, d);
        LoadHistory(conn, wh, d);
        LoadOrders(conn, wh, d);

        size_t districtBytes =
            CUSTOMERS_PER_DISTRICT * BYTES_PER_CUSTOMER +
            CUSTOMERS_PER_DISTRICT * BYTES_PER_HISTORY +
            CUSTOMERS_PER_DISTRICT * BYTES_PER_ORDER +
            NEW_ORDERS_PER_DISTRICT * BYTES_PER_NEW_ORDER +
            CUSTOMERS_PER_DISTRICT * AVG_ORDER_LINES_PER_ORDER * BYTES_PER_ORDER_LINE;
        state.DataSizeLoaded.fetch_add(districtBytes, std::memory_order_relaxed);
    }

    state.WarehousesLoaded.fetch_add(1, std::memory_order_relaxed);
}

} // anonymous

//-----------------------------------------------------------------------------

void ImportSync(const TImportConfig& config) {
    if (config.WarehouseCount == 0) {
        LOG_E("Specified zero warehouses");
        throw std::runtime_error("Warehouse count must be greater than zero");
    }

    size_t threadCount = config.LoadThreadCount;
    if (threadCount == 0) {
        threadCount = std::min(config.WarehouseCount, NumberOfMyCpus());
    }
    threadCount = std::max(threadCount, size_t(1));
    threadCount = std::min(threadCount, config.WarehouseCount);

    LOG_I("Starting TPC-C data import for {} warehouses using {} threads",
          config.WarehouseCount, threadCount);

    auto startTime = Clock::now();

    TImportState state{GetGlobalInterruptSource().get_token()};
    state.ApproximateDataSize =
        EstimateSharedDataSize() + config.WarehouseCount * EstimatePerWarehouseDataSize();

    // Load small tables on the main connection
    {
        pqxx::connection conn(config.ConnectionString);
        SetSearchPath(conn, config.Path);
        LoadItems(conn);
        state.DataSizeLoaded.fetch_add(EstimateSharedDataSize(), std::memory_order_relaxed);
        LoadWarehouses(conn, 1, static_cast<int>(config.WarehouseCount));
        LoadDistricts(conn, 1, static_cast<int>(config.WarehouseCount));
    }

    // Load per-warehouse data in parallel
    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (size_t tid = 0; tid < threadCount; ++tid) {
        int whStart = static_cast<int>(tid * config.WarehouseCount / threadCount + 1);
        int whEnd = static_cast<int>((tid + 1) * config.WarehouseCount / threadCount);

        threads.emplace_back([&config, &state, whStart, whEnd]() {
            try {
                pqxx::connection conn(config.ConnectionString);
                SetSearchPath(conn, config.Path);
                for (int wh = whStart; wh <= whEnd; ++wh) {
                    if (state.StopToken.stop_requested()) return;
                    LoadWarehouse(conn, wh, state);

                    LOG_I("Warehouse {} loaded ({}/{})",
                          wh, state.WarehousesLoaded.load(), config.WarehouseCount);
                }
            } catch (const std::exception& ex) {
                LOG_E("Import thread failed: {}", ex.what());
                RequestStopWithError();
            }
        });
    }

#ifdef TPCC_HAS_TUI
    TLogCapture logCapture(TUI_LOG_LINES);
    std::unique_ptr<TImportTui> tui;
    if (config.UseTui) {
        StartLogCapture(logCapture);
        TImportDisplayData initData(state);
        tui = std::make_unique<TImportTui>(
            logCapture, config.WarehouseCount, threadCount, initData);
    }
#endif

    {
        size_t prevLoaded = state.DataSizeLoaded.load(std::memory_order_relaxed);
        auto prevTime = Clock::now();

        while (state.WarehousesLoaded.load(std::memory_order_relaxed) < config.WarehouseCount
               && !state.StopToken.stop_requested())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            auto now = Clock::now();
            auto elapsed = std::chrono::duration<double>(now - startTime);
            size_t loaded = state.DataSizeLoaded.load(std::memory_order_relaxed);

#ifdef TPCC_HAS_TUI
            if (tui) {
                TImportDisplayData data(state);
                auto& s = data.StatusData;
                s.CurrentDataSizeLoaded = loaded;
                s.PercentLoaded = state.ApproximateDataSize > 0
                    ? 100.0 * loaded / state.ApproximateDataSize : 0;

                auto sincePrev = std::chrono::duration<double>(now - prevTime);
                if (sincePrev.count() > 0.01) {
                    s.InstantSpeedMiBs =
                        (loaded - prevLoaded) / (1024.0 * 1024.0) / sincePrev.count();
                }
                if (elapsed.count() > 0.01) {
                    s.AvgSpeedMiBs = loaded / (1024.0 * 1024.0) / elapsed.count();
                }

                int totalSec = static_cast<int>(elapsed.count());
                s.ElapsedMinutes = totalSec / 60;
                s.ElapsedSeconds = totalSec % 60;

                if (s.AvgSpeedMiBs > 0.01 && state.ApproximateDataSize > loaded) {
                    double remainSec =
                        (state.ApproximateDataSize - loaded) / (s.AvgSpeedMiBs * 1024 * 1024);
                    int etaSec = static_cast<int>(remainSec);
                    s.EstimatedTimeLeftMinutes = etaSec / 60;
                    s.EstimatedTimeLeftSeconds = etaSec % 60;
                }

                tui->Update(data);
                prevLoaded = loaded;
                prevTime = now;
            }
#endif
        }
    }

    bool wasInterrupted = GetGlobalInterruptSource().stop_requested();

#ifdef TPCC_HAS_TUI
    tui.reset();
    StopLogCapture();
#endif

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    if (wasInterrupted) {
        throw std::runtime_error("Import was interrupted or failed. See logs.");
    }

    LOG_I("Running ANALYZE on TPC-C tables...");
    {
        pqxx::connection conn(config.ConnectionString);
        SetSearchPath(conn, config.Path);
        pqxx::nontransaction ntx(conn);
        for (const auto* table : TPCC_TABLES) {
            ntx.exec(fmt::format("ANALYZE {}", table));
        }
    }

    CreateIndexes(config.ConnectionString, config.Path);

    auto elapsed = Clock::now() - startTime;
    auto seconds = std::chrono::duration<double>(elapsed).count();
    LOG_I("TPC-C data import completed successfully in {:.1f}s", seconds);
}

} // namespace NTPCC
