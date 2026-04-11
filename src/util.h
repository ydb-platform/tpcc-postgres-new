#pragma once

#include "constants.h"

#include <random>
#include <string>
#include <stop_token>
#include <atomic>

namespace NTPCC {

//-----------------------------------------------------------------------------

// [from; to] inclusive range
inline size_t RandomNumber(size_t from, size_t to) {
    thread_local std::mt19937 rng(std::random_device{}());
    return std::uniform_int_distribution<size_t>(from, to)(rng);
}

// Non-uniform random number generation as per TPC-C spec
inline int NonUniformRandom(int A, int C, int min, int max) {
    int randomNum = RandomNumber(0, A);
    int randomNum2 = RandomNumber(min, max);
    return (((randomNum | randomNum2) + C) % (max - min + 1)) + min;
}

inline int GetRandomCustomerID() {
    return NonUniformRandom(1023, C_ID_C, 1, CUSTOMERS_PER_DISTRICT);
}

inline int GetRandomItemID() {
    return NonUniformRandom(8191, OL_I_ID_C, 1, ITEM_COUNT);
}

constexpr const char* const NameTokens[] = {"BAR", "OUGHT", "ABLE", "PRI",
        "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

inline std::string GetLastName(int num) {
    std::string result;
    result += NameTokens[num / 100];
    result += NameTokens[(num / 10) % 10];
    result += NameTokens[num % 10];
    return result;
}

inline std::string GetNonUniformRandomLastNameForRun() {
    return GetLastName(NonUniformRandom(255, C_LAST_RUN_C, 0, 999));
}

inline std::string GetNonUniformRandomLastNameForLoad() {
    return GetLastName(NonUniformRandom(255, C_LAST_LOAD_C, 0, 999));
}

//-----------------------------------------------------------------------------

std::string GetFormattedSize(size_t size);

//-----------------------------------------------------------------------------

std::stop_source& GetGlobalInterruptSource();
std::atomic<bool>& GetGlobalErrorVariable();

inline void RequestStopWithError() {
    GetGlobalErrorVariable().store(true);
    GetGlobalInterruptSource().request_stop();
}

//-----------------------------------------------------------------------------

size_t NumberOfMyCpus();

} // namespace NTPCC
