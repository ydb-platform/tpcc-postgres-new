#include "util.h"

#include <sstream>
#include <iomanip>

#ifdef __linux__
#include <sched.h>
#include <thread>
#endif

namespace NTPCC {

//-----------------------------------------------------------------------------

std::string GetFormattedSize(size_t size) {
    constexpr size_t TiB = 1024ULL * 1024 * 1024 * 1024;
    constexpr size_t GiB = 1024ULL * 1024 * 1024;
    constexpr size_t MiB = 1024ULL * 1024;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2);

    if (size >= TiB) {
        ss << static_cast<double>(size) / TiB << " TiB";
    } else if (size >= GiB) {
        ss << static_cast<double>(size) / GiB << " GiB";
    } else if (size >= MiB) {
        ss << static_cast<double>(size) / MiB << " MiB";
    } else if (size >= 1024) {
        ss << static_cast<double>(size) / 1024 << " KiB";
    } else {
        ss << size << " B";
    }

    return ss.str();
}

std::stop_source& GetGlobalInterruptSource() {
    static std::stop_source StopByInterrupt;
    return StopByInterrupt;
}

std::atomic<bool>& GetGlobalErrorVariable() {
    static std::atomic<bool> errorFlag{false};
    return errorFlag;
}

#ifdef __linux__
size_t NumberOfMyCpus() {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1) {
        return std::thread::hardware_concurrency();
    }

    int count = 0;
    for (int i = 0; i < CPU_SETSIZE; i++) {
        if (CPU_ISSET(i, &set))
            count++;
    }

    return count;
}
#else
size_t NumberOfMyCpus() {
    return std::thread::hardware_concurrency();
}
#endif

//-----------------------------------------------------------------------------

std::string GetEffectiveSchema(const std::string& path) {
    return path.empty() ? "public" : path;
}

} // namespace NTPCC
