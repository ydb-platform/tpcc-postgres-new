#pragma once

#include <string>

namespace NTPCC {

// Pre-flight data checks before doing anything else.
// On issues, prints error and exits.

void CheckDbForInit(const std::string& connectionString, const std::string& path = {}) noexcept;

void CheckDbForImport(const std::string& connectionString, const std::string& path = {}) noexcept;

void CheckDbForRun(const std::string& connectionString, int expectedWhCount,
                   const std::string& path = {}) noexcept;

} // namespace NTPCC
