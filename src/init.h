#pragma once

#include <string>

namespace NTPCC {

void InitSync(const std::string& connectionString);
void CreateIndexes(const std::string& connectionString);

} // namespace NTPCC
