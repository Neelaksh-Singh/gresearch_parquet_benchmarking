#pragma once
#include <arrow/api.h>
#include <string>

enum class StatsLevel {
    NONE,
    CHUNK,
    PAGE
};

class DataGenerator {
public:
    static arrow::Status WriteParquetFile(int num_columns, int num_rows, const std::string& filename, 
                                          StatsLevel stats_level = StatsLevel::NONE);
};