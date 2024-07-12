#ifndef METADATA_BENCHMARK_H
#define METADATA_BENCHMARK_H

#include <string>
#include <vector>
#include "data_generator.h"

struct BenchmarkChunksAndPagesResult {
    double total_decode_time;
    double thrift_decode_time;
    double schema_build_time;
    int64_t size;
    int num_columns;
    StatsLevel stats_level;
};

struct BenchmarkStatsResult {
    double stats_decode_time;
    int64_t size;
    int num_columns;
    int num_row_groups;
    bool stats_enabled;
};

BenchmarkChunksAndPagesResult BenchmarkChunksAndPages(const std::string& filename);
BenchmarkStatsResult BenchmarkStats(const std::string& filename);
void WriteChunksAndPagesResults(const std::vector<BenchmarkChunksAndPagesResult>& results, const std::string& filename);
void WriteStatsBenchmarkResults(const std::vector<BenchmarkStatsResult>& results, const std::string& filename);

#endif  // METADATA_BENCHMARK_H