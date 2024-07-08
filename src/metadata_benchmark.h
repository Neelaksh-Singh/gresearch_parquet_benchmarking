#ifndef METADATA_BENCHMARK_H
#define METADATA_BENCHMARK_H

#include <string>
#include <vector>
#include "data_generator.h"

struct BenchmarkResult {
    int num_columns;
    double total_decode_time;  // in microseconds
    double thrift_decode_time; // in microseconds
    double schema_build_time;  // in microseconds
    double size;               // in bytes
    StatsLevel stats_level;
};

BenchmarkResult BenchmarkMetadata(const std::string& filename);
void WriteBenchmarkResults(const std::vector<BenchmarkResult>& results, const std::string& filename);

#endif  // METADATA_BENCHMARK_H