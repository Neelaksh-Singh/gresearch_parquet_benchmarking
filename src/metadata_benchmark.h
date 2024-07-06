#ifndef METADATA_BENCHMARK_H
#define METADATA_BENCHMARK_H

#include <string>

struct BenchmarkResult {
    int num_columns;
    double decode_time;  // in milliseconds
    double size;         // in megabytes
};

BenchmarkResult BenchmarkMetadata(const std::string& filename);
void WriteBenchmarkResults(const std::vector<BenchmarkResult>& results, const std::string& filename);

#endif  // METADATA_BENCHMARK_H
