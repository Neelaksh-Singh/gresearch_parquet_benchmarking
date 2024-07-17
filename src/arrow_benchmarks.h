#ifndef ARROW_BENCHMARKS_H
#define ARROW_BENCHMARKS_H

#include <string>
#include <vector>

struct BenchmarkResult {
    int num_columns;
    double decode_time;  // in milliseconds
    double size;         // in megabytes
};

BenchmarkResult BenchmarkMetadata(const std::string& filename);
void WriteBenchmarkResults(const std::vector<BenchmarkResult>& results, const std::string& filename);

#endif  // ARROW_BENCHMARKS_H
