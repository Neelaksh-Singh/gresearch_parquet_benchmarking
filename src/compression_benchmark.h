#pragma once

#include <arrow/api.h>
#include <string>
#include <vector>

enum class CompressionAlgorithm {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    BROTLI,
    ZSTD
};

struct CompressionBenchmarkResult {
    CompressionAlgorithm algorithm;
    int num_columns;
    int num_rows;
    double encoding_time_ms;
    double decoding_time_ms;
    double compressed_size_mb;
};

class CompressionBenchmark {
public:
    static arrow::Status RunBenchmark(int num_columns, int num_rows, const std::string& filename_prefix);
    static void WriteBenchmarkResults(const std::vector<CompressionBenchmarkResult>& results, const std::string& filename);
};