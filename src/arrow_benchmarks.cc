#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <chrono>
#include <fstream>
#include "arrow_benchmarks.h"

BenchmarkResult BenchmarkMetadata(const std::string& filename) {
    BenchmarkResult result;

    auto start = std::chrono::high_resolution_clock::now();
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);

    result.decode_time = std::chrono::duration<double, std::milli>(
        std::chrono::high_resolution_clock::now() - start).count();

    result.size = infile->GetSize().ValueOrDie() / (1024.0 * 1024.0);  // size in MB

    return result;
}

void WriteBenchmarkResults(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    file << "num_columns,decode_time_ms,size_mb\n";
    for (const auto& result : results) {
        file << result.num_columns << "," << result.decode_time << "," << result.size << "\n";
    }
}

int main() {
    std::vector<int> column_counts = {10, 100, 1000, 10000};
    std::string output_file = "benchmark_decode_and_size.csv";

    std::vector<BenchmarkResult> results;
    for (int num_columns : column_counts) {
        std::string filename = "benchmark_float32_" + std::to_string(num_columns) + "cols.parquet";
        auto result = BenchmarkMetadata(filename);
        result.num_columns = num_columns;
        results.push_back(result);
    }

    WriteBenchmarkResults(results, output_file);
    return 0;
}
