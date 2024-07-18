#ifndef DATA_READ_BENCHMARK_H
#define DATA_READ_BENCHMARK_H

#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <string>
#include <vector>

struct BenchmarkResult {
    int num_columns;
    int num_rows;
    double metadata_decode_time_ms;
    double full_data_read_time_ms;
    double random_column_read_time_ms;
    double page_read_time_ms;
};

class DataReadBenchmark {
public:
    static arrow::Status GenerateParquetFile(int num_columns, int num_rows, const std::string& filename);
    static double MeasureMetadataDecodeTime(const std::string& filename);
    static double MeasureFullDataReadTime(const std::unique_ptr<parquet::arrow::FileReader>& reader);
    static double MeasureRandomColumnReadTime(const std::unique_ptr<parquet::arrow::FileReader>& reader, int num_columns);
    static double MeasurePageReadTime(const std::unique_ptr<parquet::arrow::FileReader>& reader);
    static arrow::Status RunBenchmark(int num_columns, int num_rows, const std::string& filename);
    static void WriteBenchmarkResults(const std::vector<BenchmarkResult>& results, const std::string& filename);
};

#endif // DATA_READ_BENCHMARK_H