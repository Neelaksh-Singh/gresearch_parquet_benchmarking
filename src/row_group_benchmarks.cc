#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include <parquet/arrow/writer.h>
#include <random>
#include <chrono>
#include <fstream>
#include "metadata_benchmark.h"

arrow::Status WriteCustomParquetFile(int num_columns, int num_rows, 
                                     const std::string& filename, 
                                     parquet::Compression::type compression,
                                     int row_group_size,
                                     int page_size,
                                     bool enable_statistics) {
    // Create schema
    arrow::FieldVector fields;
    for (int i = 0; i < num_columns; ++i) {
        fields.push_back(arrow::field("col_" + std::to_string(i), arrow::float32()));
    }
    auto schema = arrow::schema(fields);

    // Create random data
    std::default_random_engine generator;
    std::uniform_real_distribution<float> distribution(0.0, 100.0);

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < num_columns; ++i) {
        arrow::FloatBuilder builder;
        for (int j = 0; j < num_rows; ++j) {
            ARROW_RETURN_NOT_OK(builder.Append(distribution(generator)));
        }
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder.Finish(&array));
        arrays.push_back(array);
    }

    auto table = arrow::Table::Make(schema, arrays);

    // Open output file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(filename));

    // Set up writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(compression)
           ->enable_statistics()
           ->data_pagesize(page_size);

    auto properties = builder.build();

    // Write the table
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), 
                                                   outfile, row_group_size, properties));

    return arrow::Status::OK();
}

struct BenchmarkResult {
    int num_columns;
    int num_rows;
    int row_group_size;
    int page_size;
    StatsLevel stats_level;
    double write_time_ms;
    double total_decode_time_ms;
    double thrift_decode_time_ms;
    double schema_build_time_ms;
    double stats_decode_time_ms;
    double file_size_mb;
    bool enable_statistics;
};

BenchmarkResult RunBenchmarkWithStats(int num_columns, int num_rows, int row_group_size, int page_size, bool enable_statistics) {
    BenchmarkResult result;
    result.num_columns = num_columns;
    result.num_rows = num_rows;
    result.row_group_size = row_group_size;
    result.page_size = page_size;
    result.enable_statistics = enable_statistics;

    std::string filename = "benchmark_float32_" + std::to_string(num_columns) + 
                           "cols_" + std::to_string(row_group_size) + "rg_" +
                           std::to_string(page_size) + "ps_" +
                           (enable_statistics ? "stats" : "nostats") + ".parquet";

    // Write Parquet file
    auto start = std::chrono::high_resolution_clock::now();
    auto status = WriteCustomParquetFile(num_columns, num_rows, filename, 
                                         parquet::Compression::SNAPPY,  // You can change this as needed
                                         row_group_size, page_size, enable_statistics);
    auto end = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

    if (!status.ok()) {
        std::cerr << "Error writing file " << filename << ": " << status.ToString() << std::endl;
        return result;
    }

    // Benchmark chunks and pages
    auto chunks_and_pages_result = BenchmarkChunksAndPages(filename);
    result.total_decode_time_ms = chunks_and_pages_result.total_decode_time / 1000.0;
    result.thrift_decode_time_ms = chunks_and_pages_result.thrift_decode_time / 1000.0;
    result.schema_build_time_ms = chunks_and_pages_result.schema_build_time / 1000.0;

    // Benchmark stats
    auto stats_result = BenchmarkStats(filename);
    result.stats_decode_time_ms = stats_result.stats_decode_time / 1000.0;

    // Calculate file size
    result.file_size_mb = chunks_and_pages_result.size / (1024.0 * 1024.0);

    std::remove(filename.c_str());

    return result;
}

void WriteResults(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    file << "num_columns,num_rows,row_group_size,page_size,stats_level,write_time_ms,total_decode_time_ms,"
         << "thrift_decode_time_ms,schema_build_time_ms,stats_decode_time_ms,file_size_mb\n";
    for (const auto& result : results) {
        file << result.num_columns << ","
             << result.num_rows << ","
             << result.row_group_size << ","
             << result.page_size << ","
             << static_cast<int>(result.stats_level) << ","
             << result.write_time_ms << ","
             << result.total_decode_time_ms << ","
             << result.thrift_decode_time_ms << ","
             << result.schema_build_time_ms << ","
             << result.stats_decode_time_ms << ","
             << result.file_size_mb << "\n";
    }
}

int main() {
    std::vector<int> column_counts = {10, 100, 1000};
    int num_rows = 10000;  // Keep this constant as in your original script
    std::vector<int> row_group_sizes = {1000, 2000, 5000, 10000};
    std::vector<int> page_sizes = {8 * 1024, 64 * 1024, 1 * 1024 * 1024, 8 * 1024 * 1024};
    std::vector<bool> enable_statistics_options = {false, true};

    std::vector<BenchmarkResult> results;

    for (int num_columns : column_counts) {
        for (int row_group_size : row_group_sizes) {
            for (int page_size : page_sizes) {
                for (bool enable_statistics : enable_statistics_options) {
                    results.push_back(RunBenchmarkWithStats(num_columns, num_rows, row_group_size, page_size, enable_statistics));
                }
            }
        }
    }

    WriteResults(results, "extensive_benchmark_results.csv");
    return 0;
}