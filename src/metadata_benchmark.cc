#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include <chrono>
#include <fstream>
#include "metadata_benchmark.h"

BenchmarkChunksAndPagesResult BenchmarkChunksAndPages(const std::string& filename) {
    BenchmarkChunksAndPagesResult result;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename));

    auto start_total = std::chrono::high_resolution_clock::now();
    
    auto start_thrift = std::chrono::high_resolution_clock::now();
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader = 
        parquet::ParquetFileReader::Open(infile);
    auto end_thrift = std::chrono::high_resolution_clock::now();

    auto start_schema = std::chrono::high_resolution_clock::now();
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(parquet_reader), &arrow_reader));
    std::shared_ptr<arrow::Schema> schema;
    PARQUET_THROW_NOT_OK(arrow_reader->GetSchema(&schema));
    auto end_schema = std::chrono::high_resolution_clock::now();

    auto end_total = std::chrono::high_resolution_clock::now();

    result.total_decode_time = std::chrono::duration<double, std::micro>(end_total - start_total).count();
    result.thrift_decode_time = std::chrono::duration<double, std::micro>(end_thrift - start_thrift).count();
    result.schema_build_time = std::chrono::duration<double, std::micro>(end_schema - start_schema).count();
    result.size = infile->GetSize().ValueOrDie();
    result.num_columns = schema->num_fields();

    return result;
}

BenchmarkStatsResult BenchmarkStats(const std::string& filename) {
    BenchmarkStatsResult result;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename));

    auto start = std::chrono::high_resolution_clock::now();
    
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader = 
        parquet::ParquetFileReader::Open(infile);
    
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    int num_row_groups = file_metadata->num_row_groups();
    int num_columns = file_metadata->num_columns();

    for (int r = 0; r < num_row_groups; ++r) {
        auto row_group_reader = parquet_reader->RowGroup(r);
        for (int c = 0; c < num_columns; ++c) {
            auto column_chunk = row_group_reader->metadata()->ColumnChunk(c);
            if (column_chunk->is_stats_set()) {
                // Access statistics (this will trigger decoding)
                column_chunk->statistics()->HasMinMax();
            
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();

    result.stats_decode_time = std::chrono::duration<double, std::micro>(end - start).count();
    result.size = infile->GetSize().ValueOrDie();
    result.num_columns = num_columns;
    result.num_row_groups = num_row_groups;

    return result;
}

void WriteChunksAndPagesResults(const std::vector<BenchmarkChunksAndPagesResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    file << "num_columns,total_decode_time_us,thrift_decode_time_us,schema_build_time_us,size_bytes,stats_level\n";
    for (const auto& result : results) {
        file << result.num_columns << ","
             << result.total_decode_time << ","
             << result.thrift_decode_time << ","
             << result.schema_build_time << ","
             << result.size << ","
             << static_cast<int>(result.stats_level) << "\n";
    }
}

void WriteStatsBenchmarkResults(const std::vector<BenchmarkStatsResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    file << "num_columns,num_row_groups,stats_decode_time_us,size_bytes,stats_enabled\n";
    for (const auto& result : results) {
        file << result.num_columns << ","
             << result.num_row_groups << ","
             << result.stats_decode_time << ","
             << result.size << ","
             << (result.stats_enabled ? "true" : "false") << "\n";
    }
}

int main() {
    std::vector<int> column_counts = {10, 100, 1000, 10000};
    std::vector<StatsLevel> stats_levels = {StatsLevel::NONE, StatsLevel::CHUNK, StatsLevel::PAGE};
    int num_rows = 10000;  // 1 million rows
    std::string chunks_and_pages_output_file = "benchmark_chunks_and_pages.csv";
    std::string stats_output_file = "benchmark_results.csv";

    std::vector<BenchmarkChunksAndPagesResult> chunks_and_pages_results;
    std::vector<BenchmarkStatsResult> stats_results;

    for (int num_columns : column_counts) {
        for (auto stats_level : stats_levels) {
            std::string filename = "benchmark_float32_" + std::to_string(num_columns) +
                                   "cols_" + std::to_string(static_cast<int>(stats_level)) + "sl.parquet";
            
            auto status = DataGenerator::WriteParquetFile(num_columns, num_rows, filename, stats_level);
            if (!status.ok()) {
                std::cerr << "Error writing file " << filename << ": " << status.ToString() << std::endl;
                continue;
            }

            auto chunks_and_pages_result = BenchmarkChunksAndPages(filename);
            chunks_and_pages_result.stats_level = stats_level;
            chunks_and_pages_results.push_back(chunks_and_pages_result);

            // Benchmark statistics
            auto stats_result = BenchmarkStats(filename);
            stats_result.stats_enabled = (stats_level != StatsLevel::NONE);
            stats_results.push_back(stats_result);

            std::remove(filename.c_str());
        }
    }

    WriteChunksAndPagesResults(chunks_and_pages_results, chunks_and_pages_output_file);
    WriteStatsBenchmarkResults(stats_results, stats_output_file);
    return 0;
}