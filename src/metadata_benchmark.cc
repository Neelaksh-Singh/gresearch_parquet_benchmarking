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

arrow::Status WriteCustomParquetFile(int num_columns, int num_rows, const std::string& filename, parquet::Compression::type compression,
                                     int row_group_size, int page_size, bool enable_statistics) {
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
    result.stats_level = enable_statistics ? StatsLevel::CHUNK : StatsLevel::NONE;

    std::string filename = "benchmark_float32_" + std::to_string(num_columns) + 
                           "cols_" + std::to_string(row_group_size) + "rg_" +
                           std::to_string(page_size) + "ps_" +
                           (enable_statistics ? "stats" : "nostats") + ".parquet";

    // Write Parquet file
    auto start = std::chrono::high_resolution_clock::now();
    auto status = WriteCustomParquetFile(num_columns, num_rows, filename, 
                                         parquet::Compression::SNAPPY,
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

void WriteRowGroupResults(const std::vector<BenchmarkResult>& results, const std::string& filename) {
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
    std::vector<int> column_counts = {10, 100, 1000, 10000};
    std::vector<StatsLevel> stats_levels = {StatsLevel::NONE, StatsLevel::CHUNK, StatsLevel::PAGE};
    int num_rows = 10000;
    std::string chunks_and_pages_output_file = "benchmark_chunks_and_pages.csv";
    std::string stats_output_file = "benchmark_stats.csv";
    std::string row_group_output_file = "benchmark_rowgroup.csv";

    std::vector<BenchmarkChunksAndPagesResult> chunks_and_pages_results;
    std::vector<BenchmarkStatsResult> stats_results;
    std::vector<BenchmarkResult> row_group_results;

    // Original metadata benchmark
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

            auto stats_result = BenchmarkStats(filename);
            stats_result.stats_enabled = (stats_level != StatsLevel::NONE);
            stats_results.push_back(stats_result);

            std::remove(filename.c_str());
        }
    }

    // Row group benchmark
    std::vector<int> row_group_sizes = {1000, 2000, 5000, 10000};
    std::vector<int> page_sizes = {8 * 1024, 64 * 1024, 1 * 1024 * 1024, 8 * 1024 * 1024};
    std::vector<bool> enable_statistics_options = {false, true};

    for (int num_columns : {10, 100, 1000}) {
        for (int row_group_size : row_group_sizes) {
            for (int page_size : page_sizes) {
                for (bool enable_statistics : enable_statistics_options) {
                    row_group_results.push_back(RunBenchmarkWithStats(num_columns, num_rows, row_group_size, page_size, enable_statistics));
                }
            }
        }
    }

    WriteChunksAndPagesResults(chunks_and_pages_results, chunks_and_pages_output_file);
    WriteStatsBenchmarkResults(stats_results, stats_output_file);
    WriteRowGroupResults(row_group_results, row_group_output_file);

    return 0;
}