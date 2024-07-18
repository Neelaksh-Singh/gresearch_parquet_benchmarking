#include "compression_benchmark.h"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/file.h>
#include <random>
#include <chrono>
#include <fstream>
#include <iostream>

arrow::Status CompressionBenchmark::RunBenchmark(int num_columns, int num_rows, const std::string& filename_prefix) {
    std::vector<CompressionBenchmarkResult> results;
    std::vector<CompressionAlgorithm> algorithms = {
        CompressionAlgorithm::UNCOMPRESSED,
        CompressionAlgorithm::SNAPPY,
        CompressionAlgorithm::GZIP,
        CompressionAlgorithm::BROTLI,
        CompressionAlgorithm::ZSTD
    };

    for (auto algo : algorithms) {
        CompressionBenchmarkResult result;
        result.algorithm = algo;
        result.num_columns = num_columns;
        result.num_rows = num_rows;

        // Generate data
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::vector<std::shared_ptr<arrow::Field>> schema_vector;
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (int i = 0; i < num_columns; ++i) {
            schema_vector.push_back(arrow::field("col_" + std::to_string(i), arrow::float32()));

            arrow::FloatBuilder builder(pool);
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<float> dis(-1000.0f, 1000.0f);

            for (int j = 0; j < num_rows; ++j) {
                ARROW_RETURN_NOT_OK(builder.Append(dis(gen)));
            }

            std::shared_ptr<arrow::Array> array;
            ARROW_RETURN_NOT_OK(builder.Finish(&array));
            arrays.push_back(array);
        }

        auto schema = std::make_shared<arrow::Schema>(schema_vector);
        auto table = arrow::Table::Make(schema, arrays);

        // Write with compression
        std::string filename = filename_prefix + "_" + std::to_string(static_cast<int>(algo)) + ".parquet";
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(filename));

        parquet::WriterProperties::Builder builder;
        builder.compression(static_cast<parquet::Compression::type>(algo));

        auto start = std::chrono::high_resolution_clock::now();
        ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile, 10000, builder.build()));
        auto end = std::chrono::high_resolution_clock::now();
        result.encoding_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

        ARROW_RETURN_NOT_OK(outfile->Close());

        // Get file size
        std::shared_ptr<arrow::io::ReadableFile> infile;
        ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));
        result.compressed_size_mb = static_cast<double>(infile->GetSize().ValueOrDie()) / (1024 * 1024);

        // Read and measure decoding time
        start = std::chrono::high_resolution_clock::now();
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, pool, &reader));
        std::shared_ptr<arrow::Table> read_table;
        ARROW_RETURN_NOT_OK(reader->ReadTable(&read_table));
        end = std::chrono::high_resolution_clock::now();
        result.decoding_time_ms = std::chrono::duration<double, std::milli>(end - start).count();

        results.push_back(result);

        // Clean up the temporary file
        std::remove(filename.c_str());
    }

    WriteBenchmarkResults(results, filename_prefix + "_compression_benchmark.csv");
    return arrow::Status::OK();
}

void CompressionBenchmark::WriteBenchmarkResults(const std::vector<CompressionBenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    file << "algorithm,num_columns,num_rows,encoding_time_ms,decoding_time_ms,compressed_size_mb\n";
    for (const auto& result : results) {
        file << static_cast<int>(result.algorithm) << ","
             << result.num_columns << ","
             << result.num_rows << ","
             << result.encoding_time_ms << ","
             << result.decoding_time_ms << ","
             << result.compressed_size_mb << "\n";
    }
}

int main() {
    std::vector<int> column_counts = {10, 100, 1000, 10000};
    int num_rows = 10000;  
    std::string filename_prefix = "./temp/compression_benchmark";

    for (int num_columns : column_counts) {
        std::cout << "Running benchmark for " << num_columns << " columns..." << std::endl;
        auto status = CompressionBenchmark::RunBenchmark(num_columns, num_rows, 
                                                         filename_prefix + "_" + std::to_string(num_columns));
        if (!status.ok()) {
            std::cerr << "Error running benchmark for " << num_columns << " columns: " 
                      << status.ToString() << std::endl;
            return 1;
        }
    }

    std::cout << "All benchmarks completed successfully. Results saved to CSV files." << std::endl;
    return 0;
}