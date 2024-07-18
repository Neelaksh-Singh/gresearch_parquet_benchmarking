#include "data_read_benchmark.h"
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <random>
#include <iostream>
#include <fstream>
#include <chrono>

arrow::Status DataReadBenchmark::GenerateParquetFile(int num_columns, int num_rows, const std::string& filename) {
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

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(filename));

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 10000));
    ARROW_RETURN_NOT_OK(outfile->Close());

    return arrow::Status::OK();
}

double DataReadBenchmark::MeasureMetadataDecodeTime(const std::string& filename) {
    auto start = std::chrono::high_resolution_clock::now();
    
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(filename).Value(&infile));

    std::shared_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), reinterpret_cast<std::unique_ptr<parquet::arrow::FileReader, std::default_delete<parquet::arrow::FileReader>>*>(&reader)));

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

double DataReadBenchmark::MeasureFullDataReadTime(const std::unique_ptr<parquet::arrow::FileReader>& reader) {
    auto start = std::chrono::high_resolution_clock::now();

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

double DataReadBenchmark::MeasureRandomColumnReadTime(const std::unique_ptr<parquet::arrow::FileReader>& reader, int num_columns) {
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int> column_indices;
    for (int i = 0; i < num_columns / 2; ++i) {
        column_indices.push_back(rand() % num_columns);
    }

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(column_indices, &table));

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

double DataReadBenchmark::MeasurePageReadTime(const std::unique_ptr<parquet::arrow::FileReader>& reader) {
    auto start = std::chrono::high_resolution_clock::now();

    auto file_metadata = reader->parquet_reader()->metadata();
    int num_row_groups = file_metadata->num_row_groups();
    int num_columns = file_metadata->num_columns();

    for (int i = 0; i < num_row_groups; i += num_row_groups / 10) {  // Read every 10th row group
    
        std::shared_ptr<arrow::ChunkedArray> array;
        PARQUET_THROW_NOT_OK(reader->ReadColumn(i, &array));
        
    }

    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

arrow::Status DataReadBenchmark::RunBenchmark(int num_columns, int num_rows, const std::string& filename) {
    ARROW_RETURN_NOT_OK(GenerateParquetFile(num_columns, num_rows, filename));

    BenchmarkResult result;
    result.num_columns = num_columns;
    result.num_rows = num_rows;

    result.metadata_decode_time_ms = MeasureMetadataDecodeTime(filename);

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    result.full_data_read_time_ms = MeasureFullDataReadTime(reader);
    result.random_column_read_time_ms = MeasureRandomColumnReadTime(reader, num_columns);
    result.page_read_time_ms = MeasurePageReadTime(reader);

    std::vector<BenchmarkResult> results = {result};
    WriteBenchmarkResults(results, filename + "_benchmark_results.csv");

    return arrow::Status::OK();
}

void DataReadBenchmark::WriteBenchmarkResults(const std::vector<BenchmarkResult>& results, const std::string& filename) {
    std::ofstream file(filename);
    file << "num_columns,num_rows,metadata_decode_time_ms,full_data_read_time_ms,random_column_read_time_ms,page_read_time_ms\n";
    for (const auto& result : results) {
        file << result.num_columns << ","
             << result.num_rows << ","
             << result.metadata_decode_time_ms << ","
             << result.full_data_read_time_ms << ","
             << result.random_column_read_time_ms << ","
             << result.page_read_time_ms << "\n";
    }
}

int main() {
    std::vector<int> column_counts = {10, 100, 1000, 2000};
    int num_rows = 100000;  

    for (int num_columns : column_counts) {
        std::string filename = "./temp/data_read_benchmark_" + std::to_string(num_columns) + ".parquet";
        std::cout << "Running benchmark for " << num_columns << " columns..." << std::endl;
        
        auto status = DataReadBenchmark::RunBenchmark(num_columns, num_rows, filename);
        if (!status.ok()) {
            std::cerr << "Error running benchmark for " << num_columns << " columns: " 
                      << status.ToString() << std::endl;
            return 1;
        }
        std::remove(filename.c_str());
    }

    std::cout << "All benchmarks completed successfully. Results saved to CSV files." << std::endl;
    return 0;
}