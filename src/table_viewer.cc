#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <arrow/io/file.h>
#include <iostream>

arrow::Status ReadParquetFile(const std::string& filename) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Open the Parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(filename));

    // Open the Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, pool, &arrow_reader));

    // Read the table from the Parquet file
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    // Get the number of rows in the table
    int num_rows = table->num_rows();

    // Display the top 20 rows
    int display_rows = std::min(20, num_rows);
    for (int i = 0; i < display_rows; ++i) {
        std::cout << "Row " << i << ":" << std::endl;
        for (int j = 0; j < table->num_columns(); ++j) {
            auto column = table->column(j);
            auto chunk = column->chunk(0);
            std::cout << "  " << table->schema()->field(j)->name() << ": "<< chunk->GetScalar(i).ValueOrDie()->ToString() << std::endl;
        }
        std::cout << std::endl;
    }

    return arrow::Status::OK();
}

int main() {
    std::string filename = "./temp/benchmark_float32_10cols.parquet";  // Replace with the desired filename

    auto status = ReadParquetFile(filename);
    if (!status.ok()) {
        std::cerr << "Error reading file " << filename << ": " << status.ToString() << std::endl;
    }

    return 0;
}