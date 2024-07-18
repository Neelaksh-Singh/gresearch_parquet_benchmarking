#include <arrow/api.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/file.h>
#include <random>
#include <vector>
#include <iostream>

arrow::Status WriteParquetFile(int num_columns, int num_rows, const std::string& filename) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    // Create schema and data for each column
    for (int i = 0; i < num_columns; ++i) {
        schema_vector.push_back(arrow::field("col_" + std::to_string(i), arrow::float32()));

        arrow::FloatBuilder builder(pool);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(-1000.0f, 1000.0f);

        for (int j = 0; j < num_rows; ++j) {
            ARROW_RETURN_NOT_OK(builder.Append(static_cast<float>(dis(gen))));
        }

        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder.Finish(&array));
        arrays.push_back(array);
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    auto table = arrow::Table::Make(schema, arrays);

    // Write the table to a Parquet file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(filename));

    auto write_result = parquet::arrow::WriteTable(*table, pool, outfile, 10000);
    ARROW_RETURN_NOT_OK(write_result);

    // Close the file after writing
    ARROW_RETURN_NOT_OK(outfile->Close());

    return arrow::Status::OK();
}

int main() {
    std::vector<int> column_counts = {10, 100, 1000, 10000};
    int num_rows = 10000;  // Adjust as needed

    for (int num_columns : column_counts) {
        std::string filename = "benchmark_float32_" + std::to_string(num_columns) + "cols.parquet";
        auto status = WriteParquetFile(num_columns, num_rows, filename);
        if (!status.ok()) {
            std::cerr << "Error writing file " << filename << ": " << status.ToString() << std::endl;
        } else {
            std::cout << "Successfully wrote " << filename << std::endl;
        }
    }

    return 0;
}