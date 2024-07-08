#include "data_generator.h"
#include <parquet/arrow/writer.h>
#include <arrow/io/file.h>
#include <random>

arrow::Status DataGenerator::WriteParquetFile(int num_columns, int num_rows, const std::string& filename,
                                              StatsLevel stats_level) {
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

    parquet::WriterProperties::Builder builder;
    builder.version(parquet::ParquetVersion::PARQUET_2_6);

    switch (stats_level) {
        case StatsLevel::NONE:
            builder.disable_statistics();
            break;
        case StatsLevel::CHUNK:
            builder.enable_statistics();
            break;
        case StatsLevel::PAGE:
            builder.enable_statistics();
            // Note: Page index is enabled by default in newer versions
            break;
    }

    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 10000, builder.build()));
    ARROW_RETURN_NOT_OK(outfile->Close());

    return arrow::Status::OK();
}