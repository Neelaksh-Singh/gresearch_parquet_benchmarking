#include <iostream>
#include <vector>
#include <random>
#include <memory>
#include <fstream>
#include <stdexcept>

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/array/builder_primitive.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include "flatbuff_ns_generated.h"
#include <benchmark/benchmark.h>

class ParquetFlatbufferWriter {
public:
    ParquetFlatbufferWriter(const std::string& filename, int num_columns, int num_rows)
        : filename_(filename), num_columns_(num_columns), num_rows_(num_rows) {}

    void Write() {
        CreateParquetFile();
        // Removed AppendFlatbuffer() call
    }

    std::string GetFilename() const { return filename_; }

    flatbuffers::Offset<parquet2::FileMetaData> ConvertToFlatbuffer(
        const std::shared_ptr<parquet::FileMetaData>& metadata,
        flatbuffers::FlatBufferBuilder& builder) {
        try {
            auto schema = ConvertSchema(metadata->schema(), builder);
            // std::cout << "Schema converted successfully" << std::endl;
            
            auto row_groups = ConvertRowGroups(metadata, builder);
            // std::cout << "Row groups converted successfully" << std::endl;
            
            auto created_by = builder.CreateString(metadata->created_by());
            // std::cout << "Created by string created successfully" << std::endl;

            return parquet2::CreateFileMetaData(
                builder,
                metadata->version(),
                schema,
                metadata->num_rows(),
                row_groups,
                0,  // key_value_metadata
                created_by
            );
        } catch (const std::exception& e) {
            std::cerr << "Error in ConvertToFlatbuffer: " << e.what() << std::endl;
            throw;
        }
    }

private:
    void CreateParquetFile() {
        // Create schema
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (int i = 0; i < num_columns_; ++i) {
            fields.push_back(arrow::field("column_" + std::to_string(i), arrow::float32()));
        }
        auto schema = arrow::schema(fields);

        // Create random data
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(-1000.0, 1000.0);

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (int i = 0; i < num_columns_; ++i) {
            arrow::FloatBuilder builder;
            for (int j = 0; j < num_rows_; ++j) {
                PARQUET_THROW_NOT_OK(builder.Append(static_cast<float>(dis(gen))));
            }
            std::shared_ptr<arrow::Array> array;
            PARQUET_THROW_NOT_OK(builder.Finish(&array));
            arrays.push_back(array);
        }

        auto table = arrow::Table::Make(schema, arrays);

        // Write to Parquet
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(filename_));

        parquet::WriterProperties::Builder builder;
        builder.disable_statistics();
        auto properties = builder.build();

        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, num_rows_, properties));
    }

    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<parquet2::SchemaElement>>>
    ConvertSchema(const parquet::SchemaDescriptor* schema, flatbuffers::FlatBufferBuilder& builder) {
        std::vector<flatbuffers::Offset<parquet2::SchemaElement>> elements;
        for (int i = 0; i < schema->num_columns(); ++i) {
            const auto& column = schema->Column(i);
            try {
                elements.push_back(parquet2::CreateSchemaElement(
                    builder,
                    static_cast<parquet2::Type>(column->physical_type()),
                    0,  // type_length
                    parquet2::FieldRepetitionType_REQUIRED,
                    builder.CreateString(column->name())
                ));
            } catch (const std::exception& e) {
                std::cerr << "Error converting schema element " << i << ": " << e.what() << std::endl;
                throw;
            }
        }
        return builder.CreateVector(elements);
    }

    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<parquet2::RowGroup>>>
    ConvertRowGroups(const std::shared_ptr<parquet::FileMetaData>& metadata,
                     flatbuffers::FlatBufferBuilder& builder) {
        std::vector<flatbuffers::Offset<parquet2::RowGroup>> row_groups;
        for (int i = 0; i < metadata->num_row_groups(); ++i) {
            const auto& rg = metadata->RowGroup(i);
            try {
                row_groups.push_back(parquet2::CreateRowGroup(
                    builder,
                    ConvertColumns(rg.get(), builder),
                    rg->total_byte_size(),
                    rg->num_rows()
                ));
            } catch (const std::exception& e) {
                std::cerr << "Error converting row group " << i << ": " << e.what() << std::endl;
                throw;
            }
        }
        return builder.CreateVector(row_groups);
    }

    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<parquet2::ColumnChunk>>>
    ConvertColumns(const parquet::RowGroupMetaData* rg,
                   flatbuffers::FlatBufferBuilder& builder) {
        std::vector<flatbuffers::Offset<parquet2::ColumnChunk>> columns;
        for (int i = 0; i < rg->num_columns(); ++i) {
            auto col = rg->ColumnChunk(i);
            columns.push_back(parquet2::CreateColumnChunk(
                builder,
                builder.CreateString(col->file_path()),
                col->file_offset(),
                ConvertColumnMetadata(col, builder)
            ));
        }
        return builder.CreateVector(columns);
    }

    flatbuffers::Offset<parquet2::ColumnMetadata>
    ConvertColumnMetadata(const std::unique_ptr<parquet::ColumnChunkMetaData>& col,
                          flatbuffers::FlatBufferBuilder& builder) {
        return parquet2::CreateColumnMetadata(
            builder,
            static_cast<parquet2::Type>(col->type()),
            builder.CreateVector(std::vector<int8_t>{static_cast<int8_t>(col->encodings()[0])}),
            builder.CreateVector(std::vector<flatbuffers::Offset<flatbuffers::String>>{}),
            static_cast<parquet2::CompressionCodec>(col->compression()),
            col->num_values(),
            col->total_uncompressed_size(),
            col->total_compressed_size(),
            col->data_page_offset()
        );
    }

    std::string filename_;
    int num_columns_;
    int num_rows_;
};

std::shared_ptr<arrow::io::RandomAccessFile> OpenReadableFile(const std::string& filename) {
    PARQUET_ASSIGN_OR_THROW(auto file, arrow::io::ReadableFile::Open(filename));
    return file;
}

std::string AppendExtension(std::string thrift, std::string ext) {
    auto append_uleb = [](uint32_t x, std::string* out) {
        while (true) {
            int c = x & 0x7F;
            if ((x >>= 7) == 0) {
                out->push_back(c);
                return;
            } else {
                out->push_back(c | 0x80);
            }
        }
    };
    thrift.pop_back();  // remove the trailing 0
    thrift += "\x08\xFF\xFF\x01";
    append_uleb(ext.size(), &thrift);
    thrift += ext;
    thrift += "\x00";  // add the trailing 0 back
    return thrift;
}

void GenerateTestFiles() {
    std::vector<std::pair<int, int>> file_specs = {
        {3000, 10000},
        {5000, 10000}
    };

    for (const auto& spec : file_specs) {
        int num_columns = spec.first;
        int num_rows = spec.second;
        std::string filename = "benchmark_float32_" + std::to_string(num_columns) + "cols.parquet";
        if (std::ifstream(filename)) {
            std::cout << "File " << filename << " already exists. Skipping..." << std::endl;
            continue;
        }
        
        try {
            ParquetFlatbufferWriter writer(filename, num_columns, num_rows);
            writer.Write();
            std::cout << "Generated file: " << filename << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error generating file " << filename << ": " << e.what() << std::endl;
        }
    }
}

static void BM_ParseThrift(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float32_3000cols.parquet" : 
        "benchmark_float32_5000cols.parquet";
    
    auto file = OpenReadableFile(filename);
    
    for (auto _ : state) {
        std::shared_ptr<parquet::FileMetaData> metadata = parquet::ReadMetaData(file);
        benchmark::DoNotOptimize(metadata);
    }
}
BENCHMARK(BM_ParseThrift)->Arg(3000)->Arg(5000)->Unit(benchmark::kMillisecond);

static void BM_EncodeFlatbuffer(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float32_3000cols.parquet" : 
        "benchmark_float32_5000cols.parquet";
    
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);

    for (auto _ : state) {
        try {
            flatbuffers::FlatBufferBuilder builder;
            auto flatbuffer_metadata = writer.ConvertToFlatbuffer(metadata, builder);
            builder.Finish(flatbuffer_metadata);
        } catch (const std::exception& e) {
            std::cerr << "Error in BM_EncodeFlatbuffer: " << e.what() << std::endl;
            state.SkipWithError("FlatBuffer encoding failed");
            break;
        }
    }
}
BENCHMARK(BM_EncodeFlatbuffer)->Arg(3000)->Arg(5000)->Unit(benchmark::kMillisecond);

static void BM_ParseFlatbuffer(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float32_3000cols.parquet" : 
        "benchmark_float32_5000cols.parquet";
    
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);
    
    flatbuffers::FlatBufferBuilder builder;
    auto flatbuffer_metadata = writer.ConvertToFlatbuffer(metadata, builder);
    builder.Finish(flatbuffer_metadata);

    for (auto _ : state) {
        auto fmd = parquet2::GetFileMetaData(builder.GetBufferPointer());
        benchmark::DoNotOptimize(fmd->version());
    }
}
BENCHMARK(BM_ParseFlatbuffer)->Arg(3000)->Arg(5000);

static void BM_ParseWithExtension(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float32_3000cols.parquet" : 
        "benchmark_float32_5000cols.parquet";
    
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);
    
    flatbuffers::FlatBufferBuilder builder;
    auto flatbuffer_metadata = writer.ConvertToFlatbuffer(metadata, builder);
    builder.Finish(flatbuffer_metadata);
    
    auto file = OpenReadableFile(filename);
    PARQUET_ASSIGN_OR_THROW(auto buffer, file->Read(file->GetSize().ValueOrDie()));
    std::string combined_footer = AppendExtension(buffer->ToString(), 
        std::string(reinterpret_cast<const char*>(builder.GetBufferPointer()), builder.GetSize()));
    
    auto combined_file = std::make_shared<arrow::io::BufferReader>(combined_footer);
    
    for (auto _ : state) {
        std::shared_ptr<parquet::FileMetaData> md = parquet::ReadMetaData(combined_file);
        benchmark::DoNotOptimize(md);
    }
}
BENCHMARK(BM_ParseWithExtension)->Arg(3000)->Arg(5000)->Unit(benchmark::kMillisecond);

int main(int argc, char** argv) {
    try {
        
        GenerateTestFiles();
    } catch (const std::exception& e) {
        std::cerr << "Error in GenerateTestFiles: " << e.what() << std::endl;
        return 1;
    }
    
    ::benchmark::Initialize(&argc, argv);
    try {
        ::benchmark::RunSpecifiedBenchmarks();
    } catch (const std::exception& e) {
        std::cerr << "Error during benchmarking: " << e.what() << std::endl;
        return 1;
    }
    ::benchmark::Shutdown();
    return 0;
}