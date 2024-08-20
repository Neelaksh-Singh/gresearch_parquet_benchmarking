#include <iostream>
#include <vector>
#include <random>
#include <memory>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <zlib.h>
#include <chrono>

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
            fields.push_back(arrow::field("column_" + std::to_string(i), arrow::float64()));
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

void SetDefaultCounters(benchmark::State& state) {
    state.counters["OriginalMetadataSize"] = 0;
    state.counters["CombinedMetadataSize"] = 0;
    state.counters["FlatBufferSize"] = 0;
}

void SetMetadataCounter(benchmark::State& state, size_t metadata_size) {
    state.counters["MetadataSize"] = metadata_size;
}
//Struct for Storing benchmark data
struct BenchmarkResult {
    int num_columns;
    double thrift_parse_time;
    double flatbuffer_encode_time;
    double flatbuffer_parse_time;
    double combined_parse_time;
    size_t original_metadata_size;
    size_t combined_metadata_size;
    size_t flatbuffer_size;
    double thrift_parse_time_avg;
    double flatbuffer_parse_time_avg;
};
std::map<int, BenchmarkResult> benchmark_results;

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
        {2000, 10000}
    };

    for (const auto& spec : file_specs) {
        int num_columns = spec.first;
        int num_rows = spec.second;
        std::string filename = "benchmark_float64_" + std::to_string(num_columns) + "cols.parquet";
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
        "benchmark_float64_3000cols.parquet" : 
        "benchmark_float64_2000cols.parquet";
    
    auto file = OpenReadableFile(filename);
    
    double total_time = 0;
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        std::shared_ptr<parquet::FileMetaData> metadata = parquet::ReadMetaData(file);
        auto end = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration<double, std::milli>(end - start).count();
        benchmark::DoNotOptimize(metadata);
    }
    total_time *=  state.iterations();
    benchmark_results[state.range(0)].num_columns = state.range(0);
    benchmark_results[state.range(0)].thrift_parse_time = total_time;
    benchmark_results[state.range(0)].thrift_parse_time_avg = total_time / state.iterations();
    SetDefaultCounters(state);
    // state.counters["OriginalMetadataSize"] = metadata.size();
}
BENCHMARK(BM_ParseThrift)->Arg(3000)->Arg(2000);

static void BM_EncodeFlatbuffer(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float64_3000cols.parquet" : 
        "benchmark_float64_2000cols.parquet";
    
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);

    double total_time = 0;
    size_t flatbuffer_size = 0;
    for (auto _ : state) {
        try {
            auto start = std::chrono::high_resolution_clock::now();
            flatbuffers::FlatBufferBuilder builder;
            auto flatbuffer_metadata = writer.ConvertToFlatbuffer(metadata, builder);
            builder.Finish(flatbuffer_metadata);
            auto end = std::chrono::high_resolution_clock::now();
            total_time += std::chrono::duration<double, std::milli>(end - start).count();
            flatbuffer_size = builder.GetSize();
        } catch (const std::exception& e) {
            std::cerr << "Error in BM_EncodeFlatbuffer: " << e.what() << std::endl;
            state.SkipWithError("FlatBuffer encoding failed");
            break;
        }
    }
    total_time *=  state.iterations();
    benchmark_results[state.range(0)].flatbuffer_encode_time = total_time;
    benchmark_results[state.range(0)].flatbuffer_size = flatbuffer_size;
    SetDefaultCounters(state);
    state.counters["FlatBufferSize"] = flatbuffer_size;
}
BENCHMARK(BM_EncodeFlatbuffer)->Arg(3000)->Arg(2000);

static void BM_ParseFlatbuffer(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float64_3000cols.parquet" : 
        "benchmark_float64_2000cols.parquet";
    
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);
    
    flatbuffers::FlatBufferBuilder builder;
    auto flatbuffer_metadata = writer.ConvertToFlatbuffer(metadata, builder);
    builder.Finish(flatbuffer_metadata);
    double total_time = 0;
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto fmd = parquet2::GetFileMetaData(builder.GetBufferPointer());
        auto end = std::chrono::high_resolution_clock::now();
        total_time += std::chrono::duration<double, std::milli>(end - start).count();
        benchmark::DoNotOptimize(fmd->version());
    }
    total_time *=  state.iterations();
    benchmark_results[state.range(0)].flatbuffer_parse_time = total_time;
    benchmark_results[state.range(0)].flatbuffer_parse_time_avg = total_time / state.iterations();
    SetDefaultCounters(state);
}
BENCHMARK(BM_ParseFlatbuffer)->Arg(3000)->Arg(2000);

static void BM_ParseWithExtension(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float64_3000cols.parquet" : 
        "benchmark_float64_2000cols.parquet";
    
    // Read the original Parquet file
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> parquet_metadata = reader->metadata();
    
    // Serialize the original metadata to Thrift
    std::shared_ptr<arrow::io::BufferOutputStream> out_stream;
    PARQUET_ASSIGN_OR_THROW(out_stream, arrow::io::BufferOutputStream::Create());
    parquet_metadata->WriteTo(out_stream.get());
    
    std::shared_ptr<arrow::Buffer> buffer;
    PARQUET_ASSIGN_OR_THROW(buffer, out_stream->Finish());
    std::string serialized_metadata = buffer->ToString();

    // Create FlatBuffer
    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);
    flatbuffers::FlatBufferBuilder builder;
    auto flatbuffer_metadata = writer.ConvertToFlatbuffer(parquet_metadata, builder);
    builder.Finish(flatbuffer_metadata);

    // Combine Thrift and FlatBuffer metadata
    std::string combined_metadata = serialized_metadata;
    std::string flatbuffer_data(reinterpret_cast<const char*>(builder.GetBufferPointer()), builder.GetSize());
    combined_metadata += flatbuffer_data;
    
    // Add separator
    combined_metadata += "FBUF";

    // Add metadata size (4 bytes) and magic bytes
    int32_t metadata_size = combined_metadata.size();
    combined_metadata.append(reinterpret_cast<const char*>(&metadata_size), 4);
    combined_metadata += "PAR1";

    double total_combined_parse_time = 0.0;
    int iterations = 0;

    for (auto _ : state) {
        try {
            auto buffer = std::make_shared<arrow::Buffer>(combined_metadata);
            auto file = std::make_shared<arrow::io::BufferReader>(buffer);

            auto start = std::chrono::high_resolution_clock::now();
            
            // Parse Thrift metadata
            std::shared_ptr<parquet::FileMetaData> md = parquet::ReadMetaData(file);
            benchmark::DoNotOptimize(md);

            // Parse FlatBuffer metadata
            auto fmd = parquet2::GetFileMetaData(flatbuffer_data.data());
            benchmark::DoNotOptimize(fmd);
            
            auto end = std::chrono::high_resolution_clock::now();
            total_combined_parse_time += std::chrono::duration<double, std::nano>(end - start).count();
            iterations++;

        } catch (const std::exception& e) {
            std::cerr << "Error parsing combined metadata: " << e.what() << std::endl;
            state.SkipWithError("Parsing failed");
            break;
        }
    }

    // Only print results once, after all iterations
    // if (state.iterations() == iterations) {
    //     std::cout << "Columns: " << state.range(0)
    //               << ", Original metadata size: " << serialized_metadata.size()
    //               << ", Combined metadata size: " << combined_metadata.size()
    //               << ", FlatBuffer size: " << builder.GetSize() << std::endl;
    //     std::cout << "Average combined parse time: " << (total_combined_parse_time / iterations / 1e6) << " ms" << std::endl;
    // }

    // Update the benchmark_results map
    benchmark_results[state.range(0)].num_columns = state.range(0);
    benchmark_results[state.range(0)].combined_parse_time = total_combined_parse_time / iterations;
    benchmark_results[state.range(0)].original_metadata_size = serialized_metadata.size();
    benchmark_results[state.range(0)].combined_metadata_size = combined_metadata.size();
    benchmark_results[state.range(0)].flatbuffer_size = flatbuffer_data.size();
    SetDefaultCounters(state);
    state.counters["OriginalMetadataSize"] = serialized_metadata.size();
    state.counters["CombinedMetadataSize"] = combined_metadata.size();
    state.counters["FlatBufferSize"] = builder.GetSize();
}
BENCHMARK(BM_ParseWithExtension)->Arg(3000)->Arg(2000);

static void BM_ReadPartialData(benchmark::State& state) {
    std::string filename = state.range(0) == 3000 ? 
        "benchmark_float64_3000cols.parquet" : 
        "benchmark_float64_2000cols.parquet";
    
    // Read the original Parquet file
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(filename);
    std::shared_ptr<parquet::FileMetaData> parquet_metadata = reader->metadata();
    
    // Serialize the original metadata to Thrift
    std::shared_ptr<arrow::io::BufferOutputStream> out_stream;
    PARQUET_ASSIGN_OR_THROW(out_stream, arrow::io::BufferOutputStream::Create());
    parquet_metadata->WriteTo(out_stream.get());
    
    std::shared_ptr<arrow::Buffer> buffer;
    PARQUET_ASSIGN_OR_THROW(buffer, out_stream->Finish());
    std::string serialized_metadata = buffer->ToString();

    // Create FlatBuffer
    ParquetFlatbufferWriter writer(filename, state.range(0), 10000);
    flatbuffers::FlatBufferBuilder builder;
    auto flatbuffer_metadata = writer.ConvertToFlatbuffer(parquet_metadata, builder);
    builder.Finish(flatbuffer_metadata);
    std::string flatbuffer_data(reinterpret_cast<const char*>(builder.GetBufferPointer()), builder.GetSize());

    int subset_size = state.range(1);  // New parameter for subset size

    double thrift_time = 0;
    double flatbuffer_time = 0;

    for (auto _ : state) {
        // Read partial data from Thrift
        auto start_thrift = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < subset_size && i < parquet_metadata->num_columns(); ++i) {
            std::string column_name = parquet_metadata->schema()->Column(i)->name();
            benchmark::DoNotOptimize(column_name);
        }
        auto end_thrift = std::chrono::high_resolution_clock::now();
        auto thrift_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_thrift - start_thrift);
        thrift_time += thrift_duration.count();

        // Read partial data from FlatBuffer
        auto start_flatbuffer = std::chrono::high_resolution_clock::now();
        auto fmd = parquet2::GetFileMetaData(flatbuffer_data.data());
        for (int i = 0; i < subset_size && i < fmd->schema()->size(); ++i) {
            std::string column_name = fmd->schema()->Get(i)->name()->str();
            benchmark::DoNotOptimize(column_name);
        }
        auto end_flatbuffer = std::chrono::high_resolution_clock::now();
        auto flatbuffer_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_flatbuffer - start_flatbuffer);
        flatbuffer_time += flatbuffer_duration.count();
    }

    state.counters["ThriftTime"] = benchmark::Counter(thrift_time / state.iterations(), benchmark::Counter::kAvgThreads);
    state.counters["FlatBufferTime"] = benchmark::Counter(flatbuffer_time / state.iterations(), benchmark::Counter::kAvgThreads);
    state.counters["ThriftSize"] = serialized_metadata.size();
    state.counters["FlatBufferSize"] = flatbuffer_data.size();
    state.counters["NumColumns"] = state.range(0);
    state.counters["SubsetSize"] = subset_size;
}

BENCHMARK(BM_ReadPartialData)
    ->Args({3000, 10})   // 3000 columns, read 10
    ->Args({3000, 100})  // 3000 columns, read 100
    ->Args({3000, 1000}) // 3000 columns, read 1000
    ->Args({3000, 3000}) // 3000 columns, read all
    ->Args({2000, 10})   // 2000 columns, read 10
    ->Args({2000, 100})  // 2000 columns, read 100
    ->Args({2000, 1000}) // 2000 columns, read 1000
    ->Args({2000, 2000}) // 2000 columns, read all
    ->Unit(benchmark::kNanosecond);

void WriteResultsToCSV(const std::string& filename) {
    std::ofstream file(filename);
    file << "NumColumns,ThriftParseTimeNs,FlatbufferEncodeTimeNs,FlatbufferParseTimeNs,CombinedParseTimeNs,OriginalMetadataSize,CombinedMetadataSize,FlatbufferSize,ThriftParseTimeAvgMs,FlatbufferParseTimeAvgMs\n";
    for (const auto& [num_columns, result] : benchmark_results) {
        file << result.num_columns << ","
             << result.thrift_parse_time << ","
             << result.flatbuffer_encode_time << ","
             << result.flatbuffer_parse_time << ","
             << result.combined_parse_time << ","
             << result.original_metadata_size << ","
             << result.combined_metadata_size << ","
             << result.flatbuffer_size << ","
             << (result.thrift_parse_time_avg * 1e-6) << ","
             << (result.flatbuffer_parse_time_avg * 1e-6) << "\n";
    }
}

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

    // WriteResultsToCSV("benchmark_flatbuffers.csv");

    return 0;
}