#include <fstream>
#include <iostream>
#include <string_view>

#include "benchmark/benchmark.h"
#include "flatbuffers/flatbuffers.h"
#include "glog/logging.h"
#include "sql/core/src/main/c/parquet/thrift/parquet2_generated.h"
#include "sql/core/src/main/c/parquet/thrift/parquet_types.h"
#include "sql/core/src/main/c/parquet/thrift/serialization.h"

namespace thrift {
namespace {

const char* kFooters[] = {
    "sql/core/src/main/c/parquet/thrift/benchdata/large-footer1",
    "sql/core/src/main/c/parquet/thrift/benchdata/large-footer2",
};

const char* Footer(benchmark::State& state) { return kFooters[state.range(0)]; }

std::string ReadFile(std::string_view name) {
  std::stringstream buffer;
  std::ifstream t(name);
  buffer << t.rdbuf();
  return buffer.str();
}

std::string Serialize(const parquet::FileMetaData& md) {
  ThriftSerializer ser;
  absl::Span<uint8_t> footer;
  CHECK(ser.Serialize(md, &footer).ok());
  return std::string(reinterpret_cast<const char*>(footer.data()), footer.size());
}

void Parse(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  for (auto _ : state) {
    parquet::FileMetaData md;
    int64_t n;
    auto s = DeserializeThriftMessage(footer, &md, &n);
    CHECK(s.ok()) << s;
  }
}
BENCHMARK(Parse)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

parquet::FileMetaData OptimizeStatistics(parquet::FileMetaData md) {
  for (auto& rg : md.row_groups) {
    for (auto& cc : rg.columns) {
      if (cc.__isset.meta_data) {
        auto& cmd = cc.meta_data;
        if (cmd.__isset.statistics) {
          auto& s = cmd.statistics;
          // clear deprecated fields
          s.__isset.max = false;
          s.max.clear();
          s.__isset.min = false;
          s.min.clear();
          // optimize new ones
          if (s.__isset.max_value) {
            s.__isset.max8 = true;
            if (s.max_value.size() == 1) {
              s.__isset.max8 = s.max_value[0];
            } else if (s.max_value.size() == 4) {
              s.__isset.max8 = absl::little_endian::Load32(s.max_value.data());
            } else if (s.max_value.size() == 8) {
              s.__isset.max8 = absl::little_endian::Load64(s.max_value.data());
            }

            s.__isset.max_value = false;
            s.max_value.clear();
          }
          if (s.__isset.min_value) {
            s.__isset.min8 = true;
            if (s.min_value.size() == 1) {
              s.__isset.min8 = s.min_value[0];
            } else if (s.min_value.size() == 4) {
              s.__isset.min8 = absl::little_endian::Load32(s.min_value.data());
            } else if (s.min_value.size() == 8) {
              s.__isset.min8 = absl::little_endian::Load64(s.min_value.data());
            }
            s.__isset.min_value = false;
            s.min_value.clear();
          }
        }
      }
    }
  }
  return md;
}

parquet::FileMetaData OptimizePathInSchema(parquet::FileMetaData md) {
  for (auto& rg : md.row_groups) {
    int i = 1;
    for (auto& cc : rg.columns) {
      if (cc.__isset.meta_data) {
        auto& cmd = cc.meta_data;

        cmd.__isset.type = false;
        cmd.__isset.path_in_schema = false;
        cmd.path_in_schema.clear();

        cmd.__isset.schema_index = true;
        cmd.schema_index = i;
        ++i;
      }
    }
  }
  return md;
}

parquet::FileMetaData OptimizeEncodings(parquet::FileMetaData md) {
  for (auto& rg : md.row_groups) {
    for (auto& cc : rg.columns) {
      if (cc.__isset.meta_data) {
        auto& cmd = cc.meta_data;

        cmd.__isset.is_fully_dict_encoded = true;
        cmd.is_fully_dict_encoded = true;
        for (auto& e : cmd.encoding_stats) {
          if (e.page_type == parquet::PageType::DATA_PAGE ||
              e.page_type == parquet::PageType::DATA_PAGE_V2) {
            cmd.is_fully_dict_encoded &= e.encoding == parquet::Encoding::PLAIN_DICTIONARY ||
                                         e.encoding == parquet::Encoding::RLE_DICTIONARY;
          }
        }
        cmd.__isset.encoding_stats = false;
        cmd.encoding_stats.clear();
      }
    }
  }
  return md;
}

parquet::FileMetaData OptimizeT1(parquet::FileMetaData md) {
  return OptimizeStatistics(OptimizeEncodings(md));
}

parquet::FileMetaData OptimizeT2(parquet::FileMetaData md) {
  return OptimizePathInSchema(md);
}

parquet::FileMetaData OptimizeAll(parquet::FileMetaData md) {
  return OptimizeT1(OptimizeT2(md));
}

void ParseOptimizedT1(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  md = OptimizeT1(md);
  ThriftSerializer ser;
  absl::Span<uint8_t> footer2;
  CHECK(ser.Serialize(md, &footer2).ok());

  for (auto _ : state) {
    int64_t n;
    CHECK(DeserializeThriftMessage(footer2, &md, &n).ok());
  }
}
BENCHMARK(ParseOptimizedT1)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

void ParseOptimizedT2(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  md = OptimizeT2(md);
  ThriftSerializer ser;
  absl::Span<uint8_t> footer2;
  CHECK(ser.Serialize(md, &footer2).ok());

  for (auto _ : state) {
    int64_t n;
    CHECK(DeserializeThriftMessage(footer2, &md, &n).ok());
  }
}
BENCHMARK(ParseOptimizedT2)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

void ParseOptimizedAll(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  md = OptimizeAll(md);
  ThriftSerializer ser;
  absl::Span<uint8_t> footer2;
  CHECK(ser.Serialize(md, &footer2).ok());

  for (auto _ : state) {
    int64_t n;
    CHECK(DeserializeThriftMessage(footer2, &md, &n).ok());
  }
}
BENCHMARK(ParseOptimizedAll)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

class Converter {
 public:
  auto operator()(parquet::TimeUnit t) {
    if (t.__isset.NANOS) return parquet2::TimeUnit::NanoSeconds;
    if (t.__isset.MICROS) return parquet2::TimeUnit::MicroSeconds;
    return parquet2::TimeUnit::MilliSeconds;
  }

  auto operator()(parquet::Type::type t) { return static_cast<parquet2::Type>(t); }
  auto operator()(parquet::FieldRepetitionType::type t) {
    return static_cast<parquet2::FieldRepetitionType>(t);
  }
  auto operator()(parquet::ConvertedType::type t) {
    return static_cast<parquet2::ConvertedType>(t);
  }
  auto operator()(parquet::CompressionCodec::type c) {
    return static_cast<parquet2::CompressionCodec>(c);
  }
  auto operator()(parquet::Encoding::type e) { return static_cast<parquet2::Encoding>(e); }
  auto operator()(parquet::PageType::type p) { return static_cast<parquet2::PageType>(p); }

  template <typename T>
  auto operator()(const std::vector<T>& in) {
    std::vector<decltype((*this)(in[0]))> out;
    out.reserve(in.size());
    for (auto&& e : in) out.push_back((*this)(e));
    return builder_.CreateVector(out);
  }

  std::pair<parquet2::LogicalType, flatbuffers::Offset<void>> operator()(
      const parquet::LogicalType& t) {
    if (t.__isset.STRING) {
      return {parquet2::LogicalType::StringType, parquet2::CreateStringType(builder_).Union()};
    } else if (t.__isset.MAP) {
      return {parquet2::LogicalType::MapType, parquet2::CreateMapType(builder_).Union()};
    } else if (t.__isset.LIST) {
      return {parquet2::LogicalType::ListType, parquet2::CreateListType(builder_).Union()};
    } else if (t.__isset.ENUM) {
      return {parquet2::LogicalType::EnumType, parquet2::CreateEnumType(builder_).Union()};
    } else if (t.__isset.DECIMAL) {
      return {parquet2::LogicalType::DecimalType,
              parquet2::CreateDecimalType(builder_, t.DECIMAL.precision, t.DECIMAL.scale).Union()};
    } else if (t.__isset.DATE) {
      return {parquet2::LogicalType::DateType, parquet2::CreateDateType(builder_).Union()};
    } else if (t.__isset.TIME) {
      auto tu = (*this)(t.TIME.unit);
      return {parquet2::LogicalType::TimeType,
              parquet2::CreateTimeType(builder_, t.TIME.isAdjustedToUTC, tu).Union()};
    } else if (t.__isset.TIMESTAMP) {
      auto tu = (*this)(t.TIMESTAMP.unit);
      return {parquet2::LogicalType::TimestampType,
              parquet2::CreateTimeType(builder_, t.TIMESTAMP.isAdjustedToUTC, tu).Union()};
    } else if (t.__isset.INTEGER) {
      return {parquet2::LogicalType::IntType,
              parquet2::CreateIntType(builder_, t.INTEGER.bitWidth, t.INTEGER.isSigned).Union()};
    } else if (t.__isset.UNKNOWN) {
      return {parquet2::LogicalType::NullType, parquet2::CreateNullType(builder_).Union()};
    } else if (t.__isset.JSON) {
      return {parquet2::LogicalType::JsonType, parquet2::CreateJsonType(builder_).Union()};
    } else if (t.__isset.BSON) {
      return {parquet2::LogicalType::BsonType, parquet2::CreateBsonType(builder_).Union()};
    } else if (t.__isset.UUID) {
      return {parquet2::LogicalType::UUIDType, parquet2::CreateUUIDType(builder_).Union()};
    }
    std::unreachable();
  }

  auto operator()(const parquet::SchemaElement& e) {
    auto name = builder_.CreateSharedString(e.name);
    auto [logical_type, logical_type_union] = (*this)(e.logicalType);

    parquet2::SchemaElementBuilder b(builder_);
    if (e.__isset.type) b.add_type((*this)(e.type));
    if (e.__isset.type_length) b.add_type_length(e.type_length);
    if (e.__isset.repetition_type) b.add_repetition_type((*this)(e.repetition_type));
    b.add_name(name);
    if (e.__isset.num_children) b.add_num_children(e.num_children);
    if (e.__isset.converted_type) b.add_converted_type((*this)(e.converted_type));
    if (e.__isset.scale) b.add_scale(e.scale);
    if (e.__isset.precision) b.add_precision(e.precision);
    if (e.__isset.field_id) b.add_field_id(e.field_id);
    if (e.__isset.logicalType) {
      b.add_logicalType_type(logical_type);
      b.add_logicalType(logical_type_union);
    }
    return b.Finish();
  }

  auto operator()(const parquet::KeyValue& kv) {
    auto key = builder_.CreateSharedString(kv.key);
    auto value = builder_.CreateString(kv.value);

    parquet2::KeyValueBuilder b(builder_);
    b.add_key(key);
    b.add_value(value);
    return b.Finish();
  }

  auto operator()(const parquet::PageEncodingStats& s) {
    auto p = (*this)(s.page_type);
    auto e = (*this)(s.encoding);
    return parquet2::CreatePageEncodingStats(builder_, p, e, s.count);
  }

  auto ToBinary(const std::string& v) {
    return builder_.CreateVector(reinterpret_cast<const int8_t*>(v.data()), v.size());
  }

  auto operator()(const parquet::Statistics& s) {
    auto max = ToBinary(s.max);
    auto min = ToBinary(s.min);
    auto max_value = ToBinary(s.max_value);
    auto min_value = ToBinary(s.min_value);

    parquet2::StatisticsBuilder b(builder_);
    if (s.__isset.max) b.add_max(max);
    if (s.__isset.min) b.add_min(min);
    if (s.__isset.null_count) b.add_null_count(s.null_count);
    if (s.__isset.distinct_count) b.add_distinct_count(s.distinct_count);
    if (s.__isset.max_value) b.add_max_value(max_value);
    if (s.__isset.min_value) b.add_min_value(min_value);
    if (s.__isset.is_max_value_exact) b.add_is_max_value_exact(s.is_max_value_exact);
    if (s.__isset.is_min_value_exact) b.add_is_min_value_exact(s.is_min_value_exact);
    if (s.__isset.max8) b.add_max8(s.max8);
    if (s.__isset.min8) b.add_min8(s.min8);
    return b.Finish();
  }

  auto ToSharedStrings(const std::vector<std::string>& in) {
    std::vector<flatbuffers::Offset<flatbuffers::String>> out;
    out.reserve(in.size());
    for (auto&& s : in) out.push_back(builder_.CreateSharedString(s));
    return builder_.CreateVector(out);
  }

  auto operator()(const parquet::ColumnMetaData& cm) {
    auto type = (*this)(cm.type);
    auto encodings = (*this)(cm.encodings);
    auto path_in_schema = ToSharedStrings(cm.path_in_schema);
    auto codec = (*this)(cm.codec);
    auto kv_metadata = (*this)(cm.key_value_metadata);
    auto statistics = (*this)(cm.statistics);
    auto encoding_stats = (*this)(cm.encoding_stats);

    parquet2::ColumnMetadataBuilder b(builder_);
    b.add_type(type);
    b.add_encodings(encodings);
    b.add_path_in_schema(path_in_schema);
    b.add_codec(codec);
    b.add_num_values(cm.num_values);
    b.add_total_uncompressed_size(cm.total_uncompressed_size);
    b.add_key_value_metadata(kv_metadata);
    b.add_data_page_offset(cm.data_page_offset);
    if (cm.__isset.index_page_offset) b.add_index_page_offset(cm.index_page_offset);
    if (cm.__isset.dictionary_page_offset) b.add_dictionary_page_offset(cm.dictionary_page_offset);
    b.add_statistics(statistics);
    b.add_encoding_stats(encoding_stats);
    if (cm.__isset.bloom_filter_offset) b.add_bloom_filter_offset(cm.bloom_filter_offset);
    if (cm.__isset.schema_index) b.add_schema_index(cm.schema_index);
    return b.Finish();
  }

  auto operator()(const parquet::ColumnChunk& cc) {
    auto file_path = builder_.CreateString(cc.file_path);
    auto meta_data = (*this)(cc.meta_data);

    parquet2::ColumnChunkBuilder b(builder_);
    b.add_file_path(file_path);
    b.add_file_offset(cc.file_offset);
    b.add_meta_data(meta_data);
    if (cc.__isset.offset_index_offset) b.add_offset_index_offset(cc.offset_index_offset);
    if (cc.__isset.offset_index_length) b.add_offset_index_length(cc.offset_index_length);
    if (cc.__isset.column_index_offset) b.add_column_index_offset(cc.column_index_offset);
    if (cc.__isset.column_index_length) b.add_column_index_length(cc.column_index_length);
    if (cc.__isset.crypto_metadata) {
      // TODO
    }
    if (cc.__isset.encrypted_column_metadata) {
      // TODO
    }
    return b.Finish();
  }

  auto operator()(const parquet::SortingColumn& sc) {
    return parquet2::CreateSortingColumn(builder_, sc.column_idx, sc.descending, sc.nulls_first);
  }

  auto operator()(const parquet::RowGroup& rg) {
    auto columns = (*this)(rg.columns);
    auto sorting_columns = (*this)(rg.sorting_columns);

    parquet2::RowGroupBuilder b(builder_);
    b.add_columns(columns);
    b.add_total_byte_size(rg.total_byte_size);
    b.add_sorting_columns(sorting_columns);
    if (rg.__isset.file_offset) b.add_file_offset(rg.file_offset);
    if (rg.__isset.total_compressed_size) b.add_total_compressed_size(rg.total_compressed_size);
    if (rg.__isset.ordinal) b.add_ordinal(rg.ordinal);
    return b.Finish();
  }

  std::string operator()(const parquet::FileMetaData& md) {
    builder_.Clear();
    auto schema = (*this)(md.schema);
    auto row_groups = (*this)(md.row_groups);
    auto key_value_metadata = (*this)(md.key_value_metadata);
    auto created_by = builder_.CreateString(md.created_by);
    auto footer_signing_key_metadata = ToBinary(md.footer_signing_key_metadata);

    parquet2::FileMetaDataBuilder b(builder_);
    b.add_version(md.version);
    b.add_schema(schema);
    b.add_num_rows(md.num_rows);
    b.add_row_groups(row_groups);
    b.add_key_value_metadata(key_value_metadata);  // check empty
    b.add_created_by(created_by);                  // check empty
    // TODO column_orders
    // TODO encryption_algorithm
    b.add_footer_signing_key_metadata(footer_signing_key_metadata);
    auto out = b.Finish();
    builder_.Finish(out);
    return {reinterpret_cast<const char*>(builder_.GetBufferPointer()), builder_.GetSize()};
  }

 private:
  flatbuffers::FlatBufferBuilder builder_;
};

std::string ToFlatBuffer(const parquet::FileMetaData& md) { return Converter()(md); }

void EncodeFlatbuf(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());

  for (auto _ : state) {
    auto ser = ToFlatBuffer(md);
  }
}
BENCHMARK(EncodeFlatbuf)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

void ParseFlatbuf(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  auto ser = ToFlatBuffer(md);

  for (auto _ : state) {
    auto fmd = parquet2::GetFileMetaData(ser.data());
    CHECK_EQ(fmd->version(), md.version);
  }
}
BENCHMARK(ParseFlatbuf)->DenseRange(0, 1);

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

void ParseWithExtension(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  footer = AppendExtension(std::move(footer), ToFlatBuffer(md));
  for (auto _ : state) {
    parquet::FileMetaData md;
    int64_t n;
    auto s = DeserializeThriftMessage(footer, &md, &n);
    CHECK(s.ok()) << s;
  }
}
BENCHMARK(ParseWithExtension)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

void ParseAndVerifyFlatbuf(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  auto ser = ToFlatBuffer(md);

  for (auto _ : state) {
    flatbuffers::Verifier v(reinterpret_cast<uint8_t*>(ser.data()), ser.size());
    auto fmd = parquet2::GetFileMetaData(ser.data());
    CHECK_EQ(fmd->version(), md.version);
    CHECK(fmd->Verify(v));
  }
}
BENCHMARK(ParseAndVerifyFlatbuf)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

void EncodeOptimizedFlatbuf(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  md = OptimizeAll(md);

  for (auto _ : state) {
    auto ser = ToFlatBuffer(md);
  }
}
BENCHMARK(EncodeOptimizedFlatbuf)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

void ParseOptimizedFlatbuf(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  auto ser = ToFlatBuffer(OptimizeAll(md));

  for (auto _ : state) {
    auto fmd = parquet2::GetFileMetaData(ser.data());
    CHECK_EQ(fmd->version(), md.version);
  }
}
BENCHMARK(ParseOptimizedFlatbuf)->DenseRange(0, 1);

void ParseAndVerifyOptimizedFlatbuf(benchmark::State& state) {
  std::string footer = ReadFile(Footer(state));
  parquet::FileMetaData md;
  int64_t n;
  CHECK(DeserializeThriftMessage(footer, &md, &n).ok());
  auto ser = ToFlatBuffer(OptimizeAll(md));

  for (auto _ : state) {
    flatbuffers::Verifier v(reinterpret_cast<uint8_t*>(ser.data()), ser.size());
    auto fmd = parquet2::GetFileMetaData(ser.data());
    CHECK_EQ(fmd->version(), md.version);
    CHECK(fmd->Verify(v));
  }
}
BENCHMARK(ParseAndVerifyOptimizedFlatbuf)->DenseRange(0, 1)->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace thrift

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  ::google::InitGoogleLogging(argv[0]);
  for (const char* footer : thrift::kFooters) {
    std::string f = thrift::ReadFile(footer);
    parquet::FileMetaData md;
    int64_t n;
    CHECK(thrift::DeserializeThriftMessage(f, &md, &n).ok());
    auto md_opt_t1 = thrift::OptimizeStatistics(md);
    auto md_opt_t2 = thrift::OptimizePathInSchema(md);
    auto md_opt_all = thrift::OptimizeAll(md);
    ::benchmark::AddCustomContext(
        footer,
        absl::StrFormat("num-cols=%d orig=%d opt-t1=%d opt-t2=%d opt-all=%d flatbuf=%d "
                        "flatbuf-opt-all=%d",
                        md.schema.size(), thrift::Serialize(md).size(),
                        thrift::Serialize(md_opt_t1).size(), thrift::Serialize(md_opt_t2).size(),
                        thrift::Serialize(md_opt_all).size(), thrift::ToFlatBuffer(md).size(),
                        thrift::ToFlatBuffer(md_opt_all).size()));
  }
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
