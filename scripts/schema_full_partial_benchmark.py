import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import csv
import os

def create_parquet_file(num_columns, num_rows, filename):
    data = {f'col_{i}': np.random.rand(num_rows).astype(np.float32) for i in range(num_columns)}
    table = pa.Table.from_pydict(data)
    pq.write_table(table, filename)

def parquet_type_to_arrow_type(parquet_type):
    type_mapping = {
        'BOOLEAN': pa.bool_(),
        'INT32': pa.int32(),
        'INT64': pa.int64(),
        'FLOAT': pa.float32(),
        'DOUBLE': pa.float64(),
        'BYTE_ARRAY': pa.string(),
        'FIXED_LEN_BYTE_ARRAY': pa.binary(1)  # This might need adjustment based on actual length
    }
    return type_mapping.get(parquet_type, pa.string())  # Default to string if type is not recognized

def benchmark_schema_build(filename, column_subset=None):
    start_total = time.time()

    # Measure Thrift decoding time
    start_thrift = time.time()
    metadata = pq.read_metadata(filename)
    end_thrift = time.time()
    thrift_time = (end_thrift - start_thrift) * 1e6  # Convert to microseconds

    # Measure schema build time
    start_schema = time.time()
    if column_subset is None:
        # Full schema
        schema = metadata.schema.to_arrow_schema()
    else:
        # Subset schema
        subset_schema = metadata.schema
        subset_fields = [subset_schema.column(i) for i in column_subset]
        schema = pa.schema([pa.field(f.name, parquet_type_to_arrow_type(f.physical_type)) for f in subset_fields])
    end_schema = time.time()
    schema_time = (end_schema - start_schema) * 1e6  # Convert to microseconds

    end_total = time.time()
    total_time = (end_total - start_total) * 1e6  # Convert to microseconds

    return {
        'total_time': total_time,
        'thrift_time': thrift_time,
        'schema_time': schema_time,
        'total_columns': metadata.num_columns,
        'schema_columns': len(schema.names),
        'is_subset': column_subset is not None
    }

def run_benchmarks():
    column_counts = [10, 100, 1000, 10000]
    num_rows = 10000
    results = []

    for num_columns in column_counts:
        filename = f"benchmark_float32_{num_columns}cols.parquet"
        create_parquet_file(num_columns, num_rows, filename)

        try:
            # Full schema benchmark
            full_result = benchmark_schema_build(filename)
            results.append(full_result)

            # Subset schema benchmark (10% of columns or 10 columns, whichever is larger)
            subset_size = max(10, num_columns // 10)
            column_subset = list(range(subset_size))
            subset_result = benchmark_schema_build(filename, column_subset)
            results.append(subset_result)

        finally:
            # Delete the file after use
            if os.path.exists(filename):
                os.remove(filename)

    # Write results to CSV
    with open('benchmark_schema_full_partial.csv', 'w', newline='') as csvfile:
        fieldnames = ['total_columns', 'schema_columns', 'total_time', 'thrift_time', 'schema_time', 'is_subset']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)

if __name__ == "__main__":
    run_benchmarks()