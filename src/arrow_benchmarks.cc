#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/parquet_version.h>
#include <iostream>
#include<chrono>

int main() {
    std::cout << "Arrow version: " << ARROW_VERSION_STRING << std::endl;
    std::cout << "Parquet version: " 
              << PARQUET_VERSION_MAJOR << "."
              << PARQUET_VERSION_MINOR << "."
              << PARQUET_VERSION_PATCH << std::endl;
    return 0;
}