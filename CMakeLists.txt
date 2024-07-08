cmake_minimum_required(VERSION 3.16)

project(arrow_benchmarks)

# Set the vcpkg toolchain file
set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake"
    CACHE STRING "Vcpkg toolchain file")

# Enable verbose CMake logging
set(CMAKE_FIND_DEBUG_MODE ON)

# Find packages
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

# Print found package information
message(STATUS "Arrow version: ${Arrow_VERSION}")
message(STATUS "Parquet version: ${Parquet_VERSION}")

# Print available targets
get_property(targets DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" PROPERTY IMPORTED_TARGETS)
message(STATUS "Available imported targets: ${targets}")

add_library(data_generator STATIC
    src/data_generator.cc
)

# Link Arrow and Parquet to data_generator
target_link_libraries(data_generator PRIVATE 
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
)

# metadata_benchmark executable
add_executable(
    metadata_benchmark 
    src/metadata_benchmark.cc
)

# Link data_generator and other libraries to metadata_benchmark
target_link_libraries(metadata_benchmark PRIVATE 
    data_generator
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
    pthread
    dl
    stdc++fs
)