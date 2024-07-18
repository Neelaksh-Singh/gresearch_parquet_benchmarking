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

# Automatically find all source files in the src directory
file(GLOB_RECURSE SRC_FILES "src/*.cc")

# Create a list of all benchmark executables
set(BENCHMARK_EXECUTABLES "")

# Function to add a benchmark executable
function(add_benchmark_executable NAME)
    add_executable(${NAME} src/${NAME}.cc)
    target_link_libraries(${NAME} PRIVATE 
        data_generator
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
        pthread
        dl
        stdc++fs
    )
    list(APPEND BENCHMARK_EXECUTABLES ${NAME})
    set(BENCHMARK_EXECUTABLES ${BENCHMARK_EXECUTABLES} PARENT_SCOPE)
endfunction()

# Create the data_generator library
add_library(data_generator STATIC
    src/data_generator.cc
)

# Link Arrow and Parquet to data_generator
target_link_libraries(data_generator PRIVATE 
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
)

# Add benchmark executables
add_benchmark_executable(metadata_benchmark)
add_benchmark_executable(compression_benchmark)
add_benchmark_executable(data_read_benchmark)
# Automatically add new benchmark executables
foreach(SRC_FILE ${SRC_FILES})
    get_filename_component(FILE_NAME ${SRC_FILE} NAME_WE)
    if(NOT ${FILE_NAME} STREQUAL "data_generator" AND NOT ${FILE_NAME} IN_LIST BENCHMARK_EXECUTABLES)
        add_benchmark_executable(${FILE_NAME})
    endif()
endforeach()

# Print the list of all benchmark executables
message(STATUS "Benchmark executables: ${BENCHMARK_EXECUTABLES}")