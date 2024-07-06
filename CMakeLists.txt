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

# List of source files for executables
set(SOURCE_FILES
    arrow_benchmarks
    test_data_generator
    table_viewer
    metadata_benchmark
)

# Add executables and link libraries in a loop
foreach(SOURCE_FILE IN LISTS SOURCE_FILES)
    add_executable(${SOURCE_FILE} src/${SOURCE_FILE}.cc)
    
    target_link_libraries(${SOURCE_FILE} PRIVATE
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>"
        pthread
        dl
        stdc++fs
    )
endforeach()
