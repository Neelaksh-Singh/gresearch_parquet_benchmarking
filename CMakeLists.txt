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

# test_data_generator.cc
# arrow_benchmarks.cc
add_executable(
    arrow_benchmarks src/test_data_generator.cc
)

find_package(Arrow CONFIG REQUIRED)
target_link_libraries(arrow_benchmarks PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")

find_package(Parquet CONFIG REQUIRED)
target_link_libraries(arrow_benchmarks PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")

add_executable(
    table_viewer src/table_viewer.cc
)


find_package(Arrow CONFIG REQUIRED)
target_link_libraries(table_viewer PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")

find_package(Parquet CONFIG REQUIRED)
target_link_libraries(table_viewer PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")

target_link_libraries(arrow_benchmarks PRIVATE 
    pthread
    dl
    stdc++fs
)

target_link_libraries(table_viewer PRIVATE 
    pthread
    dl
    stdc++fs
)


