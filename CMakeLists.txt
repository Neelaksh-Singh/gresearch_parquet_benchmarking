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
find_package(Snappy CONFIG REQUIRED)

# Print found package information
message(STATUS "Arrow version: ${Arrow_VERSION}")
message(STATUS "Parquet version: ${Parquet_VERSION}")
message(STATUS "Snappy version: ${Snappy_VERSION}")

# Print available targets
get_property(targets DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" PROPERTY IMPORTED_TARGETS)
message(STATUS "Available imported targets: ${targets}")

add_executable(arrow_benchmarks src/arrow_benchmarks.cc)

# Link against static libraries
target_link_libraries(arrow_benchmarks PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/build/vcpkg_installed/x64-linux/lib/libarrow.a
    ${CMAKE_CURRENT_SOURCE_DIR}/build/vcpkg_installed/x64-linux/lib/libparquet.a
    Snappy::snappy
)

# Add include directories
target_include_directories(arrow_benchmarks PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/build/vcpkg_installed/x64-linux/include
)

# Arrow and Parquet might require additional system libraries
target_link_libraries(arrow_benchmarks PRIVATE 
    pthread
    dl
)

# If you're using C++11 or later, you might need to link against std::filesystem
target_link_libraries(arrow_benchmarks PRIVATE stdc++fs)