#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the root directory of your project
PROJECT_ROOT=$(dirname "$0")

# Define the vcpkg directory
VCPKG_DIR="$PROJECT_ROOT/vcpkg"

# Clone vcpkg if it doesn't already exist
if [ ! -d "$VCPKG_DIR" ] || [ -z "$( ls -A $VCPKG_DIR )" ]; then
    git clone https://github.com/Microsoft/vcpkg.git "$VCPKG_DIR"
fi

# Bootstrap vcpkg
"$VCPKG_DIR/bootstrap-vcpkg.sh"

# Integrate vcpkg with the project
"$VCPKG_DIR/vcpkg" integrate install

# Install dependencies from vcpkg.json
"$VCPKG_DIR/vcpkg" install

# Define the path to the vcpkg toolchain file
VCPKG_TOOLCHAIN_FILE="$VCPKG_DIR/scripts/buildsystems/vcpkg.cmake"

# Create the temp directory for storing test datasets
mkdir -p "$PROJECT_ROOT/temp"

# Create the build directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/build"

# Generate flatbuffer code
# Note that vcpkg.json must be set to use the correct version of flatbuffers
# See src/flatbuff_ns.fbs for details.
cd ./src
../vcpkg_installed/x64-linux/tools/flatbuffers/flatc --cpp -o . flatbuff_ns.fbs
cd ..


# Run CMake to configure the project
cmake -B "$PROJECT_ROOT/build" -S "$PROJECT_ROOT" -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN_FILE"

# Build the project
cmake --build "$PROJECT_ROOT/build" -j4
