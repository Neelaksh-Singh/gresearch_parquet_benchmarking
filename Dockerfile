FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y sudo build-essential git cmake curl zip unzip tar

WORKDIR /home

# Copy our vcpkg manifest defining our C++ dependencies
COPY vcpkg.json .
COPY Parquet-CPP-Benchmarking.ipynb .
COPY CMakeLists.txt .
COPY setup.sh .
COPY requirements.txt .
COPY test.sh .
ADD src ./src
ADD scripts ./scripts

# Run the setup
# vcpkg requirements
RUN apt-get install -y pkg-config flex bison
RUN --mount=type=cache,target=./vcpkg --mount=type=cache,target=./vcpkg_installed ./setup.sh

# Test requirements
RUN apt-get install -y python3.10-venv jupyter-core jupyter-nbconvert
