#!/bin/bash

docker build -f Dockerfile -t parquet_benchmarking .
docker run --rm -ti parquet_benchmarking ./test.sh
