#!/bin/bash

# You may need these if not already installed
# sudo apt install python3.10-venv
# sudo apt install jupyter-core
# sudo apt install jupyter-nbconvert

python3 -m venv ./venv
source ./venv/bin/activate
pip install -r requirements.txt

jupyter nbconvert --to python 'Parquet-CPP-Benchmarking.ipynb'

# Run the notebook, check for errors
rm -f ./temp/*
python 'Parquet-CPP-Benchmarking.py'

