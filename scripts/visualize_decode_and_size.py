import matplotlib.pyplot as plt
import pandas as pd
import sys

def plot_benchmarks(data):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    ax1.plot(data['num_columns'], data['decode_time_ms'], marker='o')
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax1.set_xlabel('Number of columns')
    ax1.set_ylabel('Metadata decode time (ms)')
    ax1.set_title('Metadata decode time (trend)')

    ax2.plot(data['num_columns'], data['size_mb'], marker='o')
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.set_xlabel('Number of columns')
    ax2.set_ylabel('Metadata size (MB)')
    ax2.set_title('Metadata size (trend)')

    plt.tight_layout()
    plt.savefig('./temp/decode_and_size.png')
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python visualize_benchmarks.py <benchmark_decode_and_size.csv>")
        sys.exit(1)

    data = pd.read_csv(sys.argv[1])
    plot_benchmarks(data)
