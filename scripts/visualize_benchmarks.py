import matplotlib.pyplot as plt
import pandas as pd
import sys
import os

def plot_benchmarks(data, output_path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Plot metadata decode time
    ax1.plot(data['num_columns'], data['decode_time_ms'], marker='o', label='Decode time (ms)')
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax1.set_xlabel('Number of columns')
    ax1.set_ylabel('Metadata decode time (ms)')
    ax1.set_title('Metadata decode time (trend)')
    ax1.legend()
    
    # Annotate data points
    for i in range(len(data)):
        ax1.annotate(f"{data['decode_time_ms'][i]:.2f}", (data['num_columns'][i], data['decode_time_ms'][i]), 
                     textcoords="offset points", xytext=(0,5), ha='center')

    # Plot metadata size
    ax2.plot(data['num_columns'], data['size_mb'], marker='o', label='Size (MB)')
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.set_xlabel('Number of columns')
    ax2.set_ylabel('Metadata size (MB)')
    ax2.set_title('Metadata size (trend)')
    ax2.legend()
    
    # Annotate data points
    for i in range(len(data)):
        ax2.annotate(f"{data['size_mb'][i]:.2f}", (data['num_columns'][i], data['size_mb'][i]), 
                     textcoords="offset points", xytext=(0,5), ha='center')

    plt.tight_layout()
    plt.savefig(output_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python visualize_benchmarks.py <benchmark_results.csv>")
        sys.exit(1)
    
    data = pd.read_csv(sys.argv[1])
    
    # Ensure the temp directory exists
    temp_dir = './temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    output_path = os.path.join(temp_dir, 'parquet_benchmarks.png')
    plot_benchmarks(data, output_path)
    print(f"Plot saved to {output_path}")
