import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob

# Read all CSV files
csv_files = glob.glob('compression_benchmark_*_compression_benchmark.csv')
dfs = [pd.read_csv(f) for f in csv_files]
df = pd.concat(dfs, ignore_index=True)

# Map algorithm numbers to names
algo_map = {0: 'Uncompressed', 1: 'Snappy', 2: 'Gzip', 3: 'Brotli', 4: 'Zstd'}
df['algorithm'] = df['algorithm'].map(algo_map)

# Set up the plot style
sns.set_style("whitegrid")

# Plot 1: Encoding time
plt.figure(figsize=(15, 10))
sns.barplot(x='num_columns', y='encoding_time_ms', hue='algorithm', data=df)
plt.title('Encoding Time by Compression Algorithm and Number of Columns')
plt.ylabel('Encoding Time (ms)')
plt.xlabel('Number of Columns')
plt.yscale('log')
plt.legend(title='Algorithm')
plt.tight_layout()
plt.savefig('./temp/encoding_time.png')
plt.close()

# Plot 2: Decoding time
plt.figure(figsize=(15, 10))
sns.barplot(x='num_columns', y='decoding_time_ms', hue='algorithm', data=df)
plt.title('Decoding Time by Compression Algorithm and Number of Columns')
plt.ylabel('Decoding Time (ms)')
plt.xlabel('Number of Columns')
plt.yscale('log')
plt.legend(title='Algorithm')
plt.tight_layout()
plt.savefig('./temp/decoding_time.png')
plt.close()

# Plot 3: Compressed size
plt.figure(figsize=(15, 10))
sns.barplot(x='num_columns', y='compressed_size_mb', hue='algorithm', data=df)
plt.title('Compressed Size by Compression Algorithm and Number of Columns')
plt.ylabel('Compressed Size (MB)')
plt.xlabel('Number of Columns')
plt.yscale('log')
plt.legend(title='Algorithm')
plt.tight_layout()
plt.savefig('./temp/compressed_size.png')
plt.close()

print("Plots have been saved as separate PNG files.")