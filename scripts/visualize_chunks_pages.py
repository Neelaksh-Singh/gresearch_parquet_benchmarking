import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the CSV file
df = pd.read_csv('benchmark_chunks_and_pages.csv')

# Convert microseconds to milliseconds
time_columns = ['total_decode_time_us', 'thrift_decode_time_us', 'schema_build_time_us']
for col in time_columns:
    df[col.replace('_us', '_ms')] = df[col] / 1000

# Convert bytes to megabytes
df['size_mb'] = df['size_bytes'] / (1024 * 1024)

# Map stats levels to readable labels
stats_map = {0: 'None', 1: 'Chunk', 2: 'Page'}
df['stats_level'] = df['stats_level'].map(stats_map)

# Set up the plot style
sns.set_style("whitegrid")

# Plot 1: Metadata decode time
plt.figure(figsize=(12, 8))
for stats in df['stats_level'].unique():
    data = df[df['stats_level'] == stats]
    plt.plot(data['num_columns'], data['total_decode_time_ms'], marker='o', label=stats)
    for i, point in data.iterrows():
        plt.annotate(f"{point['total_decode_time_ms']:.2f}", 
                     (point['num_columns'], point['total_decode_time_ms']),
                     textcoords="offset points", xytext=(0,10), ha='center')
plt.xscale('log')
plt.yscale('log')
plt.xlabel('Number of columns')
plt.ylabel('Metadata decode time (ms)')
plt.title('Metadata decode time')
plt.legend(title='Stats level')
plt.tight_layout()
plt.savefig('./temp/metadata_decode_time.png')
plt.close()

# Plot 2: Decode time per column
plt.figure(figsize=(12, 8))
df['time_per_column_ms'] = df['total_decode_time_ms'] / df['num_columns']
sns.barplot(x='stats_level', y='time_per_column_ms', data=df)
plt.xlabel('Stats level')
plt.ylabel('Time per column (ms)')
plt.title('Average decode time per column')
for i, bar in enumerate(plt.gca().patches):
    plt.annotate(f'{bar.get_height():.4f}', 
                 (bar.get_x() + bar.get_width()/2, bar.get_height()),
                 ha='center', va='bottom', xytext=(0, 3),
                 textcoords='offset points')
plt.tight_layout()
plt.savefig('./temp/decode_time_per_column.png')
plt.close()

print("Plots have been saved as separate PNG files.")