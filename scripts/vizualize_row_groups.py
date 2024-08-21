import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the CSV file
df = pd.read_csv('benchmark_rowgroup.csv')

# Map stats levels to readable labels
stats_map = {0: 'None', 1: 'Chunk', 2: 'Page'}
df['stats_level'] = df['stats_level'].map(stats_map)
df['page_size_mb'] = df['page_size'] / (1024 * 1024)

# Set up the plot style
sns.set_style("whitegrid")

# Plot 1: Heatmap of total decode time vs row group size and page size (in MB)
plt.figure(figsize=(12, 8))
pivot = df.pivot_table(values='total_decode_time_ms', index='row_group_size', columns='page_size_mb', aggfunc='mean')
sns.heatmap(pivot, annot=True, fmt='.2f', cmap='YlOrRd')
plt.title('Total Decode Time (ms) vs Row Group Size and Page Size')
plt.xlabel('Page Size (MB)')
plt.ylabel('Row Group Size')
plt.tight_layout()
plt.savefig('heatmap_decode_time.png')
plt.close()

# Plot 2: Box plot of total decode time for different row group sizes
plt.figure(figsize=(12, 8))
sns.boxplot(x='row_group_size', y='total_decode_time_ms', data=df)
plt.xlabel('Row Group Size')
plt.ylabel('Total Decode Time (ms)')
plt.title('Distribution of Total Decode Time for Different Row Group Sizes')
plt.tight_layout()
plt.savefig('boxplot_decode_time_row_group.png')
plt.close()

# Plot 3: File Size Analysis
plt.figure(figsize=(12, 8))
sns.barplot(x='num_columns', y='file_size_mb', hue='row_group_size', data=df)
plt.title('File Size vs Number of Columns for Different Row Group Sizes')
plt.xlabel('Number of Columns')
plt.ylabel('File Size (MB)')
plt.legend(title='Row Group Size')
plt.tight_layout()
plt.savefig('file_size_analysis.png')
plt.close()

# Plot 4: Performance Metrics Correlation Heatmap
plt.figure(figsize=(12, 10))
correlation_matrix = df[['write_time_ms', 'total_decode_time_ms', 'thrift_decode_time_ms', 
                         'schema_build_time_ms', 'stats_decode_time_ms', 'file_size_mb']].corr()
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, center=0)
plt.title('Correlation Heatmap of Performance Metrics')
plt.tight_layout()
plt.savefig('correlation_heatmap.png')
plt.close()

print("All four plots have been saved as separate PNG files.")