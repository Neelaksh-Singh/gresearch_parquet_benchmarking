import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the CSV file
df = pd.read_csv('benchmark_stats.csv')

# Convert microseconds to milliseconds
df['stats_decode_time_ms'] = df['stats_decode_time_us'] / 1000

# Convert bytes to megabytes
df['size_mb'] = df['size_bytes'] / (1024 * 1024)

# Set up the plot style
sns.set_style("whitegrid")
plt.rcParams['font.size'] = 12
plt.rcParams['axes.labelsize'] = 14
plt.rcParams['axes.titlesize'] = 16
plt.rcParams['xtick.labelsize'] = 12
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['legend.fontsize'] = 12
plt.rcParams['figure.titlesize'] = 20

# Define colors for Stats Enabled and Disabled
color_enabled = '#ff7f0e'  # Orange
color_disabled = '#1f77b4'  # Blue
palette = {True: color_enabled, False: color_disabled}

# Plot 1: File size comparison
plt.figure(figsize=(12, 8))
ax = sns.barplot(x='num_columns', y='size_mb', hue='stats_enabled', data=df, palette=palette)
plt.xlabel('Number of columns')
plt.ylabel('File size (MB)')
plt.title('File size comparison: Stats Enabled vs Disabled')
handles, labels = ax.get_legend_handles_labels()
plt.legend(handles=handles, labels=['Stats Disabled', 'Stats Enabled'])
plt.yscale('log')  # Use log scale for y-axis
for i, bar in enumerate(plt.gca().patches):
    plt.annotate(f'{bar.get_height():.2f}', 
                 (bar.get_x() + bar.get_width()/2, bar.get_height()),
                 ha='center', va='bottom', xytext=(0, 3),
                 textcoords='offset points')
plt.tight_layout()
plt.savefig('./temp/stats_file_size_comparison_log.png', dpi=300)
plt.close()

# Plot 2: Total statistics decode time
plt.figure(figsize=(12, 8))
ax = sns.barplot(x='num_columns', y='stats_decode_time_ms', hue='stats_enabled', data=df, palette=palette)
plt.xlabel('Number of columns')
plt.ylabel('Total decode time (ms)')
plt.title('Total statistics decode time')
handles, labels = ax.get_legend_handles_labels()
plt.legend(handles=handles, labels=['Stats Disabled', 'Stats Enabled'])
for i, bar in enumerate(plt.gca().patches):
    plt.annotate(f'{bar.get_height():.2f}', 
                 (bar.get_x() + bar.get_width()/2, bar.get_height()),
                 ha='center', va='bottom', xytext=(0, 3),
                 textcoords='offset points')
plt.tight_layout()
plt.savefig('./temp/stats_total_decode_time.png', dpi=300)
plt.close()

# # Plot 3: Overhead of statistics
# plt.figure(figsize=(12, 8))
# df_overhead = df[df['stats_enabled']].copy()
# df_overhead = df_overhead.merge(df[~df['stats_enabled']], on='num_columns', suffixes=('_enabled', '_disabled'))
# df_overhead['overhead_percentage'] = ((df_overhead['size_mb_enabled'] - df_overhead['size_mb_disabled']) / df_overhead['size_mb_disabled']) * 100
# ax = sns.barplot(x='num_columns', y='overhead_percentage', data=df_overhead, color=color_enabled)
# plt.xlabel('Number of columns')
# plt.ylabel('Overhead percentage')
# plt.title('Overhead of statistics (percentage increase in file size)')
# for i, bar in enumerate(plt.gca().patches):
#     plt.annotate(f'{bar.get_height():.2f}%', 
#                  (bar.get_x() + bar.get_width()/2, bar.get_height()),
#                  ha='center', va='bottom', xytext=(0, 3),
#                  textcoords='offset points')
# plt.tight_layout()
# plt.savefig('./temp/stats_overhead_percentage.png', dpi=300)
# plt.close()

# Plot 4: Decode time per column (log-log scale)
plt.figure(figsize=(12, 8))
df['time_per_column_ms'] = df['stats_decode_time_ms'] / df['num_columns']
handles = []
labels = []
for stats_enabled in df['stats_enabled'].unique():
    data = df[df['stats_enabled'] == stats_enabled]
    color = color_enabled if stats_enabled else color_disabled
    label = 'Stats Enabled' if stats_enabled else 'Stats Disabled'
    line, = plt.plot(data['num_columns'], data['time_per_column_ms'], marker='o', label=label, color=color)
    handles.append(line)
    labels.append(label)
    for _, point in data.iterrows():
        plt.annotate(f"{point['time_per_column_ms']:.4f}", 
                     (point['num_columns'], point['time_per_column_ms']),
                     textcoords="offset points", xytext=(0,10), ha='center')
plt.xscale('log')
plt.yscale('log')
plt.xlabel('Number of columns')
plt.ylabel('Time per column (ms)')
plt.title('Average statistics decode time per column (log-log scale)')
plt.legend(handles=handles, labels=labels)
plt.tight_layout()
plt.savefig('./temp/stats_decode_time_per_column_log.png', dpi=300)
plt.close()

print("All plots have been saved as separate PNG files in the ./temp/ directory.")
