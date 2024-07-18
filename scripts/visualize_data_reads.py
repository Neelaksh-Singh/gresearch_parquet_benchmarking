import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob

# Read all CSV files
csv_files = glob.glob('data_read_benchmark_*_benchmark_results.csv')
dfs = [pd.read_csv(f) for f in csv_files]
df = pd.concat(dfs, ignore_index=True)

# Set up the plot style
sns.set_style("whitegrid")

# Melt the dataframe for easier plotting
df_melted = df.melt(id_vars=['num_columns', 'num_rows'], 
                    var_name='operation', 
                    value_name='time_ms')

# Plot
plt.figure(figsize=(15, 10))
sns.barplot(x='num_columns', y='time_ms', hue='operation', data=df_melted)
plt.title('Data Read Operations Comparison')
plt.xlabel('Number of Columns')
plt.ylabel('Time (ms)')
plt.yscale('log')
plt.legend(title='Operation')
plt.tight_layout()
plt.savefig('./temp/data_read_benchmark.png')
plt.close()

print("Plot has been saved as data_read_benchmark.png")

# Print results to console
for _, row in df.iterrows():
    print(f"\nResults for {row['num_columns']} columns and {row['num_rows']} rows:")
    print(f"Metadata decode time: {row['metadata_decode_time_ms']:.2f} ms")
    print(f"Full data read time: {row['full_data_read_time_ms']:.2f} ms")
    print(f"Random column read time: {row['random_column_read_time_ms']:.2f} ms")
    print(f"Page read time: {row['page_read_time_ms']:.2f} ms")