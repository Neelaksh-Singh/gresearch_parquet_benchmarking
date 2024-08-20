#%%
import json
import matplotlib.pyplot as plt
import numpy as np

# Load the benchmark data from the test.json file
with open('test.json') as f:
    data = json.load(f)

# Initialize lists to hold times for 2000 and 3000 columns
thrift_decode_times = {'2000': 0, '3000': 0}
flatbuffer_encode_times = {'2000': 0, '3000': 0}
flatbuffer_parse_times = {'2000': 0, '3000': 0}
labels = ['2000', '3000']

# Extract relevant data
for benchmark in data['benchmarks']:
    name = benchmark['name']
    cols = name.split('/')[1]  # Get the number of columns (2000 or 3000)
    if "ParseThrift" in name:
        thrift_decode_times[cols] = benchmark['real_time']
    elif "EncodeFlatbuffer" in name:
        flatbuffer_encode_times[cols] = benchmark['real_time']
    elif "ParseFlatbuffer" in name:
        flatbuffer_parse_times[cols] = benchmark['real_time']

# Plotting
x = np.arange(len(labels))  # Label locations
width = 0.25  # Width of the bars

fig, ax = plt.subplots(figsize=(12, 8))

# Create bar charts for each category
rects1 = ax.bar(x - width, [thrift_decode_times[label] for label in labels], width, label='Thrift Decode', color='blue')
rects2 = ax.bar(x, [flatbuffer_encode_times[label] for label in labels], width, label='FlatBuffer Encode', color='green')
rects3 = ax.bar(x + width, [flatbuffer_parse_times[label] for label in labels], width, label='FlatBuffer Parse', color='orange')

# Set y-axis to logarithmic scale
ax.set_yscale('log')

# Add some text for labels, title, and custom x-axis tick labels
ax.set_xlabel('Number of Columns')
ax.set_ylabel('Time (ns) - Log Scale')
ax.set_title('Performance of Thrift Decode, FlatBuffer Encode, and Parse')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

# Function to add labels above the bars
def add_labels(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height:.0f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

add_labels(rects1)
add_labels(rects2)
add_labels(rects3)

fig.tight_layout()

plt.show()

# %%
# Initialize dictionaries to store the extracted sizes
combined_metadata_size = {}
flatbuffer_size = {}
original_metadata_size = {}

# Loop through the data and extract the relevant sizes for the given benchmarks
for benchmark in data['benchmarks']:
    if 'BM_ParseWithExtension' in benchmark['name']:
        columns = benchmark['name'].split('/')[-1]
        combined_metadata_size[columns] = benchmark['CombinedMetadataSize']
        flatbuffer_size[columns] = benchmark['FlatBufferSize']
        original_metadata_size[columns] = benchmark['OriginalMetadataSize']

# Prepare the data for plotting
labels = combined_metadata_size.keys()
combined_sizes = combined_metadata_size.values()
flatbuffer_sizes = flatbuffer_size.values()
original_sizes = original_metadata_size.values()

x = np.arange(len(labels))  # the label locations
width = 0.2  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width, combined_sizes, width, label='CombinedMetadataSize', color='blue')
rects2 = ax.bar(x, flatbuffer_sizes, width, label='FlatBufferSize', color='green')
rects3 = ax.bar(x + width, original_sizes, width, label='OriginalMetadataSize', color='orange')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Number of Columns')
ax.set_ylabel('Size (bytes)')
ax.set_title('Metadata Sizes for Thrift and FlatBuffer')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

# Add labels above the bars
def add_labels(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{int(height)}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

add_labels(rects1)
add_labels(rects2)
add_labels(rects3)

fig.tight_layout()
plt.show()
# %%
# Initialize dictionaries to store the extracted times
thrift_times_2000 = {}
flatbuffer_times_2000 = {}

thrift_times_3000 = {}
flatbuffer_times_3000 = {}

# Loop through the data and extract the relevant times for the given benchmarks
for benchmark in data['benchmarks']:
    if 'BM_ReadPartialData' in benchmark['name']:
        columns = benchmark['name'].split('/')[1]  # Number of columns
        subset_size = benchmark['name'].split('/')[-1]  # Subset size
        key = f"{subset_size}"
        if columns == "2000":
            thrift_times_2000[key] = benchmark['ThriftTime']
            flatbuffer_times_2000[key] = benchmark['FlatBufferTime']
        elif columns == "3000":
            thrift_times_3000[key] = benchmark['ThriftTime']
            flatbuffer_times_3000[key] = benchmark['FlatBufferTime']

# Function to plot the data
def plot_data(thrift_times, flatbuffer_times, title):
    labels = thrift_times.keys()
    thrift_time_values = thrift_times.values()
    flatbuffer_time_values = flatbuffer_times.values()

    x = np.arange(len(labels))  # the label locations
    width = 0.3  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, thrift_time_values, width, label='Thrift Time', color='blue')
    rects2 = ax.bar(x + width/2, flatbuffer_time_values, width, label='FlatBuffer Time', color='green')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_xlabel('Subset Size')
    ax.set_ylabel('Time (ns)')
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    # Add labels above the bars
    def add_labels(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{int(height)}',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    add_labels(rects1)
    add_labels(rects2)

    fig.tight_layout()
    plt.show()

# Plot the data for 2000 columns
plot_data(thrift_times_2000, flatbuffer_times_2000, 'Time Taken for Partial Columns Reads (2000 Columns): Thrift vs FlatBuffer')

# Plot the data for 3000 columns
plot_data(thrift_times_3000, flatbuffer_times_3000, 'Time Taken for Partial Columns Reads (3000 Columns): Thrift vs FlatBuffer')
# %%