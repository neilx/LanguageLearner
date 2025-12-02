# spaced_repetition_concat_test.py

# Full intervals
intervals = [0, 3, 7, 14, 30, 60, 120, 240]

# Number of items and number of appearances per item
n = 30
x = 5

# Use only the first x intervals
use_intervals = intervals[:x]

arrays = {}

# Populate the arrays dictionary
for item in range(1, n + 1):
    indices = [item + use_intervals[0]]
    for i in range(1, len(use_intervals)):
        indices.append(indices[-1] + use_intervals[i])
    for idx in indices:
        arrays.setdefault(idx, []).append(item)

# Print the arrays by index
print("Arrays by index:")
for key in sorted(arrays):
    print(f"Index {key}: {arrays[key]}")

# Concatenate all values in key order
concatenated = []
for key in sorted(arrays):
    concatenated.extend(arrays[key])

print("\nConcatenated output:")
print(concatenated)
