# spaced_repetition_concat_test_with_verification.py

from collections import Counter

# Full intervals
intervals = [0, 3, 7, 14, 30, 60, 120, 240]


n = 5 # number of sentence pairs
x = 2 # ow often each sentence pair must appearin the schedule

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

# Verification function
def verify_output(output, n, x):
    """Check that output contains exactly x instances of each number 1..n"""
    counts = Counter(output)
    success = True
    for i in range(1, n + 1):
        if counts[i] != x:
            print(f"Error: Item {i} appears {counts[i]} times (expected {x})")
            success = False
    if success:
        print("Verification passed: all items appear exactly", x, "times.")

# Run verification
verify_output(concatenated, n, x)
