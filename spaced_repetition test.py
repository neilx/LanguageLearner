# spaced_repetition_concat_test_with_verification.py

from collections import Counter
data = [
    {"word": "Epistle", "study_day": 3, "use_repetitions": True},
    {"word": "Juxtapose", "study_day": 1, "use_repetitions": True},
    {"word": "Verisimilitude", "study_day": 2, "use_repetitions": False},
    {"word": "Cacophony", "study_day": 4, "use_repetitions": True},
    {"word": "Knell", "study_day": 3, "use_repetitions": False},
    {"word": "Zephyr", "study_day": 1, "use_repetitions": True},
    {"word": "Mirth", "study_day": 2, "use_repetitions": True},
    {"word": "Tryst", "study_day": 4, "use_repetitions": False},
    {"word": "Halcyon", "study_day": 1, "use_repetitions": True},
    {"word": "Ubiquitous", "study_day": 3, "use_repetitions": False},
    {"word": "Serendipity", "study_day": 2, "use_repetitions": True},
    {"word": "Pernicious", "study_day": 4, "use_repetitions": True},
    {"word": "Reticent", "study_day": 1, "use_repetitions": False},
    {"word": "Soliloquy", "study_day": 3, "use_repetitions": True},
    {"word": "Labyrinth", "study_day": 2, "use_repetitions": False},
    {"word": "Vacillate", "study_day": 4, "use_repetitions": True},
    {"word": "Nefarious", "study_day": 1, "use_repetitions": False},
    {"word": "Ephemeral", "study_day": 3, "use_repetitions": True},
    {"word": "Quixotic", "study_day": 2, "use_repetitions": True},
    {"word": "Taciturn", "study_day": 4, "use_repetitions": False},
    {"word": "Ebullient", "study_day": 1, "use_repetitions": True},
    {"word": "Innuendo", "study_day": 3, "use_repetitions": True},
    {"word": "Onomatopoeia", "study_day": 2, "use_repetitions": False},
    {"word": "Palimpsest", "study_day": 4, "use_repetitions": True},
    {"word": "Somnambulist", "study_day": 1, "use_repetitions": False},
    {"word": "Wanderlust", "study_day": 3, "use_repetitions": True},
    {"word": "Mellifluous", "study_day": 2, "use_repetitions": True},
    {"word": "Pulchritude", "study_day": 4, "use_repetitions": False},
    {"word": "Susurrus", "study_day": 1, "use_repetitions": True},
    {"word": "Lugubrious", "study_day": 3, "use_repetitions": False}
]
# Full intervals
intervals = [0, 3, 7, 14, 30, 60, 120, 240]

n = len(data) # number of sentence pairs
print(f"The total count of dictionary items is: {len(data)}")
x = 6 # how often each sentence pair must appearin the schedule

# Use only the first x intervals
use_intervals = intervals[:x]

arrays = {}

# Populate the arrays dictionary
for item in range(1, len(data) + 1):
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
    for i in range(1, len(data) + 1):
        if counts[i] != x:
            print(f"Error: Item {i} appears {counts[i]} times (expected {x})")
            success = False
    if success:
        print("Verification passed: all items appear exactly", x, "times.")

# Run verification
verify_output(concatenated, n, x)
