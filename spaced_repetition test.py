# spaced_repetition_concat_test_with_verification.py

from collections import Counter
templates = {
    # Format: (Audio Sequence String, Repetitions: Number)
    
    # *Where W means Word and L means Language ad 1 and 2 mean base and target languages"
    # The 'workout' template has a sequence of "SP W2 W1 L1 L2 L2 L2 L2 L2" and 1 repetition.
    "workout": ("SP W2 W1 L1 L2 L2 L2 L2 L2", 2), 
    
    # The 'review_forward' template has a sequence of "SP W2 W1 L1 L2" and 2 repetitions.
    "review_forward": ("SP W2 W1 L1 L2", 1), 
    
    # The 'review_reverse' template has a sequence of "SP W2 W1 L2 L1" and 2 repetitions.
    "review_reverse": ("SP W2 W1 L2 L1", 1), 
}
TEMPLATE_DELIMITER: str = ' '

data = [
    {"word": "Epistle", "study_day": 3},
    {"word": "Juxtapose", "study_day": 1},
    {"word": "Verisimilitude", "study_day": 2, },
    {"word": "Cacophony", "study_day": 4},
    {"word": "Knell", "study_day": 3, },
    {"word": "Zephyr", "study_day": 1},
    {"word": "Mirth", "study_day": 2},
    {"word": "Tryst", "study_day": 4, },
    {"word": "Halcyon", "study_day": 1},
    {"word": "Ubiquitous", "study_day": 3, },
    {"word": "Serendipity", "study_day": 2},
    {"word": "Pernicious", "study_day": 4},
    {"word": "Reticent", "study_day": 1, },
    {"word": "Soliloquy", "study_day": 3},
    {"word": "Labyrinth", "study_day": 2, },
    {"word": "Vacillate", "study_day": 4},
    {"word": "Nefarious", "study_day": 1, },
    {"word": "Ephemeral", "study_day": 3},
    {"word": "Quixotic", "study_day": 2},
    {"word": "Taciturn", "study_day": 4, },
    {"word": "Ebullient", "study_day": 1},
    {"word": "Innuendo", "study_day": 3},
    {"word": "Onomatopoeia", "study_day": 2, },
    {"word": "Palimpsest", "study_day": 4},
    {"word": "Somnambulist", "study_day": 1, },
    {"word": "Wanderlust", "study_day": 3},
    {"word": "Mellifluous", "study_day": 2},
    {"word": "Pulchritude", "study_day": 4, },
    {"word": "Susurrus", "study_day": 1},
    {"word": "Lugubrious", "study_day": 3, }
]
# Full intervals
intervals = [0, 3, 7, 14, 30, 60, 120, 240]

n = len(data) # number of sentence pairs
print(f"The total count of dictionary items is: {len(data)}")
x = second_item = templates["workout"][1] # how often each sentence pair must appearin the schedule

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
# Create the schedule array based on the concatenated list
schedule = []
for word_id in concatenated:
    # word_id is 1-based (1 to 30), corresponding to the row number.
    # We subtract 1 to get the correct 0-based index for the 'data' list.
    schedule.append(data[word_id - 1])

print("\n---")
print("Generated Schedule Array:")
# Use pprint for a clean display of the list of dictionaries
import pprint
pprint.pprint(schedule)
print("---")
# Run verification
verify_output(concatenated, n, x)
