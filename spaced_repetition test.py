# spaced_repetition_concat_filtered.py

from collections import Counter
import pprint

templates = {
    # Format: (Audio Sequence String, Repetitions: Number)

    # *Where W means Word and L means Language ad 1 and 2 mean base and target languages"
    # The 'workout' template has a sequence of "SP W2 W1 L1 L2 L2 L2 L2 L2" and 2 repetitions.
    "workout": ("SP W2 W1 L1 L2 L2 L2 L2 L2", 2),
    
    # The 'review_forward' template has a sequence of "SP W2 W1 L1 L2" and 1 repetition.
    "review_forward": ("SP W2 W1 L1 L2", 1),
    
    # The 'review_reverse' template has a sequence of "SP W2 W1 L2 L1" and 1 repetition.
    "review_reverse": ("SP W2 W1 L2 L1", 1),
}
TEMPLATE_DELIMITER: str = ' '

data_to_review = [
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

# --- MODIFICATION START ---

# 1. Find the highest study_day
highest_study_day = 0
if data_to_review:
    # Use max() with a key for efficiency
    highest_study_day = max(item['study_day'] for item in data_to_review)

print(f"Highest study_day found: {highest_study_day}")

# 2. Filter the data_to_review list based on the highest study_day
# This new list replaces the original data_to_review for scheduling
filtered_data_to_review = [
    item for item in data_to_review if item['study_day'] == highest_study_day
]

print(f"Number of items filtered with study_day {highest_study_day}: {len(filtered_data_to_review)}")
print("Filtered words:")
print([item['word'] for item in filtered_data_to_review])

# Use the filtered data for all subsequent steps
data_lookup = {item["word"]: item for item in filtered_data_to_review}
all_words = list(data_lookup.keys()) # All unique word keys from the filtered set
n = len(all_words) # number of unique words/items in the filtered set

# --- MODIFICATION END ---

# Full intervals (unchanged)
intervals = [0, 3, 7, 14, 30, 60, 120, 240]

print(f"\nThe total count of dictionary items being scheduled is: {n}")
x = templates["workout"][1] # how often each word must appear in the schedule (x=2)

# Use only the first x intervals
use_intervals = intervals[:x] # [0, 3]

arrays = {}

# Populate the arrays dictionary using the word key instead of the index
# We iterate over the words and their 1-based position (item_position)
for item_position, word in enumerate(all_words, 1):
    # The scheduling logic (interval application) remains based on the 1-based position
    indices = [item_position + use_intervals[0]]
    for i in range(1, len(use_intervals)):
        indices.append(indices[-1] + use_intervals[i])
    
    # Store the actual word string in the arrays dictionary
    for idx in indices:
        arrays.setdefault(idx, []).append(word)

# Print the arrays by index
print("\nArrays by index (now containing words from filtered set):")
for key in sorted(arrays):
    print(f"Index {key}: {arrays[key]}")

# Concatenate all values in key order
concatenated = []
for key in sorted(arrays):
    concatenated.extend(arrays[key])

print("\nConcatenated output (words filtered by highest study_day):")
print(concatenated)

# Verification function now checks for word count
def verify_output(output, all_words, x):
    """Check that output contains exactly x instances of each word in all_words."""
    counts = Counter(output)
    success = True
    
    for word in all_words:
        if counts[word] != x:
            print(f"Error: Word '{word}' appears {counts[word]} times (expected {x})")
            success = False
    
    if success:
        print("Verification passed: all items appear exactly", x, "times.")

# Create the schedule array based on the concatenated list
schedule = []
for word_key in concatenated:
    # Use the word key to look up the original dictionary item
    schedule.append(data_lookup[word_key])

print("\n---")
print("Generated Schedule Array (Filtered Data Only):")
pprint.pprint(schedule)
print("---")

# Run verification
verify_output(concatenated, all_words, x)