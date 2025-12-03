# spaced_repetition_final_correct_schedule.py

from collections import Counter
import pprint
import csv

templates = {
    "workout": ("SP W2 W1 L1 L2 L2 L2 L2 L2", 3),
    "review_forward": ("SP W2 W1 L1 L2", 1),
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

# --- CSV Saving Functions ---

def save_concatenated_to_csv(word_list, filename):
    """Saves a list of words to a CSV file in a single column."""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Word'])
        for word in word_list:
            writer.writerow([word])
    print(f"\nSuccessfully saved word list to **{filename}**")

# --- SPACING LOGIC FUNCTION ---
def generate_schedule(words_to_schedule, x, intervals):
    """Generates the concatenated schedule array for a given set of words."""
    if not words_to_schedule:
        return []
    
    use_intervals = intervals[:x]
    arrays = {}
    
    for item_position, word in enumerate(words_to_schedule, 1):
        indices = [item_position + use_intervals[0]]
        for i in range(1, len(use_intervals)):
            indices.append(indices[-1] + use_intervals[i])
        
        for idx in indices:
            arrays.setdefault(idx, []).append(word)

    concatenated = []
    for key in sorted(arrays):
        concatenated.extend(arrays[key])
        
    return concatenated

# --- Data Preparation ---

intervals = [0, 3, 7, 14, 30, 60, 120, 240]
x = templates["workout"][1] # Repetitions: 3

# 1. UNFILTERED Data Set
all_words_unfiltered = [item['word'] for item in data_to_review]

# 2. FILTERED Data Set
current_study_day = 0
if data_to_review:
    current_study_day = max(item['study_day'] for item in data_to_review)

# List of words for the highest study_day
words_to_schedule_filtered = [
    item['word'] for item in data_to_review if item['study_day'] == current_study_day
]
print(f"Highest study_day found: {current_study_day}. Filtered words: {len(words_to_schedule_filtered)}")


# --- Schedule Generation & Saving ---

## 🅰️ First File: Unfiltered Schedule (All 30 words)
concatenated_unfiltered = generate_schedule(all_words_unfiltered, x, intervals)
save_concatenated_to_csv(concatenated_unfiltered, filename="scheduled_unfiltered_sequence.csv")
print(f"File 1 length: {len(concatenated_unfiltered)} (30 words * 3 reps)")

## 🅱️ Second File: Filtered Schedule (Only High-Priority words)
# Run the spaced repetition logic ONLY on the small, filtered set.
concatenated_filtered = generate_schedule(words_to_schedule_filtered, x, intervals)
save_concatenated_to_csv(concatenated_filtered, filename="scheduled_filtered_sequence.csv")
print(f"File 2 length: {len(concatenated_filtered)} ({len(words_to_schedule_filtered)} words * 3 reps)")

print("\nProcessing complete. Both schedules now adhere to the spaced repetition pattern.")