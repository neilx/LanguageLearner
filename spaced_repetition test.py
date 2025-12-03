from collections import Counter
import pprint
import csv

# New template structure: (description, repetitions, use_filtered_data_bool)
templates = {
    # Workout uses all words (False = use filtered data)
    "workout": ("SP W2 W1 L1 L2 L2 L2 L2 L2", 3, True), 
    # Reviews use only the high-priority, current day words (True = use filtered data)
    "review_forward": ("SP W2 W1 L1 L2", 1, False), 
    "review_reverse": ("SP W2 W1 L2 L1", 1, False),
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

# Standard Spacing Intervals (Days after initial study)
intervals = [0, 3, 7, 14, 30, 60, 120, 240]

# --- CSV Saving Functions ---

def save_concatenated_to_csv(word_list, filename):
    """Saves a list of words to a CSV file in a single column."""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Word'])
        for word in word_list:
            writer.writerow([word])
    print(f"Successfully saved word list to **{filename}**")

# --- SPACING LOGIC FUNCTION ---
def generate_schedule(words_to_schedule, x, intervals):
    """
    Generates the concatenated schedule array for a given set of words,
    repeating the scheduling 'x' times based on the intervals.
    """
    if not words_to_schedule:
        return []
    
    # Use only the first 'x' intervals for scheduling repetitions
    use_intervals = intervals[:x]
    arrays = {}
    
    for item_position, word in enumerate(words_to_schedule, 1):
        # Calculate the indices (days/slots) where the word should appear
        indices = [item_position + use_intervals[0]] # Start index
        for i in range(1, len(use_intervals)):
            indices.append(indices[-1] + use_intervals[i])
        
        # Populate the arrays dictionary where key=index (day/slot), value=list of words
        for idx in indices:
            arrays.setdefault(idx, []).append(word)

    # Concatenate all lists of words in order of the indices
    concatenated = []
    for key in sorted(arrays):
        concatenated.extend(arrays[key])
        
    return concatenated

# --- Template Processing Function ---

def process_template_schedule(template_name, template_params, all_words, filtered_words):
    """
    Processes the scheduling for a single template. It selects the appropriate 
    data set based on the template definition and saves a single manifest file.
    """
    # Extract parameters from the updated template tuple
    x = template_params[1]
    use_filtered_data = template_params[2]
    
    # Determine which word list to use
    if use_filtered_data:
        words_to_use = filtered_words
        data_source = "Filtered (High-Priority)"
    else:
        words_to_use = all_words
        data_source = "Unfiltered (All Data)"
        
    # Define the final filename using only the template name and manifest suffix
    filename = f"{template_name}_manifest.csv"
    
    print(f"\n--- Processing Template: '{template_name}' (Reps: {x}) ---")
    print(f"Data Source: {data_source} ({len(words_to_use)} unique words)")

    # 1. Generate Schedule
    concatenated_schedule = generate_schedule(words_to_use, x, intervals)
    
    # 2. Save the Single Manifest File
    save_concatenated_to_csv(concatenated_schedule, filename=filename)
    
    print(f"File '{filename}' length: {len(concatenated_schedule)} ({len(words_to_use)} words * {x} reps)")


# --- Main Execution ---

# --- Data Preparation (Run once for all templates) ---
# 1. UNFILTERED Data Set
all_words_unfiltered = [item['word'] for item in data_to_review]

# 2. FILTERED Data Set: Find words with the highest study_day
current_study_day = 0
if data_to_review:
    current_study_day = max(item['study_day'] for item in data_to_review)

words_to_schedule_filtered = [
    item['word'] for item in data_to_review if item['study_day'] == current_study_day
]
print(f"Total words in data set: {len(all_words_unfiltered)}")
print(f"Highest study_day found: {current_study_day}. Filtered words to schedule: {len(words_to_schedule_filtered)}")


# --- Schedule Generation & Saving for ALL Templates ---
for name, params in templates.items():
    # Pass both lists to the processing function, which decides which one to use
    process_template_schedule(name, params, all_words_unfiltered, words_to_schedule_filtered)

print("\n--- All template processing complete. Check the generated CSV files. ---")