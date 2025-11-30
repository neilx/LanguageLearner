import os
import csv
import json
from pathlib import Path

# =========================================================================
# Configuration Constants
# =========================================================================

# The master CSV file containing all sentence pairs and initial scheduling
MASTER_DATA_FILE = Path('sentence_pairs.csv')
# The directory where all generated files (CSV, MP3s) are stored
OUTPUT_DIR = Path('output')
# The directory for storing cached API responses (e.g., audio segments)
CACHE_DIR = Path('cache')

# Define the required output files for declarative completeness checking
REQUIRED_OUTPUT_FILES = [
    'schedule.csv', 
    'forward.mp3', 
    'review.mp3', 
    'reverse.mp3'
]

# --- Scheduling Parameters ---
# The number of repetitions for new words within the same day (micro-spacing)
NEW_WORD_REPETITIONS = 3
# The column headers required for the final schedule.csv manifest
MANIFEST_COLUMNS = ['W2', 'W1', 'L1', 'L2']

# --- Macro-Spacing Intervals (in study days) ---
# Maps an initial study day (D) to the days it should be reviewed (D + interval).
# Note: For this 5-day dataset, we will only use a 1-day and a 3-day interval
MACRO_SRS_INTERVALS = [1, 3] # Review 1 day later, then 3 days later


# =========================================================================
# Core Data Structures and Utilities
# =========================================================================

def initialize_master_data():
    """
    Creates the sentence_pairs.csv file if it does not exist, 
    populating it with the required 5-day dummy data for testing.
    """
    if MASTER_DATA_FILE.exists():
        print(f"Master data found at {MASTER_DATA_FILE}. Skipping creation.")
        return

    print("Master data not found. Creating sentence_pairs.csv with mock data...")
    
    # Mock data structured to cover 5 days of learning and Macro-Spacing intervals
    mock_data = [
        # Day 1 (5 items)
        {'W2': 'sol', 'W1': 'sun', 'L1': 'Solen skinner i dag.', 'L2': 'The sun is shining today.', 'StudyDay': 1},
        {'W2': 'måne', 'W1': 'moon', 'L1': 'Månen er smuk i aften.', 'L2': 'The moon is beautiful tonight.', 'StudyDay': 1},
        {'W2': 'vand', 'W1': 'water', 'L1': 'Jeg skal have noget vand.', 'L2': 'I need some water.', 'StudyDay': 1},
        {'W2': 'jord', 'W1': 'earth', 'L1': 'Jorden drejer rundt.', 'L2': 'The earth is turning.', 'StudyDay': 1},
        {'W2': 'ild', 'W1': 'fire', 'L1': 'Hold dig væk fra ilden.', 'L2': 'Stay away from the fire.', 'StudyDay': 1},
        
        # Day 2 (6 items)
        {'W2': 'tryghed', 'W1': 'security', 'L1': 'Vi søger tryghed.', 'L2': 'We seek security.', 'StudyDay': 2},
        {'W2': 'akkord', 'W1': 'chord', 'L1': 'Han spiller en akkord.', 'L2': 'He plays a chord.', 'StudyDay': 2},
        {'W2': 'mangle', 'W1': 'lack', 'L1': 'Vi mangler tid.', 'L2': 'We lack time.', 'StudyDay': 2},
        {'W2': 'tilpasning', 'W1': 'adaptation', 'L1': 'Det kræver tilpasning.', 'L2': 'It requires adaptation.', 'StudyDay': 2},
        {'W2': 'indkøb', 'W1': 'purchase', 'L1': 'Jeg skal lave indkøb.', 'L2': 'I need to make purchases.', 'StudyDay': 2},
        {'W2': 'mæt', 'W1': 'full (satiated)', 'L1': 'Jeg er mæt nu.', 'L2': 'I am full now.', 'StudyDay': 2},
        
        # Day 3 (5 items)
        {'W2': 'lys', 'W1': 'light', 'L1': 'Der er lys for enden af tunnelen.', 'L2': 'There is light at the end of the tunnel.', 'StudyDay': 3},
        {'W2': 'mørke', 'W1': 'darkness', 'L1': 'Mørket faldt på.', 'L2': 'The darkness fell.', 'StudyDay': 3},
        {'W2': 'himmel', 'W1': 'sky', 'L1': 'Himlen er blå i dag.', 'L2': 'The sky is blue today.', 'StudyDay': 3},
        {'W2': 'storm', 'W1': 'storm', 'L1': 'Der kommer en storm.', 'L2': 'A storm is coming.', 'StudyDay': 3},
        {'W2': 'vind', 'W1': 'wind', 'L1': 'Vinden blæser kraftigt.', 'L2': 'The wind is blowing strongly.', 'StudyDay': 3},

        # Day 4 (7 items)
        {'W2': 'tilfældighed', 'W1': 'coincidence', 'L1': 'Det var en tilfældighed.', 'L2': 'It was a coincidence.', 'StudyDay': 4},
        {'W2': 'udfordring', 'W1': 'challenge', 'L1': 'Jeg accepterer udfordringen.', 'L2': 'I accept the challenge.', 'StudyDay': 4},
        {'W2': 'aftale', 'W1': 'agreement', 'L1': 'Vi har en aftale.', 'L2': 'We have an agreement.', 'StudyDay': 4},
        {'W2': 'ønske', 'W1': 'wish', 'L1': 'Jeg har et ønske.', 'L2': 'I have a wish.', 'StudyDay': 4},
        {'W2': 'rolig', 'W1': 'calm', 'L1': 'Hold dig rolig.', 'L2': 'Keep calm.', 'StudyDay': 4},
        {'W2': 'frasering', 'W1': 'phrasing', 'L1': 'Fraseringen var perfekt.', 'L2': 'The phrasing was perfect.', 'StudyDay': 4},
        {'W2': 'smør', 'W1': 'butter', 'L1': 'Der skal smør på brødet.', 'L2': 'There must be butter on the bread.', 'StudyDay': 4},

        # Day 5 (7 items)
        {'W2': 'tålmodighed', 'W1': 'patience', 'L1': 'Hav tålmodighed med mig.', 'L2': 'Have patience with me.', 'StudyDay': 5},
        {'W2': 'sprog', 'W1': 'language', 'L1': 'Dansk er et smukt sprog.', 'L2': 'Danish is a beautiful language.', 'StudyDay': 5},
        {'W2': 'regn', 'W1': 'rain', 'L1': 'Det regner udenfor.', 'L2': 'It is raining outside.', 'StudyDay': 5},
        {'W2': 'sne', 'W1': 'snow', 'L1': 'Der faldt sne i nat.', 'L2': 'It snowed last night.', 'StudyDay': 5},
        {'W2': 'vejr', 'W1': 'weather', 'L1': 'Hvad er vejret i dag?', 'L2': 'What is the weather today?', 'StudyDay': 5},
        {'W2': 'kaffe', 'W1': 'coffee', 'L1': 'Vil du have en kop kaffe?', 'L2': 'Would you like a cup of coffee?', 'StudyDay': 5},
        {'W2': 'bog', 'W1': 'book', 'L1': 'Jeg læser en god bog.', 'L2': 'I am reading a good book.', 'StudyDay': 5},
    ]

    fieldnames = ['W2', 'W1', 'L1', 'L2', 'StudyDay']
    
    try:
        with open(MASTER_DATA_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(mock_data)
        print("✅ sentence_pairs.csv created successfully.")
    except Exception as e:
        print(f"Error creating master data file: {e}")


def load_master_data():
    """Loads and validates the initial master sentence pairs CSV."""
    if not MASTER_DATA_FILE.exists():
        print(f"Error: Master data file not found at {MASTER_DATA_FILE}")
        return None, 0
    
    try:
        with open(MASTER_DATA_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)

        # Basic validation
        required_headers = MANIFEST_COLUMNS + ['StudyDay']
        if not all(h in reader.fieldnames for h in required_headers):
            print(f"Error: Master CSV must contain headers: {required_headers}")
            return None, 0

        print("✅ Master data input schema validated.")
        
        # Determine the total number of study days from the data
        max_day = max(int(item['StudyDay']) for item in data) if data else 0
        
        return data, max_day
    
    except Exception as e:
        print(f"Error loading master data: {e}")
        return None, 0

def check_declarative_completeness(day: int) -> bool:
    """Checks if all required output files for a given study day already exist."""
    day_path = OUTPUT_DIR / f"day_{day}"
    if not day_path.exists():
        return False
    
    # Check if all 4 required files are present
    for filename in REQUIRED_OUTPUT_FILES:
        if not (day_path / filename).exists():
            return False
            
    return True

# =========================================================================
# Scheduling and Aggregation Logic
# =========================================================================

def generate_schedules(master_data: list, max_day: int) -> dict:
    """
    Generates the full schedule for all days in a single, in-memory pass, 
    applying macro-spacing logic.
    """
    
    # 1. Initialize data structures
    schedules = {}
    history = [] # History is now purely transient (in-memory)

    for current_day in range(1, max_day + 1):
        
        # --- 1. MACRO-SPACING: Aggregate Due Review Items ---
        
        # Find all items from previous days that are due for review on current_day
        due_review_items = []
        
        # Iterate through the entire history to check for overdue items
        for item in history:
            original_study_day = item['StudyDay']
            
            # Check if any macro-spacing interval applies
            for interval in MACRO_SRS_INTERVALS:
                if original_study_day + interval == current_day and item['type'] == 'new':
                    # Add the original, unique item (not the repeated micro-spacing version)
                    unique_key = (item['W2'], item['StudyDay'])
                    
                    # Prevent duplicate reviews if an item satisfies multiple intervals (rare, but good practice)
                    if not any(key == unique_key for key in [(d['W2'], d['StudyDay']) for d in due_review_items]):
                        review_item = {
                            'W2': item['W2'], 
                            'W1': item['W1'], 
                            'L1': item['L1'], 
                            'L2': item['L2'],
                            'StudyDay': original_study_day, # The day it was originally learned
                            'type': 'review',
                            'repetition': 0 # Reviews don't need a repetition count
                        }
                        due_review_items.append(review_item)
                        break # Item is scheduled, move to the next item in history

        # --- 2. NEW CONTENT: Find Today's New Items ---
        
        new_items = [
            item for item in master_data 
            if int(item['StudyDay']) == current_day
        ]
        
        # --- 3. MICRO-SPACING: Apply Repetition to New Items ---
        
        micro_srs_schedule = []
        for item in new_items:
            for rep in range(1, NEW_WORD_REPETITIONS + 1):
                micro_srs_schedule.append({
                    'W2': item['W2'],
                    'W1': item['W1'],
                    'L1': item['L1'],
                    'L2': item['L2'],
                    'StudyDay': current_day,
                    'type': 'new',
                    'repetition': rep
                })
        
        # --- 4. FINAL AGGREGATION ---
        
        # Combine the review items and the micro-srs schedule
        schedules[current_day] = due_review_items + micro_srs_schedule
        
        # --- 5. UPDATE HISTORY ---
        
        # Add the unique new item to the in-memory history for future days' Macro-spacing checks
        for item in new_items:
             history.append({
                    'W2': item['W2'],
                    'W1': item['W1'],
                    'L1': item['L1'],
                    'L2': item['L2'],
                    'StudyDay': current_day,
                    'type': 'new',
                    'repetition': 1 
             })

    return schedules

# =========================================================================
# File Writing and Main Execution
# =========================================================================

def write_day_files(day: int, schedule: list):
    """
    Creates the output directory and writes the schedule.csv file.
    Also prints a summary of the schedule to the console.
    """
    
    day_path = OUTPUT_DIR / f"day_{day}"
    day_path.mkdir(parents=True, exist_ok=True)
    
    # 1. Write schedule.csv (Full SRS Data)
    schedule_path = day_path / 'schedule.csv'
    
    # The fieldnames match the combined structure (SRS columns)
    fieldnames = ['sequence'] + MANIFEST_COLUMNS + ['StudyDay', 'type', 'repetition']
    
    with open(schedule_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, item in enumerate(schedule):
            row = {'sequence': i + 1}
            row.update(item)
            # Ensure the structure matches the fieldnames (even if some fields are empty/zero)
            writer.writerow(row)

    # Calculate summary for printing and verification
    new_count = len([item for item in schedule if item['type'] == 'new'])
    review_items = [item for item in schedule if item['type'] == 'review']
    review_count = len(review_items)
    
    # Calculate review sources (for verification/scenario print)
    review_source_days = {}
    for item in review_items:
        day_learned = item['StudyDay']
        review_source_days[day_learned] = review_source_days.get(day_learned, 0) + 1
            
    print(f"  > Schedule Summary for Day {day}:")
    print(f"    - Total items: {len(schedule)}")
    print(f"    - New items (Micro-Spacing): {new_count}")
    print(f"    - Review items (Macro-Spacing): {review_count}")

    if review_source_days:
        print("    - Macro-Spacing Review Sources:")
        for source_day, count in sorted(review_source_days.items()):
            print(f"      - {count} items originally learned on Day {source_day}.")
            
    print(f"  - Wrote {schedule_path.name} ({len(schedule)} items)")

    # 2. Placeholder for MP3 files
    # In a real app, TTS would be called here to generate audio from L1/L2 columns.
    for filename in REQUIRED_OUTPUT_FILES[1:]: # Skip schedule.csv
        mp3_path = day_path / filename
        # Create an empty file to satisfy the declarative completeness check
        try:
            mp3_path.touch(exist_ok=True)
        except Exception as e:
            print(f"Error creating placeholder file {mp3_path}: {e}")

    print(f"  - Created {len(REQUIRED_OUTPUT_FILES) - 1} placeholder audio files.")


def main():
    print("## Language Learner Workflow vv0.11 Initialized (Enhanced Detailed Logging) ##")
    
    # --- Ensure critical directories exist before proceeding ---
    OUTPUT_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_exist=True) 

    # --- 0. Initialize Master Data if missing ---
    initialize_master_data()
    
    # 1. Load data
    master_data, max_day = load_master_data()
    if not master_data:
        return

    print(f"Master data loaded with {len(master_data)} items over {max_day} days.")
    
    # 2. Determine processing scope (check if any work needs to be done)
    days_to_process = []
    
    for day in range(1, max_day + 1):
        if not check_declarative_completeness(day):
            days_to_process.append(day)
            
    if not days_to_process:
        print("All days are declaratively complete. Workflow skipped.")
        return

    print(f"Days identified for processing: {days_to_process}")
    
    # 3. Generate all schedules (History is calculated in memory from Day 1 to max_day)
    schedules = generate_schedules(master_data, max_day)

    # 4. Process each identified day
    for day in days_to_process:
        schedule = schedules.get(day)
        if schedule is not None:
            write_day_files(day, schedule)
            print(f"  ✅ Day {day} processed successfully.")
        else:
            print(f"  ❌ Error: Schedule data missing for Day {day}.")


if __name__ == "__main__":
    main()