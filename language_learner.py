import os
import csv
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Tuple

# =========================================================================
# 1. Configuration Constants
# =========================================================================

class Config:
    """Consolidated configuration settings for the Language Learning workflow."""

    # --- File Paths ---
    SOURCE_FILE: Path = Path('sentence_pairs.csv')
    OUTPUT_ROOT_DIR: Path = Path('Days')  # UPDATED: Renamed to 'Days'
    TTS_CACHE_DIR: Path = Path('tts_cache')
    
    # --- Manifest & File Names ---
    REVIEW_MANIFEST_NAME: str = 'review_manifest.csv'
    WORKOUT_MANIFEST_NAME: str = 'workout_manifest.csv'

    # --- Audio File Names ---
    REVIEW_FORWARD_AUDIO_NAME: str = 'review_forward.mp3'
    REVIEW_REVERSE_AUDIO_NAME: str = 'review_reverse.mp3'
    WORKOUT_AUDIO_NAME: str = 'workout.mp3'


    # --- Language & Localization Parameters ---
    TARGET_LANG_CODE: str = 'da-DK'
    BASE_LANG_CODE: str = 'en-US'
    
    # --- Repetition Parameters ---
    MICRO_REPETITIONS_COUNT: int = 3
    REVIEW_REPETITION_COUNT: int = 0
    MACRO_REPETITION_INTERVALS: List[int] = [1, 3, 7, 14, 30, 16, 120, 240]

    # --- File Schema & Declarative Completeness (5 files total) ---
    MANIFEST_COLUMNS: List[str] = ['W2', 'W1', 'L1', 'L2']

    REQUIRED_OUTPUT_FILES: List[str] = [
        REVIEW_MANIFEST_NAME,
        WORKOUT_MANIFEST_NAME,
        REVIEW_FORWARD_AUDIO_NAME,
        REVIEW_REVERSE_AUDIO_NAME,
        WORKOUT_AUDIO_NAME
    ]

# =========================================================================
# 2. Mock Text-to-Speech API
# =========================================================================

def mock_google_tts(text: str, language_code: str, output_path: Path) -> Path:
    """
    Mocks the Google Text-to-Speech API call.
    Saves input parameters to a JSON file in the cache and creates an empty 
    MP3 file to satisfy the declarative check.
    """
    content_hash = hashlib.sha256(f"{text}{language_code}".encode()).hexdigest()
    mock_file_path = Config.TTS_CACHE_DIR / f"{content_hash}.json"

    if not mock_file_path.exists():
        try:
            with open(mock_file_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'text': text,
                    'lang': language_code,
                    'mocked_as_output': output_path.name
                }, f, indent=4)
        except Exception as e:
            print(f"    - TTS Mock Error writing cache: {e}")

    try:
        output_path.touch(exist_ok=True)
    except Exception as e:
        print(f"    - TTS Mock Error creating output file: {e}")
        
    return output_path

# =========================================================================
# 3. Data Initialization and Loading Utilities
# =========================================================================
# NOTE: The data loading functions remain unchanged
def initialize_source_data() -> None:
    if Config.SOURCE_FILE.exists():
        print(f"Master data found at {Config.SOURCE_FILE}. Skipping creation.")
        return

    print("Master data not found. Creating sentence_pairs.csv with mock data...")
    mock_data = [
        {'W2': 'sol', 'W1': 'sun', 'L1': 'Solen skinner i dag.', 'L2': 'The sun is shining today.', 'StudyDay': 1},
        {'W2': 'måne', 'W1': 'moon', 'L1': 'Månen er smuk i aften.', 'L2': 'The moon is beautiful tonight.', 'StudyDay': 1},
        {'W2': 'vand', 'W1': 'water', 'L1': 'Jeg skal have noget vand.', 'L2': 'I need some water.', 'StudyDay': 1},
        {'W2': 'jord', 'W1': 'earth', 'L1': 'Jorden drejer rundt.', 'L2': 'The earth is turning.', 'StudyDay': 1},
        {'W2': 'ild', 'W1': 'fire', 'L1': 'Hold dig VÆK fra ilden.', 'L2': 'Stay away from the fire.', 'StudyDay': 1},
        {'W2': 'tryghed', 'W1': 'security', 'L1': 'Vi søger tryghed.', 'L2': 'We seek security.', 'StudyDay': 2},
        {'W2': 'akkord', 'W1': 'chord', 'L1': 'Han spiller en akkord.', 'L2': 'He plays a chord.', 'StudyDay': 2},
        {'W2': 'mangle', 'W1': 'lack', 'L1': 'Vi mangler tid.', 'L2': 'Vi lack time.', 'StudyDay': 2},
        {'W2': 'tilpasning', 'W1': 'adaptation', 'L1': 'Det kræver tilpasning.', 'L2': 'It requires adaptation.', 'StudyDay': 2},
        {'W2': 'indkøb', 'W1': 'purchase', 'L1': 'Jeg skal lave indkøb.', 'L2': 'I need to make purchases.', 'StudyDay': 2},
        {'W2': 'mæt', 'W1': 'full (satiated)', 'L1': 'Jeg er mæt nu.', 'L2': 'I am full now.', 'StudyDay': 2},
        {'W2': 'lys', 'W1': 'light', 'L1': 'Der er lys for enden af tunnelen.', 'L2': 'There is light at the end of the tunnel.', 'StudyDay': 3},
        {'W2': 'mørke', 'W1': 'darkness', 'L1': 'Mørket faldt på.', 'L2': 'The darkness fell.', 'StudyDay': 3},
        {'W2': 'himmel', 'W1': 'sky', 'L1': 'Himlen er blå i dag.', 'L2': 'The sky is blue today.', 'StudyDay': 3},
        {'W2': 'storm', 'W1': 'storm', 'L1': 'Der kommer en storm.', 'L2': 'A storm is coming.', 'StudyDay': 3},
        {'W2': 'vind', 'W1': 'wind', 'L1': 'Vinden blæser kraftigt.', 'L2': 'The wind is blowing strongly.', 'StudyDay': 3},
        {'W2': 'tilfældighed', 'W1': 'coincidence', 'L1': 'Det var en tilfældighed.', 'L2': 'It was a coincidence.', 'StudyDay': 4},
        {'W2': 'udfordring', 'W1': 'challenge', 'L1': 'Jeg accepterer udfordringen.', 'L2': 'I accept the challenge.', 'StudyDay': 4},
        {'W2': 'aftale', 'W1': 'agreement', 'L1': 'Vi har en aftale.', 'L2': 'We have an agreement.', 'StudyDay': 4},
        {'W2': 'ønske', 'W1': 'wish', 'L1': 'Jeg har et ønske.', 'L2': 'I have a wish.', 'StudyDay': 4},
        {'W2': 'rolig', 'W1': 'calm', 'L1': 'Hold dig rolig.', 'L2': 'Keep calm.', 'StudyDay': 4},
        {'W2': 'frasering', 'W1': 'phrasing', 'L1': 'Fraseringen var perfekt.', 'L2': 'The phrasing was perfect.', 'StudyDay': 4},
        {'W2': 'smør', 'W1': 'butter', 'L1': 'Der skal smør på brødet.', 'L2': 'There must be butter on the bread.', 'StudyDay': 4},
        {'W2': 'tålmodighed', 'W1': 'patience', 'L1': 'Hav tålmodighed med mig.', 'L2': 'Have patience with me.', 'StudyDay': 5},
        {'W2': 'sprog', 'W1': 'language', 'L1': 'Dansk er et smukt sprog.', 'L2': 'Danish is a beautiful language.', 'StudyDay': 5},
        {'W2': 'regn', 'W1': 'rain', 'L1': 'Det regner udenfor.', 'L2': 'It is raining outside.', 'StudyDay': 5},
        {'W2': 'sne', 'W1': 'snow', 'L1': 'Der faldt sne i nat.', 'L2': 'It snowed last night.', 'StudyDay': 5},
        {'W2': 'vejr', 'W1': 'weather', 'L1': 'Hvad er vejret i dag?', 'L2': 'What is the weather today?', 'StudyDay': 5},
        {'W2': 'kaffe', 'W1': 'coffee', 'L1': 'Vil du have en kop kaffe?', 'L2': 'Would you like a cup of coffee?', 'StudyDay': 5},
        {'W2': 'bog', 'W1': 'book', 'L1': 'Jeg læser en god bog.', 'L2': 'I am reading a good book.', 'StudyDay': 5},
    ]

    fieldnames = Config.MANIFEST_COLUMNS + ['StudyDay']

    try:
        with open(Config.SOURCE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([{k: str(v) for k, v in item.items()} for item in mock_data])
        print("✅ sentence_pairs.csv created successfully.")
    except Exception as e:
        print(f"❌ Error creating master data file: {e}")

def load_and_validate_source_data() -> Tuple[List[Dict[str, Any]], int]:
    if not Config.SOURCE_FILE.exists():
        print(f"❌ Error: Master data file not found at {Config.SOURCE_FILE}")
        return [], 0
    try:
        with open(Config.SOURCE_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        required_headers = Config.MANIFEST_COLUMNS + ['StudyDay']
        if not all(h in reader.fieldnames for h in required_headers):
            print(f"❌ Error: Master CSV must contain headers: {required_headers}")
            return [], 0
        for item in data:
            item['StudyDay'] = int(item['StudyDay'])
        print("✅ Master data input schema validated.")
        max_day = max(item['StudyDay'] for item in data) if data else 0
        return data, max_day
    except Exception as e:
        print(f"❌ Error loading master data: {e}")
        return [], 0
    
def is_day_complete(day: int) -> bool:
    """Checks if all required output files (5 total) for a given study day already exist."""
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{day}"
    if not day_path.exists():
        return False
    for filename in Config.REQUIRED_OUTPUT_FILES:
        if not (day_path / filename).exists():
            return False
    return True

# =========================================================================
# 4. Scheduling Logic
# =========================================================================
# NOTE: This function remains unchanged as the scheduling logic is correct
def generate_full_repetition_schedule(master_data: List[Dict[str, Any]], max_day: int) -> Dict[int, List[Dict[str, Any]]]:
    schedules: Dict[int, List[Dict[str, Any]]] = {}
    history: List[Dict[str, Any]] = []

    for current_day in range(1, max_day + 1):
        due_review_items: List[Dict[str, Any]] = []
        reviewed_keys = set()
        
        for item in history:
            original_study_day = item['StudyDay']
            for interval in Config.MACRO_REPETITION_INTERVALS:
                if original_study_day + interval == current_day:
                    unique_key = (item['W2'], original_study_day)
                    if unique_key not in reviewed_keys:
                        review_item = {
                            'W2': item['W2'], 'W1': item['W1'], 'L1': item['L1'], 'L2': item['L2'],
                            'StudyDay': original_study_day,
                            'type': 'macro_review',
                            'repetition': Config.REVIEW_REPETITION_COUNT
                        }
                        due_review_items.append(review_item)
                        reviewed_keys.add(unique_key)
                        break

        new_items = [item for item in master_data if item['StudyDay'] == current_day]

        micro_repetition_schedule: List[Dict[str, Any]] = []
        for item in new_items:
            for rep in range(1, Config.MICRO_REPETITIONS_COUNT + 1):
                micro_repetition_schedule.append({
                    'W2': item['W2'], 'W1': item['W1'], 'L1': item['L1'], 'L2': item['L2'],
                    'StudyDay': current_day,
                    'type': 'micro_new',
                    'repetition': rep
                })

        schedules[current_day] = due_review_items + micro_repetition_schedule
        for item in new_items:
             history.append(item)
    return schedules

# =========================================================================
# 5. File Writing and Daily Processing
# =========================================================================

def write_manifest_csv(day_path: Path, filename: str, schedule_data: List[Dict[str, Any]], fieldnames: List[str]) -> bool:
    """Helper function to write schedule data to a CSV file."""
    schedule_path = day_path / filename
    
    try:
        with open(schedule_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, item in enumerate(schedule_data):
                row = {'sequence': i + 1}
                row.update(item)
                row['StudyDay'] = str(row['StudyDay'])
                row['repetition'] = str(row['repetition'])
                writer.writerow(row)
        
        print(f"  - Wrote {filename} ({len(schedule_data)} items)")
        return True
    except Exception as e:
        print(f"  ❌ Error writing {filename}: {e}")
        return False

def process_day(day: int, full_schedule: List[Dict[str, Any]]):
    """
    Creates manifests and mocks audio generation based on the full and workout schedules.
    """

    day_path = Config.OUTPUT_ROOT_DIR / f"day_{day}"
    day_path.mkdir(parents=True, exist_ok=True)
    
    print(f"--- Processing Day {day} ---")
    
    # --- 1. Filter and Write Manifests ---
    
    fieldnames = ['sequence'] + Config.MANIFEST_COLUMNS + ['StudyDay', 'type', 'repetition']
    
    # a) Review Manifest (New + Review) 
    write_manifest_csv(day_path, Config.REVIEW_MANIFEST_NAME, full_schedule, fieldnames)
    
    # b) Workout Manifest (New Only) 
    workout_schedule = [item for item in full_schedule if item['type'] == 'micro_new']
    write_manifest_csv(day_path, Config.WORKOUT_MANIFEST_NAME, workout_schedule, fieldnames)


    # --- 2. Mock TTS Audio Generation ---
    print("  - Mocking TTS Audio Generation...")
    
    # Define paths using the new names
    forward_path = day_path / Config.REVIEW_FORWARD_AUDIO_NAME
    reverse_path = day_path / Config.REVIEW_REVERSE_AUDIO_NAME
    workout_path = day_path / Config.WORKOUT_AUDIO_NAME

    # a) Generate REVIEW_FORWARD.MP3 (L2 Sentence)
    print(f"  - Generating {Config.REVIEW_FORWARD_AUDIO_NAME} (L2 Sentence)...")
    for item in full_schedule:
        mock_google_tts(text=item['L2'], language_code=Config.TARGET_LANG_CODE, output_path=forward_path)
    
    # b) Generate REVIEW_REVERSE.MP3 (L2, L1 Sentences)
    print(f"  - Generating {Config.REVIEW_REVERSE_AUDIO_NAME} (L2, L1 Sentences)...")
    for item in full_schedule:
        # 1. L2 Sentence (Prompt)
        mock_google_tts(text=item['L2'], language_code=Config.TARGET_LANG_CODE, output_path=reverse_path)
        # 2. L1 Sentence (Answer/Confirmation)
        mock_google_tts(text=item['L1'], language_code=Config.BASE_LANG_CODE, output_path=reverse_path)
    
    # c) Generate WORKOUT.MP3 (L2 Sentence - New items ONLY)
    print(f"  - Generating {Config.WORKOUT_AUDIO_NAME} (L2 Sentence - Workout)...")
    for item in workout_schedule:
        mock_google_tts(text=item['L2'], language_code=Config.TARGET_LANG_CODE, output_path=workout_path)


    # --- 3. Print Summary ---
    new_count = len(workout_schedule) 
    review_items = [item for item in full_schedule if item['type'] == 'macro_review']
    review_count = len(review_items)

    review_source_days = {}
    for item in review_items:
        day_learned = item['StudyDay']
        review_source_days[day_learned] = review_source_days.get(day_learned, 0) + 1

    print("\n  > Schedule Summary:")
    print(f"    - Total items ({Config.REVIEW_MANIFEST_NAME}): {len(full_schedule)}")
    print(f"    - New items ({Config.WORKOUT_MANIFEST_NAME}): {new_count}")
    print(f"    - Review items (Macro-Spaced Repetition): {review_count}")

    if review_source_days:
        print("    - Macro-Spaced Repetition Review Sources:")
        for source_day, count in sorted(review_source_days.items()):
            print(f"      - {count} items originally learned on Day {source_day}.")


# =========================================================================
# 6. Main Execution Block
# =========================================================================

def main_workflow():
    """Main function to run the language learning generation workflow."""
    print("## 📚 Language Learner Schedule Generator (v1.0) ##")

    # --- 0. Environment Setup ---
    # The output directory is now 'Days'
    Config.OUTPUT_ROOT_DIR.mkdir(exist_ok=True) 
    Config.TTS_CACHE_DIR.mkdir(exist_ok=True)

    # --- 1. Initialize & Load Data ---
    initialize_source_data()
    master_data, max_day = load_and_validate_source_data()
    
    if not master_data:
        print("Workflow aborted due to data error.")
        return

    print(f"\nMaster data loaded with {len(master_data)} items over {max_day} days.")

    # --- 2. Determine Processing Scope ---
    days_to_process = []
    for day in range(1, max_day + 1):
        if not is_day_complete(day):
            days_to_process.append(day)

    if not days_to_process:
        print("\nAll days are declaratively complete. Workflow skipped.")
        return

    print(f"\nDays identified for processing: {days_to_process}")

    # --- 3. Generate Schedules ---
    schedules = generate_full_repetition_schedule(master_data, max_day)
    print("✅ Full repetition schedule generated in memory.")

    # --- 4. Process Identified Days ---
    for day in days_to_process:
        schedule = schedules.get(day)
        if schedule is not None:
            process_day(day, schedule)
            print(f"--- Day {day} processed successfully. ---\n")
        else:
            print(f"  ❌ Error: Schedule data missing for Day {day}.")


if __name__ == "__main__":
    main_workflow()