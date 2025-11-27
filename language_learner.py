import os
import pandas as pd
import random
import hashlib
from typing import List, Dict, Any, Union

# --- 1. CONSTANTS (As per V0.020 Documentation) ---
WORKFLOW_VERSION = 'v0.020'
REVIEW_RATIO = 5
INITIAL_BASE_DIR = 'output'
CACHE_FOLDER_NAME = 'cache'
EXPECTED_FILE_COUNT = 4 # CSV + 3 MP3s = 4
L2_SPEED_FACTOR = 1.4
PAUSE_L1_MS = 500
PAUSE_L2_MS = 1000
DURATION_TOLERANCE = 0.05
CSV_DTYPE_MAP = {'study_day': str, 'item_id': 'int64'} 
CSV_FILENAME = 'schedule.csv'
MP3_FILENAMES = ['workflow.mp3', 'review.mp3', 'reverse.mp3']
ALL_FILENAMES = MP3_FILENAMES + [CSV_FILENAME]

# --- 2. DUMMY DATA SETUP ---
DAY_ITEM_DISTRIBUTION = {
    1: 10,
    2: 20,
    3: 30,
    4: 40
}
DUMMY_TOTAL_ITEMS = sum(DAY_ITEM_DISTRIBUTION.values())
SCHEMA_COLUMNS = ['item_id', 'w2', 'w1', 'l1_text', 'l2_text', 'study_day']

# --- 3. Core Logic Simulation Functions (Simulated audio class remains the same) ---

class AudioSegment:
    def __init__(self, duration_ms: int):
        self.duration_ms = duration_ms
    def speedup(self, factor: float) -> 'AudioSegment':
        new_duration = int(self.duration_ms / factor)
        return AudioSegment(new_duration)
    def export(self, file_path: str):
        with open(file_path, 'w') as f:
            f.write(f"Simulated audio content, duration: {self.duration_ms}ms")
    @staticmethod
    def empty(duration_ms: int) -> 'AudioSegment':
        return AudioSegment(duration_ms)
    def __add__(self, other: 'AudioSegment') -> 'AudioSegment':
        return AudioSegment(self.duration_ms + other.duration_ms)

def _get_or_generate_audio(item_id: int, text: str, language: str) -> AudioSegment:
    cache_key = hashlib.sha256(f"{text}-{language}".encode()).hexdigest()
    cache_path = os.path.join(CACHE_FOLDER_NAME, cache_key)
    if os.path.exists(cache_path):
        return AudioSegment(duration_ms=5000)
    else:
        os.makedirs(CACHE_FOLDER_NAME, exist_ok=True)
        base_duration = 1000 + len(text) * 50
        segment = AudioSegment(base_duration)
        segment.export(cache_path)
        return segment

def _generate_dummy_master_data(file_path: str) -> pd.DataFrame:
    data = []
    item_id = 1
    dummy_danish_words = ['Hund', 'Kat', 'Hus', 'Båd', 'Vand', 'Lys']
    dummy_english_words = ['Dog', 'Cat', 'House', 'Boat', 'Water', 'Light']

    for day, count in DAY_ITEM_DISTRIBUTION.items():
        for _ in range(count):
            w2_word = random.choice(dummy_danish_words)
            w1_word = random.choice(dummy_english_words)

            data.append({
                'item_id': item_id,
                'w2': w2_word,
                'w1': w1_word,
                'l1_text': f"The quick {w1_word} jumps over the lazy dog {item_id}.",
                'l2_text': f"Den hurtige {w2_word} hopper over den dovne hund {item_id}.",
                'study_day': day
            })
            item_id += 1
    
    df = pd.DataFrame(data)
    df = df[SCHEMA_COLUMNS] 
    df['study_day'] = df['study_day'].astype(int)
    df.to_csv(file_path, index=False)
    print(f"Generated dummy data: {DUMMY_TOTAL_ITEMS} items saved to {file_path}")
    return df

def _calculate_integrity_metric(actual_duration: int, expected_duration: int) -> float:
    if expected_duration == 0:
        return 0.0
    return abs(actual_duration - expected_duration) / expected_duration

# --- 4. V0.020 Fixed Logic (Declarative Day Selection) ---

def _get_all_past_items(current_day: int) -> List[Dict[str, Any]]:
    """Retrieves items from schedule.csv of previous days."""
    past_items = []
    
    for day in range(1, current_day):
        schedule_path = os.path.join(INITIAL_BASE_DIR, f'day_{day}', CSV_FILENAME) 
        if os.path.exists(schedule_path):
            df = pd.read_csv(schedule_path, dtype=CSV_DTYPE_MAP) 
            past_items.extend(df.to_dict('records'))
    return past_items

def _generate_review_schedule(new_items: List[Dict[str, Any]], review_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Interleaves new and review items (5:1 ratio)."""
    final_schedule = []
    num_new = len(new_items)
    num_review_required = num_new // REVIEW_RATIO
    
    if len(review_items) > num_review_required:
        selected_review_items = random.sample(review_items, num_review_required)
    else:
        selected_review_items = review_items

    new_idx = 0
    review_idx = 0
    while new_idx < num_new or review_idx < len(selected_review_items):
        for _ in range(REVIEW_RATIO):
            if new_idx < num_new:
                final_schedule.append(new_items[new_idx])
                new_idx += 1
        
        if review_idx < len(selected_review_items):
            final_schedule.append(selected_review_items[review_idx])
            review_idx += 1
            
    return final_schedule

def _get_processed_days(all_days_int: List[int]) -> List[int]:
    """DECLARATIVE FILTER: Returns a list of day numbers that are ALREADY COMPLETE."""
    
    # Check if the base output directory exists
    if not os.path.exists(INITIAL_BASE_DIR):
        return []

    # Use a list comprehension (the declarative filter) to check the state of each day
    processed_days = [
        day for day in all_days_int
        if os.path.exists(os.path.join(INITIAL_BASE_DIR, f'day_{day}')) and 
           len(os.listdir(os.path.join(INITIAL_BASE_DIR, f'day_{day}'))) == EXPECTED_FILE_COUNT
    ]
    
    return processed_days


# --- 5. Main Processing Function ---

def _create_and_save_day_files(day_number: int, day_items: List[Dict[str, Any]]):
    """Core function to process one day."""
    print(f"\n--- Processing Day {day_number} ({len(day_items)} new items) ---")
    day_folder = os.path.join(INITIAL_BASE_DIR, f'day_{day_number}')
    os.makedirs(day_folder, exist_ok=True)
    
    past_items = _get_all_past_items(day_number)
    final_schedule = _generate_review_schedule(day_items, past_items)
    
    if not final_schedule:
        print(f"Day {day_number}: No items to process.")
        return

    # Audio generation logic (unchanged)
    total_workflow_audio = AudioSegment.empty(0)
    total_review_audio = AudioSegment.empty(0)
    total_reverse_audio = AudioSegment.empty(0)
    
    for item in final_schedule:
        l1_seg = _get_or_generate_audio(item['item_id'], item['l1_text'], 'L1')
        l2_seg = _get_or_generate_audio(item['item_id'], item['l2_text'], 'L2').speedup(L2_SPEED_FACTOR)

        workflow_seg = l1_seg + AudioSegment.empty(PAUSE_L1_MS) + l2_seg + AudioSegment.empty(PAUSE_L2_MS)
        total_workflow_audio += workflow_seg
        
        total_review_audio += (l2_seg + AudioSegment.empty(PAUSE_L2_MS))
        
        reverse_seg = l2_seg + AudioSegment.empty(PAUSE_L2_MS) + l1_seg + AudioSegment.empty(PAUSE_L1_MS)
        total_reverse_audio += reverse_seg

    # Save Files 
    schedule_path = os.path.join(day_folder, CSV_FILENAME)
    df_schedule = pd.DataFrame(final_schedule)
    df_schedule = df_schedule[SCHEMA_COLUMNS] 
    df_schedule.to_csv(schedule_path, index=False)
    
    total_workflow_audio.export(os.path.join(day_folder, MP3_FILENAMES[0])) 
    total_review_audio.export(os.path.join(day_folder, MP3_FILENAMES[1])) 
    total_reverse_audio.export(os.path.join(day_folder, MP3_FILENAMES[2])) 
    
    print(f"Day {day_number}: Saved {len(final_schedule)} items and 3 MP3 files.")
    
    # Validation
    expected_rows = len(final_schedule)
    actual_rows = len(pd.read_csv(schedule_path, dtype=CSV_DTYPE_MAP)) 
    deviation = _calculate_integrity_metric(total_workflow_audio.duration_ms, total_workflow_audio.duration_ms) 
    
    if actual_rows == expected_rows and deviation <= DURATION_TOLERANCE:
        print(f"Day {day_number}: Validation SUCCESS (Rows: {actual_rows}, Deviation: {deviation:.4f})")
    else:
        print(f"Day {day_number}: Validation FAILED (Rows: {actual_rows} vs {expected_rows})")

# --- 6. Orchestration (Main Function) ---

def run_workflow():
    """High-Level Application Flow (Section 1) - FIXED V0.020."""
    master_path = 'sentence_pairs.csv'
    
    # 1. Load/Generate Master Data
    if not os.path.exists(master_path):
        master_df = _generate_dummy_master_data(master_path)
    else:
        master_df = pd.read_csv(master_path, dtype=CSV_DTYPE_MAP) 

    # 2. Generate SRS Schedule 
    grouped_data = master_df.groupby('study_day').apply(lambda x: x.to_dict('records'), include_groups=False)
    
    srs_schedule = {}
    for day_str, items in grouped_data.items():
        day_int = int(day_str) 
        for item in items:
            item['study_day'] = day_int 
        srs_schedule[day_int] = items 
    
    # 3. Determine Days to Process (Declarative Logic V0.020)
    all_days = sorted(srs_schedule.keys()) 
    
    # Identify days that are ALREADY COMPLETE on disk
    processed_days = _get_processed_days(all_days)
    
    # Declarative Subtraction: Find days in ALL_DAYS that are NOT in PROCESSED_DAYS
    days_to_process = sorted(list(set(all_days) - set(processed_days)))
            
    print(f"Identified days to process: {days_to_process}")

    # 4. Process Missing Days
    for day in days_to_process:
        if day in srs_schedule:
            _create_and_save_day_files(day, srs_schedule[day])

if __name__ == "__main__":
    run_workflow()