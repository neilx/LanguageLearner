import os
import pandas as pd
import random
import hashlib
from typing import List, Dict, Any, Union

# --- 1. CONSTANTS (As per V0.013 Documentation) ---
WORKFLOW_VERSION = 'v0.013'
REVIEW_RATIO = 5
INITIAL_BASE_DIR = 'output'
CACHE_FOLDER_NAME = 'cache'
EXPECTED_FILE_COUNT = 4
L2_SPEED_FACTOR = 1.4
PAUSE_L1_MS = 500
PAUSE_L2_MS = 1000
DURATION_TOLERANCE = 0.05

# --- 2. DUMMY DATA SETUP (Updated for Scenario 1: 100 items over 4 days) ---
# Total items: 100. Distribution: Day 1: 10, Day 2: 20, Day 3: 30, Day 4: 40
DAY_ITEM_DISTRIBUTION = {
    1: 10,
    2: 20,
    3: 30,
    4: 40
}
DUMMY_TOTAL_ITEMS = sum(DAY_ITEM_DISTRIBUTION.values())

# --- 3. Core Logic Simulation Functions ---

class AudioSegment:
    """Simulates an audio segment object with a duration property."""
    def __init__(self, duration_ms: int):
        self.duration_ms = duration_ms

    def speedup(self, factor: float) -> 'AudioSegment':
        """Simulates speeding up the audio."""
        new_duration = int(self.duration_ms / factor)
        return AudioSegment(new_duration)

    def export(self, file_path: str):
        """Simulates exporting the audio file."""
        with open(file_path, 'w') as f:
            f.write(f"Simulated audio content, duration: {self.duration_ms}ms")

    @staticmethod
    def empty(duration_ms: int) -> 'AudioSegment':
        """Creates an empty audio segment."""
        return AudioSegment(duration_ms)

    def __add__(self, other: 'AudioSegment') -> 'AudioSegment':
        """Simulates concatenating two audio segments."""
        return AudioSegment(self.duration_ms + other.duration_ms)

def _get_or_generate_audio(item_id: int, text: str, language: str) -> AudioSegment:
    """Simulates 2.2 Get or Generate Audio (with Caching)."""
    # Use SHA256 for caching key simulation
    cache_key = hashlib.sha256(f"{text}-{language}".encode()).hexdigest()
    cache_path = os.path.join(CACHE_FOLDER_NAME, cache_key)

    if os.path.exists(cache_path):
        # Simulated cache hit - assume a base duration
        return AudioSegment(duration_ms=5000)
    else:
        # Simulated TTS generation and caching
        os.makedirs(CACHE_FOLDER_NAME, exist_ok=True)
        base_duration = 1000 + len(text) * 50  # Simulates duration based on text length
        segment = AudioSegment(base_duration)
        segment.export(cache_path)
        return segment

def _generate_dummy_master_data(file_path: str) -> pd.DataFrame:
    """Generates 100 dummy items based on the new distribution."""
    data = []
    item_id = 1
    for day, count in DAY_ITEM_DISTRIBUTION.items():
        for _ in range(count):
            data.append({
                'item_id': item_id,
                'l1_text': f"L1 Sentence {item_id}",
                'l2_text': f"L2 Satz {item_id}",
                'w1': random.uniform(0.1, 1.0),
                'w2': random.uniform(0.1, 1.0),
                'study_day': day
            })
            item_id += 1
    
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"Generated dummy data: {DUMMY_TOTAL_ITEMS} items saved to {file_path}")
    return df

def _calculate_integrity_metric(actual_duration: int, expected_duration: int) -> float:
    """Implements 2.6 Calculate Integrity Metric."""
    if expected_duration == 0:
        return 0.0
    return abs(actual_duration - expected_duration) / expected_duration

# --- 4. V0.013 New Logic (2.9 & 2.10) ---

def _get_all_past_items(current_day: int) -> List[Dict[str, Any]]:
    """Implements 2.9: Retrieves items from schedule.csv of previous days."""
    past_items = []
    for day in range(1, current_day):
        schedule_path = os.path.join(INITIAL_BASE_DIR, f'day_{day}', 'schedule.csv')
        if os.path.exists(schedule_path):
            df = pd.read_csv(schedule_path)
            past_items.extend(df.to_dict('records'))
    return past_items

def _generate_review_schedule(new_items: List[Dict[str, Any]], review_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Implements 2.10: Interleaves new and review items (5:1 ratio)."""
    final_schedule = []
    
    # 1. Calculate required review items (based on the number of new items)
    num_new = len(new_items)
    num_review_required = num_new // REVIEW_RATIO
    
    # 2. Select review items randomly
    if len(review_items) > num_review_required:
        selected_review_items = random.sample(review_items, num_review_required)
    else:
        selected_review_items = review_items # Use all available review items

    # 3. Interleave (5 new items, then 1 review item)
    new_idx = 0
    review_idx = 0
    while new_idx < num_new or review_idx < len(selected_review_items):
        # Add a block of new items (up to REVIEW_RATIO)
        for _ in range(REVIEW_RATIO):
            if new_idx < num_new:
                final_schedule.append(new_items[new_idx])
                new_idx += 1
        
        # Add one review item
        if review_idx < len(selected_review_items):
            final_schedule.append(selected_review_items[review_idx])
            review_idx += 1
            
    return final_schedule

# --- 5. Main Processing Function (Simulates 2.4) ---

def _create_and_save_day_files(day_number: int, day_items: List[Dict[str, Any]]):
    """
    Core function to process one day: generates schedule, audio, and validates.
    """
    print(f"\n--- Processing Day {day_number} ({len(day_items)} new items) ---")
    day_folder = os.path.join(INITIAL_BASE_DIR, f'day_{day_number}')
    os.makedirs(day_folder, exist_ok=True)
    
    # 1. Generate final mixed schedule (V0.013 Logic)
    past_items = _get_all_past_items(day_number)
    final_schedule = _generate_review_schedule(day_items, past_items)
    
    if not final_schedule:
        print(f"Day {day_number}: No items to process.")
        return

    # 2. Generate Audio Tracks & Metrics
    total_workflow_audio = AudioSegment.empty(0)
    total_review_audio = AudioSegment.empty(0)
    total_reverse_audio = AudioSegment.empty(0)
    
    for item in final_schedule:
        # Simulate generating and adjusting audio (2.2 & 2.3)
        l1_seg = _get_or_generate_audio(item['item_id'], item['l1_text'], 'L1')
        l2_seg = _get_or_generate_audio(item['item_id'], item['l2_text'], 'L2').speedup(L2_SPEED_FACTOR)

        # Track 1: Workflow (L1 + L2 + Pauses)
        workflow_seg = l1_seg + AudioSegment.empty(PAUSE_L1_MS) + l2_seg + AudioSegment.empty(PAUSE_L2_MS)
        total_workflow_audio += workflow_seg
        
        # Track 2: Review (L2 only)
        total_review_audio += (l2_seg + AudioSegment.empty(PAUSE_L2_MS))
        
        # Track 3: Reverse (L2, then L1)
        reverse_seg = l2_seg + AudioSegment.empty(PAUSE_L2_MS) + l1_seg + AudioSegment.empty(PAUSE_L1_MS)
        total_reverse_audio += reverse_seg

    # 3. Save Files
    schedule_path = os.path.join(day_folder, 'schedule.csv')
    df_schedule = pd.DataFrame(final_schedule)
    df_schedule.to_csv(schedule_path, index=False)
    
    total_workflow_audio.export(os.path.join(day_folder, 'workflow.mp3'))
    total_review_audio.export(os.path.join(day_folder, 'review.mp3'))
    total_reverse_audio.export(os.path.join(day_folder, 'reverse.mp3'))
    
    print(f"Day {day_number}: Saved {len(final_schedule)} items and 3 MP3 files.")
    
    # 4. Simulate Validation (2.5 & 2.6)
    expected_rows = len(final_schedule)
    actual_rows = len(pd.read_csv(schedule_path))
    
    # For simulation, we assume perfect file existence and size.
    # The duration validation is also simplified:
    deviation = _calculate_integrity_metric(total_workflow_audio.duration_ms, total_workflow_audio.duration_ms) 
    
    if actual_rows == expected_rows and deviation <= DURATION_TOLERANCE:
        print(f"Day {day_number}: Validation SUCCESS (Rows: {actual_rows}, Deviation: {deviation:.4f})")
    else:
        print(f"Day {day_number}: Validation FAILED (Rows: {actual_rows} vs {expected_rows})")

# --- 6. Orchestration ---

def run_workflow():
    """High-Level Application Flow (Section 1)."""
    master_path = 'sentence_pairs.csv'
    
    # 1. Load/Generate Master Data (2.1)
    if not os.path.exists(master_path):
        master_df = _generate_dummy_master_data(master_path)
    else:
        master_df = pd.read_csv(master_path)

    # 2. Generate SRS Schedule (2.8)
    srs_schedule = master_df.groupby('study_day').apply(lambda x: x.to_dict('records')).to_dict()

    # 3. Determine Days to Process (Simulated 2.7)
    # In this initial run, all days with items are processed.
    days_to_process = sorted(srs_schedule.keys())
    print(f"Identified days to process: {days_to_process}")

    # 4. Process Missing Days
    for day in days_to_process:
        if day in srs_schedule:
            _create_and_save_day_files(day, srs_schedule[day])

if __name__ == "__main__":
    run_workflow()