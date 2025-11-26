import os
import hashlib
import time
import csv
from pathlib import Path
from typing import List, Dict, Any
from pydub import AudioSegment
from math import ceil

# --- Constants (Per V0.012 Documentation) ---
L1_LANGUAGE = 'en'
L2_LANGUAGE = 'de'
L1_SPEED_FACTOR = 1.0
L2_SPEED_FACTOR = 1.4
PAUSE_L1_MS = 500
PAUSE_L2_MS = 1000
DURATION_TOLERANCE = 0.05
MIN_FILE_SIZE_BYTES = 1024
ITEMS_PER_DAY = 100 # DEPRECATED: No longer used for scheduling, but kept as constant.
EXPECTED_OUTPUT_FILES = ['workflow.mp3', 'review.mp3', 'reverse.mp3', 'schedule.csv']
EXPECTED_FILE_COUNT = len(EXPECTED_OUTPUT_FILES)

# Workflow Version Constant
WORKFLOW_VERSION = 'v0.012' 

# Canonical Directory and File Names
INITIAL_BASE_DIR = 'output' 
CACHE_FOLDER_NAME = 'cache'
SENTENCE_PAIRS_FILE = 'sentence_pairs.csv' 
FULL_DUMMY_ITEM_COUNT = 150 # Total items for diversified test scheduling

# --- Simulation Data (Step 2.1 - Load from Arbitrary Schedule CSV) ---
def _load_data_source() -> List[Dict[str, Any]]:
    """Loads the master list of learning items from a CSV file, creating it if missing."""
    
    file_path = Path(SENTENCE_PAIRS_FILE)
    # UPDATED FIELDNAMES to include 'study_day'
    fieldnames = ['item_id', 'l1_text', 'l2_text', 'w1', 'w2', 'study_day'] 
    
    # 3. IF MISSING: Create the file with arbitrary scheduled dummy data
    if not file_path.exists():
        print(f"INFO: {SENTENCE_PAIRS_FILE} not found. Creating file with {FULL_DUMMY_ITEM_COUNT} items using arbitrary scheduling.")
        dummy_data = []
        
        # Arbitrary Scheduling Logic for Testing (Items/Day: D1: 10, D2: 40, D3: 100)
        day_counts = {1: 10, 2: 40, 3: 100}
        current_item_id = 1
        
        for day, count in day_counts.items():
            for i in range(count):
                dummy_data.append({
                    'item_id': current_item_id,
                    'l1_text': f"L1 sentence {current_item_id} for arbitrary day {day}.",
                    'l2_text': f"L2 phrase {current_item_id} for arbitrary day {day}.",
                    'w1': 0.5, 
                    'w2': 0.9,
                    'study_day': day, # CRITICAL: Assign the arbitrary day
                })
                current_item_id += 1
            
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(dummy_data)
            print(f"SUCCESS: Created {SENTENCE_PAIRS_FILE} with {current_item_id - 1} items.")
        except Exception as e:
            print(f"FATAL ERROR: Could not write {SENTENCE_PAIRS_FILE}. Error: {e}")
            return []

    # 4. Load Data: Read the data from the CSV file
    data = []
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Ensure item_id and study_day are converted to integers
                try:
                    row['item_id'] = int(row['item_id'])
                    row['study_day'] = int(row['study_day']) # CRITICAL: Convert study_day to int
                except ValueError:
                    print(f"ERROR: Invalid integer value encountered in item_id or study_day: {row}")
                    continue
                data.append(row)
    except Exception as e:
        print(f"FATAL ERROR: Could not read data from {SENTENCE_PAIRS_FILE}. Error: {e}")
        return []

    print(f"INFO: Loaded {len(data)} items from {SENTENCE_PAIRS_FILE}.")
    return data

# --- Core Workflow Class ---
class LanguageLearnerWorkflow:
    def __init__(self):
        # 0. File System Structure (Sibling Setup)
        self.output_dir = Path(INITIAL_BASE_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.cache_dir = Path(CACHE_FOLDER_NAME)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.srs_schedule: Dict[int, List[Dict[str, Any]]] = {}

    # --- TTS Simulation (Helper for Step 2.2) ---
    def _simulate_tts(self, text: str) -> AudioSegment:
        """Simulates TTS generation (Step 2.2, point 3)."""
        duration_ms = 500 + len(text) * 50
        return AudioSegment.silent(duration=duration_ms, frame_rate=44100)

    # --- 2.2. Get or Generate Audio (with Caching) ---
    def _get_or_generate_audio(self, item_id: int, text: str, language: str) -> AudioSegment:
        """Implements robust caching and TTS generation logic."""
        
        key_input = text + language
        key = hashlib.sha256(key_input.encode('utf-8')).hexdigest()
        cache_path = self.cache_dir / f"{key}.mp3" 

        if cache_path.exists():
            return AudioSegment.from_file(cache_path)

        new_audio = self._simulate_tts(text)
        
        try:
            new_audio.export(cache_path, format="mp3")
        except Exception as e:
            print(f"FATAL AUDIO EXPORT ERROR: Could not write cache file {cache_path}. Check FFmpeg setup! Error: {e}")
            
        return new_audio

    # --- 2.3. Adjust Speed ---
    def _adjust_speed(self, audio: AudioSegment, speed_factor: float) -> AudioSegment:
        """Adjusts the playback speed of an AudioSegment."""
        if speed_factor == 1.0:
            return audio
        new_rate = int(audio.frame_rate * speed_factor)
        return audio.set_frame_rate(new_rate)

    # --- 2.7. Get Missing Day Files ---
    def _get_missing_day_files(self, max_day: int) -> List[int]:
        """Identifies days that are missing one or more output files."""
        missing_days = []
        for day_num in range(1, max_day + 1):
            day_path = self.output_dir / f"day_{day_num}"
            
            if not day_path.exists():
                missing_days.append(day_num)
                continue
                
            actual_file_count = len([f for f in day_path.iterdir() if f.is_file()])
            
            if actual_file_count != EXPECTED_FILE_COUNT:
                missing_days.append(day_num)

        return sorted(list(set(missing_days)))

    # --- 2.6. Calculate Integrity Metric ---
    def _calculate_integrity_metric(self, actual_duration_ms: float, expected_duration_ms: float) -> float:
        """Calculates the percentage deviation for validation (Step 2.6)."""
        if expected_duration_ms == 0:
            return float('inf')
        
        deviation = abs(actual_duration_ms - expected_duration_ms) / expected_duration_ms
        return deviation

    # --- 2.4. Create and Save Day Files ---
    def _create_and_save_day_files(self, day_number: int, day_items: List[Dict[str, Any]]) -> Dict[str, float]:
        """Core logic to generate audio tracks and schedule for a single day."""
        
        day_path = self.output_dir / f"day_{day_number}" 
        day_path.mkdir(exist_ok=True)
        
        expected_metrics = {'workflow_ms': 0, 'review_ms': 0, 'reverse_ms': 0, 'csv_row_count': len(day_items)}
        schedule_rows: List[Dict[str, Any]] = []
        
        track_a = AudioSegment.empty()
        track_b = AudioSegment.empty()
        track_c = AudioSegment.empty()

        for item in day_items:
            l1_audio = self._get_or_generate_audio(item['item_id'], item['l1_text'], L1_LANGUAGE)
            l2_audio = self._get_or_generate_audio(item['item_id'], item['l2_text'], L2_LANGUAGE)
            
            l2_adjusted = self._adjust_speed(l2_audio, L2_SPEED_FACTOR)
            
            pause_l1 = AudioSegment.silent(duration=PAUSE_L1_MS)
            pause_l2 = AudioSegment.silent(duration=PAUSE_L2_MS)

            # Calculate Durations
            dur_l1_adj = len(l1_audio)
            dur_l2_adj = len(l2_adjusted)

            # Calculate Expected Durations for MP3 Validation
            dur_a = dur_l1_adj + PAUSE_L1_MS + dur_l2_adj + PAUSE_L2_MS
            dur_b = (dur_l2_adj + PAUSE_L2_MS) * 2
            dur_c = dur_l2_adj + PAUSE_L2_MS + dur_l1_adj + PAUSE_L1_MS

            # Update Metrics
            expected_metrics['workflow_ms'] += dur_a
            expected_metrics['review_ms'] += dur_b
            expected_metrics['reverse_ms'] += dur_c
            
            # Concatenate Audio
            track_a += l1_audio + pause_l1 + l2_adjusted + pause_l2
            track_b += l2_adjusted + pause_l2 + l2_adjusted + pause_l2 
            track_c += l2_adjusted + pause_l2 + l1_audio + pause_l1

            # Update Schedule CSV (Content only, includes study_day for reference)
            schedule_rows.append({
                'item_id': item['item_id'],
                'l1_text': item['l1_text'],
                'l2_text': item['l2_text'],
                'w1': item['w1'], 
                'w2': item['w2'], 
                'study_day': item['study_day'], # Include day number for output tracking
            })

        # Save Files
        print(f"INFO: Saving files for Day {day_number}...")
        track_a.export(day_path / "workflow.mp3", format="mp3")
        track_b.export(day_path / "review.mp3", format="mp3")
        track_c.export(day_path / "reverse.mp3", format="mp3")
        
        # Write CSV Schedule (UPDATED FIELDNAMES to include study_day)
        csv_path = day_path / "schedule.csv"
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['item_id', 'l1_text', 'l2_text', 'w1', 'w2', 'study_day']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(schedule_rows)

        print(f"INFO: Day {day_number} files saved. Total Items: {expected_metrics['csv_row_count']}")
        return expected_metrics

    # --- 2.5. Validate Day Files ---
    def _validate_day_files(self, day_number: int, expected_metrics: Dict[str, float]) -> bool:
        """Validates the generated files against the pre-calculated metrics."""
        print(f"INFO: Validating Day {day_number}...")
        day_path = self.output_dir / f"day_{day_number}"
        
        # Validate CSV (Row Count)
        csv_path = day_path / "schedule.csv"
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                actual_row_count = sum(1 for row in reader) - 1
        except FileNotFoundError:
            print(f"ERROR: CSV file not found: {csv_path}")
            return False

        expected_row_count = expected_metrics['csv_row_count']
        if actual_row_count != expected_row_count:
            print(f"ERROR: Row count mismatch. Actual: {actual_row_count}, Expected: {expected_row_count}")
            return False

        # LOOP MP3 Files
        mp3_checks = {
            "workflow.mp3": expected_metrics['workflow_ms'],
            "review.mp3": expected_metrics['review_ms'],
            "reverse.mp3": expected_metrics['reverse_ms']
        }
        
        for filename, expected_duration_ms in mp3_checks.items():
            mp3_path = day_path / filename

            if not mp3_path.exists() or mp3_path.stat().st_size < MIN_FILE_SIZE_BYTES:
                print(f"ERROR: MP3 file size check failed for {filename}. Size is too small or file missing.")
                return False

            try:
                actual_duration_ms = len(AudioSegment.from_file(mp3_path))
            except Exception as e:
                print(f"ERROR: Failed to load MP3 {filename}. Check FFmpeg setup! {e}")
                return False

            deviation = self._calculate_integrity_metric(actual_duration_ms, expected_duration_ms)

            if deviation > DURATION_TOLERANCE:
                print(f"ERROR: Duration deviation for {filename} is too high ({deviation*100:.2f}%).")
                print(f"Actual: {actual_duration_ms:.0f}ms, Expected: {expected_duration_ms:.0f}ms")
                return False

        print(f"SUCCESS: Day {day_number} validation passed.")
        return True
        
    # --- 2.8. Generate SRS Schedule (REFRACTORED to group by 'study_day') ---
    def _generate_srs_schedule(self, master_data: List[Dict[str, Any]]) -> int:
        """Groups items by the arbitrary 'study_day' key from the input data."""
        
        if not master_data:
            return 0
            
        srs_schedule = {}
        max_day = 0
        
        for item in master_data:
            day_num = item['study_day']
            if day_num not in srs_schedule:
                srs_schedule[day_num] = []
            srs_schedule[day_num].append(item)
            
            if day_num > max_day:
                max_day = day_num
                
        self.srs_schedule = srs_schedule
        
        print(f"INFO: Generated schedule based on arbitrary 'study_day' values. Max study day: {max_day}")
        
        # Log item counts for debugging
        for day, items in sorted(srs_schedule.items()):
            print(f"DEBUG: Day {day} has {len(items)} items.")
            
        return max_day

    # --- 1. High-Level Application Flow (Orchestration) ---
    def run_orchestration(self):
        print("--- Starting Language Learner Workflow ---")
        print(f"INFO: Workflow Version: {WORKFLOW_VERSION}")
        start_time = time.time()
        
        master_data = _load_data_source()
        
        if not master_data:
            print("FATAL: Master data is empty. Halting orchestration.")
            return
            
        max_day = self._generate_srs_schedule(master_data)
        
        if max_day == 0:
            print("--- No study days scheduled. Halting orchestration. ---")
            return
            
        # Get missing days from 1 up to the highest scheduled day
        missing_days = self._get_missing_day_files(max_day)
        
        # Filter the missing days list to only include days that actually exist in the schedule
        missing_days = [d for d in missing_days if d in self.srs_schedule]
        
        if not missing_days:
            print("--- All scheduled days are complete. Halting orchestration. ---")
            return

        print(f"INFO: Days to process: {missing_days}")
        
        all_ok = True
        for day_number in missing_days:
            if day_number not in self.srs_schedule:
                print(f"ERROR: Day {day_number} is missing in the generated schedule. Skipping.")
                continue

            day_items = self.srs_schedule[day_number]
            
            expected_metrics = self._create_and_save_day_files(day_number, day_items)
            
            if not self._validate_day_files(day_number, expected_metrics):
                print(f"FATAL: Validation failed for Day {day_number}. Halting process.")
                all_ok = False
                break
        
        if all_ok:
            print(f"--- Workflow Complete. Processed {len(missing_days)} day(s) successfully. ---")
        print(f"Total elapsed time: {time.time() - start_time:.2f} seconds.")


if __name__ == '__main__':
    workflow = LanguageLearnerWorkflow()
    workflow.run_orchestration()