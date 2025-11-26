import hashlib
import csv
from pathlib import Path
from typing import List, Dict, Any

# Assuming pydub is installed: pip install pydub
from pydub import AudioSegment

# --- Configuration Constants (As inferred from the documentation) ---
L1_SPEED_FACTOR = 1.0  # Assumed default
L2_SPEED_FACTOR = 1.25 # Example factor, should be a constant
PAUSE_L1_MS = 500      # Pause after L1
PAUSE_L2_MS = 1000     # Pause after L2 (between L2 repetitions)
MIN_FILE_SIZE_BYTES = 1000 # Minimum size for validation check
DURATION_TOLERANCE = 0.05  # 5% tolerance for duration validation

class LanguageLearnerWorkflow:
    """
    Implements the V0.007 Language Learner Workflow, focusing on
    TTS/caching, audio processing, and metric validation.
    """

    def __init__(self, base_dir: str = "."):
        """Initializes the workflow with a base directory for file storage."""
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        (self.base_dir / "cache").mkdir(exist_ok=True)
        (self.base_dir / "output").mkdir(exist_ok=True)
        self.data_source = self._load_data_source()

    # --- 2. Component Functionality ---

    def _load_data_source(self) -> List[Dict[str, Any]]:
        """2.1. Simulates loading the master list of learning items."""
        print("Loading data source...")
        # Placeholder data simulating item_id, L1 text, and L2 text
        return [
            {"item_id": i + 1, "l1_text": f"L1 Sentence {i+1}", "l2_text": f"L2 Sentence {i+1}"}
            for i in range(100)
        ]

    def _get_or_generate_audio(self, item_id: int, text: str, language: str) -> AudioSegment:
        """
        2.2. Implement robust caching and TTS simulation.
        CRITICAL: Key generation uses hashlib.sha256(text + language).
        CRITICAL: Cache path uses pathlib: self.base_dir / "cache" / f"{key}.mp3".
        """
        # 1. Generate Key & Path
        combined_string = text + language
        key = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
        cache_path = self.base_dir / "cache" / f"{key}.mp3"

        # 2. Check Cache (Hit)
        if cache_path.exists():
            # print(f"Cache HIT for {item_id}: {key[:6]}...")
            try:
                return AudioSegment.from_file(cache_path, format="mp3")
            except Exception as e:
                print(f"Error loading cached file {cache_path}: {e}. Regenerating.")
                cache_path.unlink(missing_ok=True) # Delete corrupted file

        # 3. Check Cache (Miss) & Simulate TTS
        # Simulate the TTS API call: duration = 500ms + 50ms per character.
        # This duration is used to create a silent AudioSegment placeholder.
        duration_ms = 500 + len(text) * 50
        # print(f"Cache MISS for {item_id}. Generating silent audio of {duration_ms}ms.")
        
        # AudioSegment.silent creates a silent segment
        audio_segment = AudioSegment.silent(duration=duration_ms, frame_rate=22050)
        
        # 4. Cache & Return
        try:
            audio_segment.export(cache_path, format="mp3", parameters=["-acodec", "libmp3lame"])
            return audio_segment
        except Exception as e:
            print(f"ERROR: Could not save generated audio to cache path {cache_path}: {e}")
            return AudioSegment.silent(duration=1000) # Return a safe 1s segment on failure

    def _adjust_speed(self, audio: AudioSegment, speed_factor: float) -> AudioSegment:
        """2.3. Adjusts the playback speed of an AudioSegment."""
        # Speed Check
        if speed_factor == 1.0:
            return audio

        # Apply Speed
        # Use pydub to adjust the frame rate for speed manipulation
        target_frame_rate = int(audio.frame_rate * speed_factor)
        return audio.set_frame_rate(target_frame_rate)

    def _create_and_save_day_files(self, day_number: int, day_items: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        2.4. Processes items for one day, concatenates audio, and calculates metrics.
        Returns pre-calculated expected_metrics.
        """
        print(f"\nProcessing Day {day_number} with {len(day_items)} items...")

        # Initialize Metrics
        running_time_ms = 0.0
        expected_metrics = {
            "track_a_duration_ms": 0.0,
            "track_b_duration_ms": 0.0,
            "track_c_duration_ms": 0.0,
            "csv_row_count": float(len(day_items))
        }
        schedule_rows = []
        
        # Initialize master tracks (using a simple 1ms silent segment to start)
        empty_audio = AudioSegment.silent(duration=1) 
        track_a = empty_audio
        track_b = empty_audio
        track_c = empty_audio

        pause_l1 = AudioSegment.silent(duration=PAUSE_L1_MS)
        pause_l2 = AudioSegment.silent(duration=PAUSE_L2_MS)

        # LOOP Item Processing
        for item in day_items:
            item_id = item["item_id"]
            l1_text = item["l1_text"]
            l2_text = item["l2_text"]

            # Get Audio (L1: native, L2: target)
            audio_l1 = self._get_or_generate_audio(item_id, l1_text, "L1")
            audio_l2 = self._get_or_generate_audio(item_id, l2_text, "L2")
            
            # Adjust Speed
            # L1 uses L1_SPEED_FACTOR (usually 1.0)
            adjusted_l1 = self._adjust_speed(audio_l1, L1_SPEED_FACTOR) 
            # L2 uses L2_SPEED_FACTOR
            adjusted_l2 = self._adjust_speed(audio_l2, L2_SPEED_FACTOR)

            # Calculate Durations (Theoretical Adjusted Durations)
            dur_adj_l1 = len(adjusted_l1)
            dur_adj_l2 = len(adjusted_l2)
            
            # Theoretical total expected duration for this item
            item_duration_A = dur_adj_l1 + PAUSE_L1_MS + dur_adj_l2
            item_duration_B = item_duration_A + PAUSE_L2_MS + dur_adj_l2
            item_duration_C = dur_adj_l2 + PAUSE_L2_MS + dur_adj_l2

            # Update Metrics
            expected_metrics["track_a_duration_ms"] += item_duration_A
            expected_metrics["track_b_duration_ms"] += item_duration_B
            expected_metrics["track_c_duration_ms"] += item_duration_C

            # Concatenate Audio
            # Track A: L1 + Pause_L1 + L2
            track_a += adjusted_l1 + pause_l1 + adjusted_l2
            
            # Track B: L1 + Pause_L1 + L2 + Pause_L2 + L2
            track_b += adjusted_l1 + pause_l1 + adjusted_l2 + pause_l2 + adjusted_l2

            # Track C: L2 + Pause_L2 + L2 (Repetition/Review track)
            track_c += adjusted_l2 + pause_l2 + adjusted_l2

            # Update Schedule CSV (based on Track A's duration)
            schedule_row = {
                "item_id": item_id,
                "l1_text": l1_text,
                "l2_text": l2_text,
                "start_time_ms": int(running_time_ms),
                "end_time_ms": int(running_time_ms + item_duration_A)
            }
            schedule_rows.append(schedule_row)
            running_time_ms += item_duration_A + PAUSE_L2_MS # Day's total running time metric

        # Save Files
        day_output_dir = self.base_dir / "output" / f"day_{day_number}"
        day_output_dir.mkdir(exist_ok=True)
        
        # Save Audio Files
        print(f"Exporting audio files for Day {day_number}...")
        track_a.export(day_output_dir / "track_A.mp3", format="mp3")
        track_b.export(day_output_dir / "track_B.mp3", format="mp3")
        track_c.export(day_output_dir / "track_C.mp3", format="mp3")

        # Save Schedule CSV
        csv_path = day_output_dir / "schedule.csv"
        if schedule_rows:
            fieldnames = list(schedule_rows[0].keys())
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(schedule_rows)
            print(f"Saved schedule to {csv_path}")
        
        # Success
        print(f"Day {day_number} expected metrics calculated.")
        return expected_metrics

    def _validate_day_files(self, day_number: int, expected_metrics: Dict[str, float]) -> bool:
        """
        2.5. Validates file integrity against pre-calculated metrics.
        Focuses on row count and duration against an independent metric.
        """
        print(f"\nValidating files for Day {day_number}...")
        day_output_dir = self.base_dir / "output" / f"day_{day_number}"
        
        # Validate CSV
        csv_path = day_output_dir / "schedule.csv"
        actual_row_count = 0
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) # Skip header
                actual_row_count = sum(1 for row in reader)
        except Exception as e:
            print(f"Validation FAILED: Could not read CSV: {e}")
            return False

        expected_row_count = int(expected_metrics["csv_row_count"])
        if actual_row_count != expected_row_count:
            print(f"Validation FAILED: Row count mismatch. Expected: {expected_row_count}, Actual: {actual_row_count}")
            return False
        print("CSV row count validated successfully.")

        mp3_files = {
            "track_A": day_output_dir / "track_A.mp3",
            "track_B": day_output_dir / "track_B.mp3",
            "track_C": day_output_dir / "track_C.mp3",
        }

        # LOOP MP3 Files
        for track_name, mp3_path in mp3_files.items():
            expected_duration_ms = expected_metrics[f"{track_name.lower()}_duration_ms"]

            # Check File Size (Sufficient Size Check)
            if not mp3_path.exists():
                print(f"Validation FAILED for {track_name}: File not found.")
                return False
            
            actual_size_bytes = mp3_path.stat().st_size
            if actual_size_bytes < MIN_FILE_SIZE_BYTES:
                print(f"Validation FAILED for {track_name}: File too small ({actual_size_bytes} bytes).")
                return False

            # Get Actual Duration & Check Duration
            try:
                audio = AudioSegment.from_file(mp3_path, format="mp3")
                actual_duration_ms = len(audio)
            except Exception as e:
                print(f"Validation FAILED for {track_name}: Could not load MP3: {e}")
                return False

            # Calculate deviation
            deviation = abs(actual_duration_ms - expected_duration_ms) / expected_duration_ms
            
            if deviation > DURATION_TOLERANCE:
                print(f"Validation FAILED for {track_name}: Duration deviation too high ({deviation:.2%}).")
                print(f"Expected: {expected_duration_ms:.0f}ms, Actual: {actual_duration_ms:.0f}ms")
                return False
            
            print(f"{track_name} duration validated. Deviation: {deviation:.2%}")

        # Success
        print(f"Day {day_number} validation SUCCEEDED.")
        return True

    # --- 1. High-Level Application Flow ---
    
    def run_orchestration(self):
        """1. Orchestrates the entire language learning file generation process."""
        print("--- Starting V0.007 Language Learner Orchestration ---")

        # 2. Generate SRS Schedule (Placeholder)
        # Simple placeholder to split 100 items into 4 days
        total_items = len(self.data_source)
        items_per_day = 25
        num_days = (total_items + items_per_day - 1) // items_per_day

        for day_number in range(1, num_days + 1):
            start_index = (day_number - 1) * items_per_day
            end_index = day_number * items_per_day
            day_items = self.data_source[start_index:end_index]
            
            if not day_items:
                break

            # 3. LOOP for Each Day: Create and Save
            expected_metrics = self._create_and_save_day_files(day_number, day_items)

            # 4. Validate Files
            validation_success = self._validate_day_files(day_number, expected_metrics)
            
            # 5. Halt or Continue
            if not validation_success:
                print("\n\n!!! PROCESS HALTED DUE TO INTEGRITY FAILURE !!!")
                return

        print("\n--- Orchestration Complete. All days validated successfully. ---")


if __name__ == "__main__":
    # Example usage:
    # This will create a 'cache' directory and an 'output' directory in the current
    # working directory, and run the simulation for 4 days.
    workflow = LanguageLearnerWorkflow()
    workflow.run_orchestration()