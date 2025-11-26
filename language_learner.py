import hashlib
import csv
import time # Added for timing optimization effect
from pathlib import Path
from typing import List, Dict, Any, Optional

# Assuming pydub is installed: pip install pydub
from pydub import AudioSegment

# --- Configuration Constants ---
L1_SPEED_FACTOR = 1.0  
L2_SPEED_FACTOR = 1.25 
PAUSE_L1_MS = 500      
PAUSE_L2_MS = 1000     
MIN_FILE_SIZE_BYTES = 1000 
DURATION_TOLERANCE = 0.05  

class LanguageLearnerWorkflow:
    """
    Implements the V0.008 Language Learner Workflow, introducing
    Day-Level Output Caching for performance optimization.
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
        2.2. Implement robust caching and TTS simulation (Item-Level Caching).
        """
        # 1. Generate Key & Path
        combined_string = text + language
        key = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
        cache_path = self.base_dir / "cache" / f"{key}.mp3"

        # 2. Check Cache (Hit)
        if cache_path.exists():
            try:
                # print(f"Item Cache HIT: {key[:6]}...")
                return AudioSegment.from_file(cache_path, format="mp3")
            except Exception as e:
                print(f"Error loading cached file {cache_path}: {e}. Regenerating.")
                cache_path.unlink(missing_ok=True) 

        # 3. Check Cache (Miss) & Simulate TTS
        duration_ms = 500 + len(text) * 50
        # print(f"Item Cache MISS. Generating audio of {duration_ms}ms.")
        audio_segment = AudioSegment.silent(duration=duration_ms, frame_rate=22050)
        
        # 4. Cache & Return
        try:
            audio_segment.export(cache_path, format="mp3", parameters=["-acodec", "libmp3lame"])
            return audio_segment
        except Exception as e:
            print(f"ERROR: Could not save generated audio to cache path {cache_path}: {e}")
            return AudioSegment.silent(duration=1000) 

    def _adjust_speed(self, audio: AudioSegment, speed_factor: float) -> AudioSegment:
        """2.3. Adjusts the playback speed of an AudioSegment."""
        if speed_factor == 1.0:
            return audio

        target_frame_rate = int(audio.frame_rate * speed_factor)
        return audio.set_frame_rate(target_frame_rate)
    
    def _check_day_cache_hit(self, day_number: int) -> bool:
        """
        NEW V0.008 IMPLEMENTATION: Checks if all final output files for a day exist.
        """
        day_output_dir = self.base_dir / "output" / f"day_{day_number}"
        
        # List of required final output files
        required_files = [
            day_output_dir / "track_A.mp3",
            day_output_dir / "track_B.mp3",
            day_output_dir / "track_C.mp3",
            day_output_dir / "schedule.csv",
        ]
        
        # Check if directory exists and all required files exist within it
        if day_output_dir.exists() and all(f.exists() for f in required_files):
            return True
        return False
    
    def _load_expected_metrics_from_csv(self, day_number: int) -> Optional[Dict[str, float]]:
        """
        Helper to load expected metrics (calculated during creation) if a day is skipped.
        In a real system, these metrics would be saved alongside the files. 
        For this simulation, we'll force recalculation or return a placeholder if we skip the creation step. 
        Since the V0.007 doc didn't include saving metrics, we must trust the re-calculation 
        or rely on the Day-Level check implicitly. For simplicity in this simulation, 
        we will require the metrics to be CALCULATED if validation is run.
        """
        # In this V0.008 simulation, a true cache hit for metrics is not possible 
        # without further doc changes. The Day-Cache Hit only skips the creation/export.
        # We'll allow the orchestration to handle the calculation logic.
        return None # Metrics must be calculated if validation runs.

    def _create_and_save_day_files(self, day_number: int, day_items: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        2.4. Processes items for one day, concatenates audio, and calculates metrics.
        (Called only on a Day Cache MISS)
        """
        print(f"\nProcessing Day {day_number} - Starting File Generation (Cache MISS)...")
        start_time = time.time() # Start timing generation

        # Initialize Metrics and Audio Tracks... (The rest of the logic remains V0.007 compliant)
        running_time_ms = 0.0
        expected_metrics = {
            "track_a_duration_ms": 0.0,
            "track_b_duration_ms": 0.0,
            "track_c_duration_ms": 0.0,
            "csv_row_count": float(len(day_items))
        }
        schedule_rows = []
        
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

            # Get Audio (This uses the fast Item-Level Cache)
            audio_l1 = self._get_or_generate_audio(item_id, l1_text, "L1")
            audio_l2 = self._get_or_generate_audio(item_id, l2_text, "L2")
            
            # Adjust Speed
            adjusted_l1 = self._adjust_speed(audio_l1, L1_SPEED_FACTOR) 
            adjusted_l2 = self._adjust_speed(audio_l2, L2_SPEED_FACTOR)

            # Calculate Durations
            dur_adj_l1 = len(adjusted_l1)
            dur_adj_l2 = len(adjusted_l2)
            
            item_duration_A = dur_adj_l1 + PAUSE_L1_MS + dur_adj_l2
            item_duration_B = item_duration_A + PAUSE_L2_MS + dur_adj_l2
            item_duration_C = dur_adj_l2 + PAUSE_L2_MS + dur_adj_l2

            # Update Metrics
            expected_metrics["track_a_duration_ms"] += item_duration_A
            expected_metrics["track_b_duration_ms"] += item_duration_B
            expected_metrics["track_c_duration_ms"] += item_duration_C

            # Concatenate Audio
            track_a += adjusted_l1 + pause_l1 + adjusted_l2
            track_b += adjusted_l1 + pause_l1 + adjusted_l2 + pause_l2 + adjusted_l2
            track_c += adjusted_l2 + pause_l2 + adjusted_l2

            # Update Schedule CSV
            schedule_row = {
                "item_id": item_id,
                "l1_text": l1_text,
                "l2_text": l2_text,
                "start_time_ms": int(running_time_ms),
                "end_time_ms": int(running_time_ms + item_duration_A)
            }
            schedule_rows.append(schedule_row)
            running_time_ms += item_duration_A + PAUSE_L2_MS

        # Save Files
        day_output_dir = self.base_dir / "output" / f"day_{day_number}"
        day_output_dir.mkdir(exist_ok=True)
        
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
        
        end_time = time.time()
        print(f"Day {day_number} generation completed in {(end_time - start_time):.2f} seconds.")
        return expected_metrics

    def _validate_day_files(self, day_number: int, expected_metrics: Dict[str, float]) -> bool:
        """
        2.5. Validates file integrity against pre-calculated metrics.
        """
        print(f"\nValidating files for Day {day_number}...")
        
        # ... (Validation logic remains V0.007 compliant)
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

        mp3_files = {
            "track_A": day_output_dir / "track_A.mp3",
            "track_B": day_output_dir / "track_B.mp3",
            "track_C": day_output_dir / "track_C.mp3",
        }

        # LOOP MP3 Files
        for track_name, mp3_path in mp3_files.items():
            expected_duration_ms = expected_metrics[f"{track_name.lower()}_duration_ms"]

            # Check File Size
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

        # Success
        print(f"Day {day_number} validation SUCCEEDED.")
        return True

    # --- 1. High-Level Application Flow ---
    
    def run_orchestration(self):
        """
        1. Orchestrates the entire process, including the new V0.008 Day-Level Cache Check.
        """
        print("--- Starting V0.008 Language Learner Orchestration ---")

        total_items = len(self.data_source)
        items_per_day = 25
        num_days = (total_items + items_per_day - 1) // items_per_day

        for day_number in range(1, num_days + 1):
            start_index = (day_number - 1) * items_per_day
            end_index = day_number * items_per_day
            day_items = self.data_source[start_index:end_index]
            
            if not day_items:
                break
            
            # --- 3. Check Day Cache ---
            if self._check_day_cache_hit(day_number):
                # --- 4. Cache HIT ---
                print(f"\nDay {day_number} output files found. Skipping generation (Day Cache HIT).")
                
                # IMPORTANT: Since we skipped generation, we must re-run the metric calculation 
                # (which is fast) to get expected_metrics for validation (Step 6).
                # This prevents a validation failure.
                print("Recalculating expected metrics for validation...")
                # The logic below performs the calculation without creating the large audio objects.
                # It's an internal optimization for V0.008's Day-Cache HIT path.
                
                expected_metrics = {
                    "track_a_duration_ms": 0.0,
                    "track_b_duration_ms": 0.0,
                    "track_c_duration_ms": 0.0,
                    "csv_row_count": float(len(day_items))
                }
                
                for item in day_items:
                    # Look up the cached audio duration for L1 and L2 (Item Cache HITs guaranteed)
                    audio_l1 = self._get_or_generate_audio(item["item_id"], item["l1_text"], "L1")
                    audio_l2 = self._get_or_generate_audio(item["item_id"], item["l2_text"], "L2")
                    
                    adjusted_l1 = self._adjust_speed(audio_l1, L1_SPEED_FACTOR) 
                    adjusted_l2 = self._adjust_speed(audio_l2, L2_SPEED_FACTOR)

                    dur_adj_l1 = len(adjusted_l1)
                    dur_adj_l2 = len(adjusted_l2)
                    
                    item_duration_A = dur_adj_l1 + PAUSE_L1_MS + dur_adj_l2
                    item_duration_B = item_duration_A + PAUSE_L2_MS + dur_adj_l2
                    item_duration_C = dur_adj_l2 + PAUSE_L2_MS + dur_adj_l2

                    expected_metrics["track_a_duration_ms"] += item_duration_A
                    expected_metrics["track_b_duration_ms"] += item_duration_B
                    expected_metrics["track_c_duration_ms"] += item_duration_C
                
            else:
                # --- 5. Cache MISS (Full generation required) ---
                expected_metrics = self._create_and_save_day_files(day_number, day_items)

            # --- 6. Validate Files ---
            validation_success = self._validate_day_files(day_number, expected_metrics)
            
            # --- 7. Halt or Continue ---
            if not validation_success:
                print("\n\n!!! PROCESS HALTED DUE TO INTEGRITY FAILURE !!!")
                return

        print("\n--- V0.008 Orchestration Complete. All days processed and validated. ---")


if __name__ == "__main__":
    # Example usage:
    # First run (Scenario 1) will create files and populate cache.
    # Second run (Scenario 2) will trigger the fast Day Cache HIT logic.
    workflow = LanguageLearnerWorkflow()
    workflow.run_orchestration()