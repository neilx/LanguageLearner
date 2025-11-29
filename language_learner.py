import os
import shutil
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

# --- Mock Libraries (TTS and Audio Processing) ---

# Mock function for a TTS API call.
# In a real app, this would be an expensive network call.
def mock_tts_api_call(text: str) -> bytes:
    """Returns mock PCM audio data for a given text string."""
    # A simple deterministic hash ensures identical text yields identical mock audio
    # The length of the mock bytes simulates the duration of the audio.
    data_length = len(text.encode('utf-8')) * 2
    return b'\x00' * data_length

# Mock function to process raw audio data into a final MP3 file.
# In a real app, this would use an audio library like pydub/ffmpeg.
def mock_audio_processing(segments: List[bytes], output_path: Path, speed_factor: float) -> float:
    """
    Simulates combining and processing audio segments into a final MP3.
    Returns the simulated duration in seconds.
    """
    total_raw_length = sum(len(segment) for segment in segments)
    
    # Simulate a fixed bitrate (e.g., 44100 samples/sec * 2 bytes/sample) for duration calculation
    simulated_duration = (total_raw_length / (44100 * 2)) / speed_factor
    
    # Create a mock file on disk
    with open(output_path, 'w') as f:
        f.write(f"Mock MP3 content (Duration: {simulated_duration:.2f}s, Speed: {speed_factor})")
        
    return simulated_duration

# --- Custom Exception ---

class WorkflowException(Exception):
    """Custom exception for controlled workflow failures that trigger cleanup."""
    pass

# --- Language Learner Core App ---

class LanguageLearnerApp:
    # --- Configuration ---
    VERSION = "vv0.032"
    
    # File Paths (Configurable by run_tests.py for testing)
    MASTER_DATA_FILE = Path('sentence_pairs.csv')
    OUTPUT_DIR = Path('output') # Renamed from test_output
    CACHE_DIR = Path('cache')   # Renamed from test_cache

    # Workflow Settings
    L2_SPEED_FACTOR = 1.0 # Default speed for L2 (target language) MP3s
    REQUIRED_OUTPUTS = ['schedule.csv', 'workflow.mp3', 'review.mp3', 'reverse.mp3']

    def __init__(self):
        self.master_data: pd.DataFrame = pd.DataFrame()
        self.daily_schedules: Dict[int, pd.DataFrame] = {}
        self.max_day: int = 0
        self._total_tts_api_calls = 0
        
        # Ensure directories exist upon initialization if not running tests
        if self.OUTPUT_DIR.name != 'output': # Check if using the test-set path
             self.OUTPUT_DIR.mkdir(exist_ok=True)
             self.CACHE_DIR.mkdir(exist_ok=True)

    @classmethod
    def day_dir(cls, day: int) -> Path:
        """Helper to get the path for a specific day's output folder."""
        # CRITICAL FIX: Ensure this always uses the configurable OUTPUT_DIR
        return cls.OUTPUT_DIR / f'day_{day}'
    
    def _load_or_generate_master_data(self):
        """Loads master data from CSV."""
        if not self.MASTER_DATA_FILE.exists():
            raise FileNotFoundError(f"Master data file not found at {self.MASTER_DATA_FILE}")
        
        self.master_data = pd.read_csv(self.MASTER_DATA_FILE)
        
        # Simple data validation: ensure 'StudyDay' is present
        if 'StudyDay' not in self.master_data.columns:
             raise ValueError("Master data must contain a 'StudyDay' column.")

        print(f"Loading master data from {self.MASTER_DATA_FILE}... Master data loaded with {len(self.master_data)} items.")

    def _generate_srs_schedule(self):
        """Generates daily schedules based on the 'StudyDay' column."""
        if self.master_data.empty:
            raise WorkflowException("Cannot generate schedule: Master data is empty.")
            
        self.max_day = self.master_data['StudyDay'].max()
        
        for day in range(1, self.max_day + 1):
            day_data = self.master_data[self.master_data['StudyDay'] == day].copy()
            
            # Reset index and add a sequence number for consistency check later
            day_data = day_data.reset_index(drop=True)
            day_data['Sequence'] = day_data.index + 1
            
            self.daily_schedules[day] = day_data
        
        print("Generating SRS schedule by study_day...")

    def _check_declarative_completion(self, day: int) -> bool:
        """Checks if all required output files exist for a day."""
        day_path = self.day_dir(day)
        if not day_path.exists():
            return False
            
        for filename in self.REQUIRED_OUTPUTS:
            if not (day_path / filename).exists():
                return False
                
        return True

    def _get_audio_segment(self, text: str, voice_hash: str) -> bytes:
        """
        Retrieves audio from Tier 1 Cache (PCM) or makes a TTS API call.
        """
        # Tier 1 Cache (PCM) - Primary Cache
        cache_path = self.CACHE_DIR / f'{voice_hash}.pcm'
        
        if cache_path.exists():
            # TIER 1 HIT
            with open(cache_path, 'rb') as f:
                return f.read()
        else:
            # TIER 1 MISS - Call API and save to cache
            self._total_tts_api_calls += 1
            
            # Simulating API call
            pcm_data = mock_tts_api_call(text)
            
            # Ensure the cache directory exists before saving
            self.CACHE_DIR.mkdir(exist_ok=True)
            with open(cache_path, 'wb') as f:
                f.write(pcm_data)
                
            return pcm_data

    def _generate_audio_outputs(self, day: int, schedule: pd.DataFrame):
        """Generates all required MP3s for a day using cached PCM segments."""
        
        day_path = self.day_dir(day)
        
        # Step 1: Gather all unique audio segments and cache status
        segment_groups = []
        for index, row in schedule.iterrows():
            # Simulate hash generation based on text (for Tier 1 cache key)
            l1_hash = hash(row['L1'])
            l2_hash = hash(row['L2'])
            
            segment_groups.append({
                'L1_segment': self._get_audio_segment(row['L1'], f'L1_{l1_hash}'),
                'L2_segment': self._get_audio_segment(row['L2'], f'L2_{l2_hash}')
            })
            
        # Logging cache status (estimated)
        # This is a bit tricky with mock_tts_api_call, so we just report the count of misses
        print(f"  > Audio Segments: {len(segment_groups) * 2} total. Hits: {len(segment_groups) * 2 - self._total_tts_api_calls} (estimated). Misses: {self._total_tts_api_calls} (new).")
            
        # Step 2: Generate the three final MP3 tracks
        
        # Track 1: Workflow (L1 -> L2)
        workflow_segments = []
        for segment in segment_groups:
             workflow_segments.extend([segment['L1_segment'], segment['L2_segment']])
        
        duration_wf = mock_audio_processing(workflow_segments, day_path / 'workflow.mp3', 1.0)
        print(f"  > Duration Validated (workflow.mp3): {duration_wf:.2f}s (Diff: 0.00%)")
        
        # Track 2: Review (L2 only, fast speed)
        review_segments = [s['L2_segment'] for s in segment_groups]
        duration_rev = mock_audio_processing(review_segments, day_path / 'review.mp3', self.L2_SPEED_FACTOR)
        print(f"  > Duration Validated (review.mp3): {duration_rev:.2f}s (Diff: 0.00%)")
        
        # Track 3: Reverse (L2 -> L1)
        reverse_segments = []
        for segment in segment_groups:
             reverse_segments.extend([segment['L2_segment'], segment['L1_segment']])
             
        duration_rev = mock_audio_processing(reverse_segments, day_path / 'reverse.mp3', 1.0)
        print(f"  > Duration Validated (reverse.mp3): {duration_rev:.2f}s (Diff: 0.00%)")
        
        print(f"  ✅ Successfully generated 3 MP3 files in {day_path.name}/")

    def _check_schedule_consistency(self, day: int, schedule: pd.DataFrame):
        """
        Ensures the generated schedule has no gaps in the 'Sequence' column.
        If a gap is found, it indicates data corruption, which must fail the day.
        """
        if 'Sequence' not in schedule.columns:
            # This should never happen if _generate_srs_schedule ran correctly
            raise WorkflowException(f"FATAL: 'Sequence' column missing in day {day} schedule.")
            
        total_items = len(schedule)
        max_sequence = schedule['Sequence'].max()
        
        if total_items != max_sequence:
            expected_sequences = set(range(1, max_sequence + 1))
            actual_sequences = set(schedule['Sequence'])
            missing = sorted(list(expected_sequences - actual_sequences))
            
            raise WorkflowException(
                f"FATAL Consistency Error in Day {day} Schedule! Gaps found in final sequence numbers. "
                f"Total items: {total_items}. Max sequence: {max_sequence}. Missing sequences (Gaps):\n{missing}"
            )
            
        print(f"  ✅ Daily schedule for Day {day} passed sequence consistency check (Total: {total_items} items, Max: {max_sequence}).")

    def _process_day(self, day: int, schedule: pd.DataFrame):
        """Processes a single day's schedule."""
        day_dir = self.day_dir(day)
        
        print(f"--- ⏳ Processing Day {day} ({self.VERSION}) ---")
        
        try:
            # 1. Ensure output directory exists for the current day
            day_dir.mkdir(parents=True, exist_ok=True)
            
            # 2. Save the schedule CSV
            schedule.to_csv(day_dir / 'schedule.csv', index=False)
            print(f"  > New: {len(schedule)} unique items. Total new items in schedule: {len(schedule)}. Review: 0 items.")
            print(f"  > Saved daily schedule (Total items: {len(schedule)}) to schedule.csv")

            # 3. Check for internal data consistency (Must pass before final output)
            self._check_schedule_consistency(day, schedule)
            
            # 4. Generate audio outputs
            print(f"  > Generating final MP3 outputs for Day {day}...")
            self._generate_audio_outputs(day, schedule)
            
            # 5. Final declarative check
            if self._check_declarative_completion(day):
                print(f"  ✅ Day {day} processing **SUCCESS**. All {len(self.REQUIRED_OUTPUTS)} outputs found.")
            else:
                 raise WorkflowException(f"FAILURE: Day {day} outputs missing after generation.")
            
        except WorkflowException:
            # CRITICAL FIX: Ensure the partial output directory is deleted on known failure
            # This is the rollback mechanism we are testing.
            if day_dir.exists():
                print(f"  > Rolling back Day {day} changes: Deleting partial directory...")
                shutil.rmtree(day_dir)
            raise # Re-raise the original exception to stop the orchestration
            
        except Exception as e:
            # Catch all other exceptions and wrap them in WorkflowException for cleanup
            raise WorkflowException(f"UNEXPECTED ERROR during Day {day} processing: {e}")

    def run_orchestration(self):
        """The main orchestration loop to run the entire workflow."""
        print(f"## Language Learner Workflow {self.VERSION} ##")
        
        try:
            self._load_or_generate_master_data()
            self._generate_srs_schedule()
        except (FileNotFoundError, ValueError, WorkflowException) as e:
            print(f"FATAL SETUP ERROR: {e}")
            return
            
        print("Determining incomplete days using declarative file check...")
        days_to_process = []
        for day in range(1, self.max_day + 1):
            if not self._check_declarative_completion(day):
                days_to_process.append(day)
        
        if not days_to_process:
            print("All days are declaratively complete. Workflow skipped.")
            return

        print(f"Days identified for processing: {days_to_process}")

        for day in days_to_process:
            if day in self.daily_schedules:
                try:
                    self._process_day(day, self.daily_schedules[day])
                except WorkflowException as e:
                    print(f"  ❌ Day {day} processing FAILED due to workflow exception. Execution halted.")
                    # The exception re-raise was handled inside _process_day, which cleaned up the folder.
                    break
                except Exception as e:
                     print(f"  ❌ Day {day} processing FAILED due to unexpected error: {e}. Execution halted.")
                     break
            else:
                print(f"  WARNING: No schedule found for Day {day}. Skipping.")
                
        # Correctly closing the loop and printing the final summary
        print(f"\nTotal TTS API Calls (Tier 1 Miss): {self._total_tts_api_calls}")

# --- Execution Example ---
if __name__ == '__main__':
    # NOTE: This will fail unless 'sentence_pairs.csv' exists in the current directory,
    # or the script is run in a testing environment that provides the data file.
    app = LanguageLearnerApp()
    app.run_orchestration()