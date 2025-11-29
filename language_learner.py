import pandas as pd
import numpy as np
import os
import shutil
import hashlib
import time
import random
from io import BytesIO

# Third-party libraries for audio manipulation
try:
    from pydub import AudioSegment
    # Check for FFMPEG dependency
    AudioSegment.converter = "ffmpeg"
except ImportError:
    print("Error: pydub is not installed. Run 'pip install pydub'")
    exit()
except FileNotFoundError:
    print("Error: FFMPEG is required by pydub but not found.")
    print("Please install FFMPEG and ensure it's in your system PATH.")
    exit()

# Third-party library for TTS
try:
    # Using the free gTTS API
    from gtts import gTTS
    from gtts.tts import gTTSError 
except ImportError:
    print("Warning: The 'gTTS' package is not installed or importable.")
    print("The script will run only in MOCK_MODE = True until installed.")

# --- 1. CONSTANTS AND CONFIGURATION ---

# **TOGGLE SET:** Set to False to use the real, free gTTS API.
MOCK_MODE = False

# File Paths
INPUT_CSV = "sentence_pairs.csv"
OUTPUT_DIR_BASE = "output"
CACHE_DIR = "cache"
RAW_PCM_CACHE = os.path.join(CACHE_DIR, "tts_raw")
MP3_CACHE = os.path.join(CACHE_DIR, "mp3")

# Audio Parameters
PCM_SAMPLE_RATE = 24000  # Target sample rate (Hz)
PCM_CHANNELS = 1         # Mono channel count
PCM_SAMPLE_WIDTH = 2     # 16-bit signed PCM (2 bytes)

# Speed and Pauses
L1_SPEED_FACTOR = 1.1
L2_SPEED_FACTOR = 1.3
PAUSE_L1_MS = 500  # Short pause (500ms)
PAUSE_L2_MS = 1000 # Long pause (1000ms)

# SRS Parameters
MICRO_REPETITION_FACTOR = 3 
REVIEW_RATIO = 5           
MIN_REVIEW = 1             
INTERLEAVE_FREQUENCY = 5   

# Workflow Parameters
VALIDATION_TOLERANCE = 0.05 
MAX_RETRIES = 5             

# --- 2. CUSTOM EXCEPTION ---

class WorkflowException(Exception):
    """Custom exception for workflow failures that require cleanup."""
    pass

# --- 3. TTS API WRAPPER (CONDITIONAL MOCKING & gTTS IMPLEMENTATION) ---

class GeminiTTSAPI:
    """
    A wrapper class for the Text-to-Speech API, using conditional mocking and gTTS.
    """
    
    def __init__(self, mock_mode: bool): 
        self.mock_mode = mock_mode
        
        if not self.mock_mode:
            print("Using gTTS (Google Translate TTS) for real API calls.")
        
        # Stats tracking
        self._total_tts_api_calls = 0
        self._total_raw_pcm_cache_hits = 0
        self._total_mp3_cache_hits = 0

    def _generate_synthetic_pcm(self, text: str) -> bytes:
        """
        MOCK IMPLEMENTATION: Simulates TTS by generating synthetic PCM audio.
        """
        text_len = len(text)
        duration_s = 0.5 + 0.05 * text_len
        num_samples = int(duration_s * PCM_SAMPLE_RATE)
        
        freq_factor = 200 + text_len * 5
        t = np.linspace(0, duration_s, num_samples, endpoint=False)
        audio_data = np.sin(2 * np.pi * freq_factor * t)
        
        max_int = 2**15 - 1
        audio_int = (audio_data * max_int).astype(np.int16)
        
        return audio_int.tobytes()

    def _call_real_tts_api(self, text: str, lang: str) -> bytes:
        """
        REAL API IMPLEMENTATION: Uses the gTTS package to generate raw PCM data.
        """
        
        # Map internal codes to gTTS standard language codes
        if lang == 'L1':
            # English voice for L1
            gtts_lang = 'en' 
        elif lang == 'L2':
            # Danish voice for L2
            gtts_lang = 'da'
        else:
            raise ValueError(f"Unknown language code for gTTS: {lang}")
            
        try:
            # 1. Generate MP3 into a memory buffer using gTTS
            tts = gTTS(text=text, lang=gtts_lang)
            mp3_fp = BytesIO()
            tts.write_to_fp(mp3_fp)
            mp3_fp.seek(0)
            
            # 2. Use pydub to load the MP3 from the buffer
            audio_segment = AudioSegment.from_file(mp3_fp, format="mp3")
            
            # 3. Export the AudioSegment as raw PCM bytes matching the target parameters
            raw_pcm_bytes = audio_segment.set_frame_rate(PCM_SAMPLE_RATE).set_channels(PCM_CHANNELS).set_sample_width(PCM_SAMPLE_WIDTH).raw_data
            
            return raw_pcm_bytes
            
        except gTTSError as e:
            # Catch specific gTTS errors (e.g., failed API call, network issue)
            raise IOError(f"gTTS API call failed: {e}") from e
        except Exception as e:
            # Catch other conversion/pydub errors
            raise IOError(f"Audio conversion failed after gTTS call: {e}") from e


    def _call_tts_service(self, text: str, lang: str) -> bytes:
        """
        Handles the TTS call, choosing between mock and real implementation, 
        and applying retry logic.
        """
        if self.mock_mode:
            tts_function = lambda t: self._generate_synthetic_pcm(t)
            simulate_error = lambda: random.random() < 0.1
        else:
            tts_function = lambda t: self._call_real_tts_api(t, lang)
            simulate_error = lambda: False 

        for attempt in range(MAX_RETRIES):
            try:
                # Mock failure simulation
                if self.mock_mode and simulate_error() and attempt < MAX_RETRIES - 1:
                    raise IOError("Mock API Call Failed (Simulated transient error)")

                raw_pcm_bytes = tts_function(text)
                self._total_tts_api_calls += 1
                return raw_pcm_bytes
            
            except IOError as e:
                # Catching IOError from mock or real API failure
                if attempt == MAX_RETRIES - 1:
                    print(f"Error: Final attempt failed for '{text}'.")
                    raise WorkflowException(f"TTS API failed after {MAX_RETRIES} attempts. Reason: {e}") from e
                
                sleep_time = 2 ** attempt
                print(f"Warning: TTS API call failed (attempt {attempt+1}). Retrying in {sleep_time}s...")
                time.sleep(sleep_time)

    def get_speed_adjusted_segment(self, text: str, lang: str, speed_factor: float) -> AudioSegment:
        """
        Retrieves/generates the speed-adjusted MP3 AudioSegment using two-tier caching.
        """
        # Note: The text_hash implicitly includes the language, because the lang is part of the hash input.
        text_hash = hashlib.sha256(f"{lang}:{text}".encode('utf-8')).hexdigest()
        speed_tag = f"{speed_factor:.2f}x".replace('.', 'p') 
        mp3_path = os.path.join(MP3_CACHE, f"{text_hash}_{speed_tag}.mp3")

        # Tier 2 Cache Check (MP3, speed-adjusted)
        if os.path.exists(mp3_path):
            self._total_mp3_cache_hits += 1
            return AudioSegment.from_mp3(mp3_path)

        # Tier 1 Cache Check (Raw PCM, non-adjusted)
        pcm_path = os.path.join(RAW_PCM_CACHE, f"{text_hash}.rawpcm")
        if os.path.exists(pcm_path):
            self._total_raw_pcm_cache_hits += 1
            with open(pcm_path, 'rb') as f:
                raw_pcm_bytes = f.read()
        else:
            # Call TTS service (gTTS)
            raw_pcm_bytes = self._call_tts_service(text, lang)
            
            # Cache Save (Tier 1)
            os.makedirs(RAW_PCM_CACHE, exist_ok=True)
            with open(pcm_path, 'wb') as f:
                f.write(raw_pcm_bytes)
        
        # Convert and Speed Adjust
        audio_segment = AudioSegment.from_raw(
            BytesIO(raw_pcm_bytes),
            sample_width=PCM_SAMPLE_WIDTH,
            frame_rate=PCM_SAMPLE_RATE,
            channels=PCM_CHANNELS
        )

        new_frame_rate = int(PCM_SAMPLE_RATE * speed_factor)
        adjusted_segment = audio_segment.set_frame_rate(new_frame_rate)

        # Tier 2 Cache Save
        os.makedirs(MP3_CACHE, exist_ok=True)
        adjusted_segment.export(
            mp3_path, 
            format="mp3", 
            parameters=["-q:a", "5"]
        )

        return adjusted_segment

# --- 4. SRS SCHEDULING LOGIC (Unchanged) ---

def _generate_review_schedule(df: pd.DataFrame, current_day: int, random_state: int) -> tuple[pd.DataFrame, int, int]:
    """
    Generates the day's schedule by interleaving new and review items.
    """
    df_pool = df[df['study_day'] <= current_day].copy()

    # 1. New Items
    new_items_df = df_pool[df_pool['study_day'] == current_day]
    unique_new_items = new_items_df.copy()
    
    # Repeat new items by the factor
    new_list = pd.concat([unique_new_items] * MICRO_REPETITION_FACTOR, ignore_index=True)
    actual_new_unique_count = len(unique_new_items)
    expected_new_count = len(new_list)

    # 2. Review Items
    review_pool_df = df_pool[df_pool['study_day'] < current_day]
    
    if not review_pool_df.empty:
        baseline_review = actual_new_unique_count / REVIEW_RATIO
        expected_count = max(MIN_REVIEW, int(np.ceil(baseline_review)))
        max_possible_review = len(review_pool_df)
        expected_review_count = min(expected_count, max_possible_review)

        random.seed(random_state)
        review_list = review_pool_df.sample(
            n=expected_review_count, 
            replace=False, 
            random_state=random_state
        )
    else:
        review_list = pd.DataFrame()
        expected_review_count = 0

    # 3. Interleaving
    schedule_list = []
    i_new, i_review = 0, 0
    
    new_records = new_list.to_dict('records')
    review_records = review_list.to_dict('records')
    
    while i_new < expected_new_count or i_review < expected_review_count:
        for _ in range(INTERLEAVE_FREQUENCY):
            if i_new < expected_new_count:
                schedule_list.append(new_records[i_new])
                i_new += 1
        
        if i_review < expected_review_count:
            schedule_list.append(review_records[i_review])
            i_review += 1
    
    schedule_df = pd.DataFrame(schedule_list)

    actual_new_count = len(schedule_df[schedule_df['study_day'] == current_day])
    actual_review_count = len(schedule_df[schedule_df['study_day'] < current_day])

    if actual_new_count != expected_new_count:
        raise WorkflowException(
            f"Schedule Validation Failed: Actual New ({actual_new_count}) != Expected New ({expected_new_count})"
        )
    if actual_review_count != expected_review_count:
        raise WorkflowException(
            f"Schedule Validation Failed: Actual Review ({actual_review_count}) != Expected Review ({expected_review_count})"
        )

    return schedule_df, expected_new_count, expected_review_count

# --- 5. MAIN WORKFLOW CLASS ---

class AudioRegenerationWorkflow:
    def __init__(self, input_csv: str, mock_mode: bool):
        self.input_csv = input_csv
        self.tts_api = GeminiTTSAPI(mock_mode=mock_mode) 
        self.pause1 = AudioSegment.silent(duration=PAUSE_L1_MS)
        self.pause2 = AudioSegment.silent(duration=PAUSE_L2_MS)
        self.data = self._load_and_validate_input()
        self._setup_directories()

    def _load_and_validate_input(self) -> pd.DataFrame:
        """Loads and validates the input CSV file."""
        if not os.path.exists(self.input_csv):
            raise FileNotFoundError(f"Input file not found: {self.input_csv}")
        
        df = pd.read_csv(self.input_csv)
        
        required_cols = ['item_id', 'l1_text', 'l2_text', 'study_day']
        if not all(col in df.columns for col in required_cols):
            raise WorkflowException(f"CSV missing required columns: {required_cols}")

        if not df['item_id'].is_unique:
            raise WorkflowException("CSV item_id column contains duplicate values.")
            
        return df

    def _setup_directories(self):
        """Ensures all required directories exist."""
        os.makedirs(CACHE_DIR, exist_ok=True)
        os.makedirs(RAW_PCM_CACHE, exist_ok=True)
        os.makedirs(MP3_CACHE, exist_ok=True)
        os.makedirs(OUTPUT_DIR_BASE, exist_ok=True)

    def _check_day_complete(self, day_output_dir: str) -> bool:
        """Checks if all expected final output files exist for a given day."""
        required_files = ["workout.mp3", "review.mp3", "reverse.mp3", "schedule.csv"]
        
        # Check if the directory exists and contains all required files
        if not os.path.isdir(day_output_dir):
            return False
            
        return all(os.path.exists(os.path.join(day_output_dir, f)) for f in required_files)

    def run(self, start_day: int, end_day: int):
        """Main execution loop for a range of days."""
        mode_str = "MOCKING" if self.tts_api.mock_mode else "REAL (gTTS)"
        print(f"--- Starting Audio Regeneration Workflow in {mode_str} Mode for Days {start_day} to {end_day} ---")
        
        for current_day in range(start_day, end_day + 1):
            day_output_dir = os.path.join(OUTPUT_DIR_BASE, f"day_{current_day}")
            print(f"\nProcessing Day {current_day}...")
            
            # CHECK 1: Skip if the final files are already present
            if self._check_day_complete(day_output_dir):
                print(f"  Day {current_day} SKIPPED: Final output files already exist in {day_output_dir}")
                continue
            
            try:
                # If we get here, the day is incomplete, so we ensure the directory exists and proceed.
                os.makedirs(day_output_dir, exist_ok=True)
                self._process_day(current_day, day_output_dir)
                print(f"Day {current_day} SUCCESS: Output saved to {day_output_dir}")
                
            except WorkflowException as e:
                print(f"Day {current_day} FAILED: {e}")
                print(f"Cleaning up partial output directory: {day_output_dir}")
                shutil.rmtree(day_output_dir, ignore_errors=True)
            
            except Exception as e:
                print(f"Day {current_day} CRITICAL FAILURE: {e}")
                shutil.rmtree(day_output_dir, ignore_errors=True)

        print("\n--- Workflow Complete ---")
        print(f"Total TTS API Calls (Tier 1 Miss): {self.tts_api._total_tts_api_calls}")
        print(f"Total RAW PCM Cache Hits (Tier 1 Hit): {self.tts_api._total_raw_pcm_cache_hits}")
        print(f"Total MP3 Cache Hits (Tier 2 Hit): {self.tts_api._total_mp3_cache_hits}")


    def _process_day(self, current_day: int, day_output_dir: str):
        """Generates the schedule, audio, and exports for a single day."""
        
        # 1. Generate Schedule
        schedule_df, exp_new, exp_review = _generate_review_schedule(self.data, current_day, current_day)
        schedule_df.to_csv(os.path.join(day_output_dir, "schedule.csv"), index=False)
        print(f"  Schedule: {len(schedule_df)} total items. New: {exp_new}, Review: {exp_review}")

        # 2. Audio Assembly Setup
        workout_track = AudioSegment.empty() 
        review_track = AudioSegment.empty()
        reverse_track = AudioSegment.empty()
        
        expected_total_duration_ms = 0
        
        # 3. Assemble Tracks
        print("  Assembling audio tracks...")
        for index, row in schedule_df.iterrows():
            l1_text = row['l1_text']
            l2_text = row['l2_text']

            # Get Audio Segments (Tier 2 check/generation)
            l1_audio = self.tts_api.get_speed_adjusted_segment(l1_text, 'L1', L1_SPEED_FACTOR)
            l2_audio = self.tts_api.get_speed_adjusted_segment(l2_text, 'L2', L2_SPEED_FACTOR)

            # Calculate Expected Duration
            expected_total_duration_ms += (
                l1_audio.duration_seconds * 1000 + 
                PAUSE_L1_MS + 
                l2_audio.duration_seconds * 1000 + 
                PAUSE_L2_MS
            )

            # Assemble Track Segments (5.1)
            workout_track += l1_audio + self.pause1 + l2_audio + self.pause2
            review_track += l2_audio + self.pause2 + l1_audio + self.pause1
            reverse_track += l1_audio + self.pause2 + l2_audio
        
        # 4. Audio Duration Validation (4.3)
        actual_duration_ms = len(workout_track)
        
        # Calculate tolerance range
        lower_bound = expected_total_duration_ms * (1 - VALIDATION_TOLERANCE)
        upper_bound = expected_total_duration_ms * (1 + VALIDATION_TOLERANCE)
        
        if not (lower_bound <= actual_duration_ms <= upper_bound):
            raise WorkflowException(
                f"Audio Duration Validation Failed: Actual ({actual_duration_ms:.0f}ms) is outside "
                f"tolerance ({lower_bound:.0f}ms - {upper_bound:.0f}ms)."
            )
        print(f"  Duration Validated: {actual_duration_ms / 1000:.2f}s (Expected: {expected_total_duration_ms / 1000:.2f}s)")


        # 5. Export (5.1)
        export_params = {"format": "mp3", "parameters": ["-q:a", "2"]}
        
        print("  Exporting tracks...")
        workout_track.export(os.path.join(day_output_dir, "workout.mp3"), **export_params)
        review_track.export(os.path.join(day_output_dir, "review.mp3"), **export_params)
        reverse_track.export(os.path.join(day_output_dir, "reverse.mp3"), **export_params)


# --- 6. SETUP AND SCENARIO TEST ---

def _cleanup_environment():
    """UTILITY: Removes ALL created directories and test data."""
    # This function is now only for manual use when a complete reset is needed.
    print("--- FULL ENVIRONMENT CLEANUP ---")
    if os.path.exists(OUTPUT_DIR_BASE):
        shutil.rmtree(OUTPUT_DIR_BASE)
        print(f"Removed {OUTPUT_DIR_BASE}/")
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)
        print(f"Removed {CACHE_DIR}/")
    if os.path.exists(INPUT_CSV):
        os.remove(INPUT_CSV)
        print(f"Removed {INPUT_CSV}")

def _create_revised_test_data():
    """Generates the test sentence_pairs.csv with 30 items for 3 days, ONLY if it doesn't exist."""
    if os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} already exists. Skipping creation.")
        return
        
    data = []
    
    for i in range(1, 31):
        item_id = f"item_{i:02d}"
        l1 = f"English sentence number {i}."
        l2 = f"Dansk sætning nummer {i}."
        study_day = 1 + (i - 1) // 10
        
        data.append({'item_id': item_id, 'l1_text': l1, 'l2_text': l2, 'study_day': study_day})

    df = pd.DataFrame(data)
    df.to_csv(INPUT_CSV, index=False)
    print(f"Created test data file: {INPUT_CSV} with {len(df)} items.")

if __name__ == '__main__':
    
    print(f"TTS API MOCK_MODE is set to: {MOCK_MODE}")

    # **CRITICAL CHANGE:** We no longer call _cleanup_environment() here.
    # The cache and output will be preserved for future runs.
    
    # Ensure the input data exists (creates it once, skips on subsequent runs)
    _create_revised_test_data()
    
    # Initialize and Run Workflow
    try:
        workflow = AudioRegenerationWorkflow(INPUT_CSV, mock_mode=MOCK_MODE)
        
        # Run the workflow. It will generate missing days or skip completed ones.
        # Use a single, comprehensive run command for stability:
        workflow.run(start_day=1, end_day=3)
        
        # Optional: If you wanted to run the full cleanup, you would call:
        # _cleanup_environment() 

    except (FileNotFoundError, WorkflowException) as e:
        print(f"\nCRITICAL SETUP ERROR: {e}")
        print("Workflow cannot proceed.")