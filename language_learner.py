import pandas as pd
import numpy as np
from pathlib import Path
import csv
import random
import os
import hashlib
from typing import List, Dict, Any

# --- External Audio Libraries ---
try:
    from gtts import gTTS
    from pydub import AudioSegment
    # NOTE: The problematic 'SilenceSegment' import has been removed.
except ImportError:
    print("Error: Required libraries (gTTS, pydub) not found.")
    print("Please ensure you installed them in your Thonny environment.")
    exit()

# --- CRITICAL: FFmpeg Path Configuration ---
# You confirmed FFmpeg is installed. If the script fails, double-check this path.
# REPLACE this example path with the actual location of your ffmpeg/bin folder
try:
    os.environ["PATH"] += os.pathsep + r'C:\Program Files\ffmpeg\bin' 
except:
    pass

# --- 4. Constants (for Python Script) ---
WORKFLOW_VERSION = 'v0.023'
REVIEW_RATIO = 5  # New Items to Review Items (5:1)
EXPECTED_FILE_COUNT = 4  # 1 CSV + 3 MP3s
L2_SPEED_FACTOR = 1.4
PAUSE_L1_MS = 500
PAUSE_L2_MS = 1000
PAUSE_SHADOW_MS = 2500 # Extra long pause for review shadowing

# --- Configuration (NFR-6 IMMUTABLE NAMES) ---
MASTER_DATA_FILE = Path('sentence_pairs.csv')
OUTPUT_DIR = Path('output') 
CACHE_DIR = Path('cache')   

# Ensure directories exist
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True) 

# Define the order of columns (as per 2.1)
SCHEDULE_COLUMNS = [
    'item_id', 'w2', 'w1', 'l1_text', 'l2_text', 'study_day'
]
# Expected output files (excluding the schedule.csv)
EXPECTED_FILES = [
    'workflow.mp3', 'review.mp3', 'reverse.mp3', 'schedule.csv'
]

# --- Cache and Audio Utility Functions ---

def _get_cached_audio_path(text: str) -> Path:
    """NFR-5: Generates a unique filename (MD5 hash) for the text content."""
    hash_object = hashlib.md5(text.strip().lower().encode('utf-8'))
    filename = f"{hash_object.hexdigest()}.mp3"
    return CACHE_DIR / filename

def _apply_speed_change(segment: AudioSegment, speed: float) -> AudioSegment:
    """Adjusts the playback speed of an AudioSegment by changing the frame rate."""
    if speed == 1.0:
        return segment
    return segment.set_frame_rate(int(segment.frame_rate * speed))

def _tts_generate_and_cache(text: str, lang: str, is_l2: bool) -> AudioSegment:
    """Generates TTS audio, uses the cache if available, and applies speed factor."""
    cache_path = _get_cached_audio_path(text)
    
    if cache_path.is_file():
        audio_segment = AudioSegment.from_mp3(cache_path)
    else:
        print(f"    - Cache miss for {lang} text (hash: {cache_path.name[:8]}...). Synthesizing and caching.")
        
        temp_file_path = CACHE_DIR / f'temp_{cache_path.name}'
        
        tts = gTTS(text=text, lang=lang, slow=False)
        tts.save(temp_file_path)
        
        audio_segment = AudioSegment.from_mp3(temp_file_path)
        
        if is_l2:
            audio_segment = _apply_speed_change(audio_segment, L2_SPEED_FACTOR)

        audio_segment.export(cache_path, format="mp3")
        
        os.remove(temp_file_path)

    return audio_segment

# --- Workflow Functions ---

def _load_or_generate_master_data(df: pd.DataFrame) -> pd.DataFrame:
    """1. Load/Generate Master Data."""
    print(f"Loading master data from {MASTER_DATA_FILE}...")
    
    if MASTER_DATA_FILE.exists():
        master_df = pd.read_csv(MASTER_DATA_FILE)
    else:
        print("Master data file not found. Generating dummy data for simulation.")
        data = {
            'item_id': list(range(1, 61)),
            'w2': [f'danish_word_{i}' for i in range(1, 61)],
            'w1': [f'english_word_{i}' for i in range(1, 61)],
            'l1_text': [f'This is the English sentence containing english_word_{i}.' for i in range(1, 61)],
            'l2_text': [f'Dette er den danske sætning, der indeholder danish_word_{i}.' for i in range(1, 61)],
            'study_day': (np.arange(60) // 10) + 1 
        }
        master_df = pd.DataFrame(data, columns=SCHEDULE_COLUMNS)
        master_df.to_csv(MASTER_DATA_FILE, index=False)

    master_df['study_day'] = master_df['study_day'].astype(int)
    print(f"Master data loaded with {len(master_df)} items.")
    return master_df

def _generate_srs_schedule(master_df: pd.DataFrame) -> Dict[int, pd.DataFrame]:
    """2. Generate SRS Schedule."""
    print("Generating SRS schedule by study_day...")
    schedule_by_day = master_df.groupby('study_day')
    return {day: df for day, df in schedule_by_day}

def _determine_days_to_process(max_day: int) -> List[int]:
    """3. Determine Days to Process (Declarative Filter)."""
    print("Determining incomplete days using declarative file check...")
    days_to_process = []
    
    for day in range(1, max_day + 1):
        day_folder = OUTPUT_DIR / f'day_{day}'
        
        if not day_folder.is_dir():
            days_to_process.append(day)
            continue

        missing_expected_file = False
        for expected_file in EXPECTED_FILES:
            if not (day_folder / expected_file).is_file():
                missing_expected_file = True
                break

        if missing_expected_file:
            days_to_process.append(day)
            
    print(f"Days identified for processing: {days_to_process}")
    return days_to_process

def _get_all_past_items(processed_days: List[int]) -> pd.DataFrame:
    """Retrieves all items from schedule.csv files for completely processed days."""
    all_past_items = []
    
    for day in processed_days:
        schedule_path = OUTPUT_DIR / f'day_{day}' / 'schedule.csv'
        if schedule_path.is_file():
            try:
                df = pd.read_csv(schedule_path)
                all_past_items.append(df)
            except pd.errors.EmptyDataError:
                pass
    
    if all_past_items:
        return pd.concat(all_past_items, ignore_index=True)
    else:
        return pd.DataFrame(columns=SCHEDULE_COLUMNS)

def _generate_daily_mp3s(day_df: pd.DataFrame, day: int) -> None:
    """Generates and saves the three required MP3 files for the day."""
    day_folder = OUTPUT_DIR / f'day_{day}'
    print(f"  > Generating final MP3 outputs for Day {day}...")
    
    # Corrected silence creation using AudioSegment.silent()
    pause_l1 = AudioSegment.silent(duration=PAUSE_L1_MS) 
    pause_l2 = AudioSegment.silent(duration=PAUSE_L2_MS) 
    pause_shadow = AudioSegment.silent(duration=PAUSE_SHADOW_MS)

    workflow_audio = AudioSegment.empty()
    review_audio = AudioSegment.empty()
    reverse_audio = AudioSegment.empty()

    for _, row in day_df.iterrows():
        # 1. Get/Cache Audio Segments
        l1_segment = _tts_generate_and_cache(row['l1_text'], 'en', is_l2=False)
        l2_segment = _tts_generate_and_cache(row['l2_text'], 'da', is_l2=True)

        # 2. Build the three distinct audio tracks
        workflow_audio += l1_segment + pause_l1 + l2_segment + pause_l2
        review_audio += l2_segment + pause_shadow
        reverse_audio += l2_segment + pause_l2 + l1_segment + pause_l2

    # 3. Export the final combined tracks
    workflow_audio.export(day_folder / 'workflow.mp3', format='mp3')
    review_audio.export(day_folder / 'review.mp3', format='mp3')
    reverse_audio.export(day_folder / 'reverse.mp3', format='mp3')
    
    print(f"  ✅ Successfully generated 3 MP3 files in {day_folder.name}/")


def _process_day(
    day: int, 
    new_items_df: pd.DataFrame, 
    processed_days: List[int]
) -> None:
    """4. Process Missing Days: Performs core processing for one incomplete day."""
    day_folder = OUTPUT_DIR / f'day_{day}'
    day_folder.mkdir(exist_ok=True) 

    print(f"\n--- ⏳ Processing Day {day} (v{WORKFLOW_VERSION}) ---")
    
    # 1. Select New Items and Review Items
    new_count = len(new_items_df)
    review_count = new_count // REVIEW_RATIO
    past_items_df = _get_all_past_items(processed_days)
    
    if len(past_items_df) > 0 and review_count > 0:
        review_items_df = past_items_df.sample(n=min(review_count, len(past_items_df)))
    else:
        review_items_df = pd.DataFrame(columns=SCHEDULE_COLUMNS)

    print(f"  > New: {new_count} items. Review: {len(review_items_df)} items.")

    # 2. Combine and Interleave (Shuffle)
    daily_schedule_df = pd.concat([new_items_df, review_items_df], ignore_index=True)
    daily_schedule_df = daily_schedule_df.sample(frac=1).reset_index(drop=True)
    
    # 3. Generate schedule.csv
    schedule_path = day_folder / 'schedule.csv'
    daily_schedule_df.to_csv(schedule_path, index=False, columns=SCHEDULE_COLUMNS)
    print(f"  > Saved daily schedule (Total items: {len(daily_schedule_df)}) to {schedule_path.name}")
    
    # 4. Generate MP3s (using the full audio implementation)
    _generate_daily_mp3s(daily_schedule_df, day)
    
    # 5. Validation
    current_file_count = len([f for f in day_folder.iterdir() if f.is_file() and not f.name.startswith('.')])
    if current_file_count == EXPECTED_FILE_COUNT:
        print(f"  ✅ Day {day} processing **SUCCESS**. All {EXPECTED_FILE_COUNT} outputs found.")
    else:
        print(f"  ❌ Day {day} processing **FAILED**. Found {current_file_count}/{EXPECTED_FILE_COUNT} files.")


# --- Main Execution ---

def run_workflow() -> None:
    """Orchestrates the four-step application flow."""
    print(f"## Language Learner Workflow v{WORKFLOW_VERSION} ##")
    
    # 1. Load/Generate Master Data
    master_df = _load_or_generate_master_data(pd.DataFrame())

    # 2. Generate SRS Schedule (Group by day)
    schedule_by_day = _generate_srs_schedule(master_df)
    max_day = max(schedule_by_day.keys()) if schedule_by_day else 0
    
    all_scheduled_days = set(schedule_by_day.keys())
    
    # 3. Determine Days to Process (Declarative Filter)
    days_to_process = _determine_days_to_process(max_day)
    
    processed_days = sorted(list(all_scheduled_days - set(days_to_process)))
    print(f"Days currently considered 'complete' for review item selection: {processed_days}")

    # 4. Process Missing Days
    if not days_to_process:
        print("\n--- ✅ All Scheduled Days Are Complete ---")
        return

    for day in sorted(days_to_process):
        if day in schedule_by_day:
            new_items_df = schedule_by_day[day]
            _process_day(day, new_items_df, processed_days)
            
            current_day_folder = OUTPUT_DIR / f'day_{day}'
            current_file_count = len([f for f in current_day_folder.iterdir() if f.is_file() and not f.name.startswith('.')])
            if current_file_count == EXPECTED_FILE_COUNT:
                 processed_days.append(day)
                 processed_days = sorted(list(set(processed_days)))
        else:
            print(f"Warning: Day {day} was marked for processing but has no new items in master data.")


if __name__ == '__main__':
    run_workflow()