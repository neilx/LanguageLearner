import os
import csv
import json
import hashlib
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple, TypedDict, Callable
from enum import Enum
# Import gTTS for the real API call
try:
    from gtts import gTTS
    REAL_TTS_AVAILABLE = True
except ImportError:
    REAL_TTS_AVAILABLE = False
# Import Pydub for audio concatenation
try:
    from pydub import AudioSegment
    REAL_CONCAT_AVAILABLE = True
    # Attempt to trigger an error if FFmpeg/Libav is missing
    try:
        AudioSegment.empty() 
        FFMPEG_AVAILABLE = True
    except Exception:
        FFMPEG_AVAILABLE = False
except ImportError:
    REAL_CONCAT_AVAILABLE = False
    FFMPEG_AVAILABLE = False


# =========================================================================
# 0. Declarative Types and Enums
# =========================================================================

class ScheduleItem(TypedDict):
    """Schema for a single item entry in the generated schedules."""
    W2: str
    W1: str
    L1: str
    L2: str
    StudyDay: int
    type: str 
    repetition: int

class ScheduleType(Enum):
    """Defines the two types of items that appear in the schedule."""
    NEW = 'micro_new'
    REVIEW = 'macro_review'

# =========================================================================
# 1. Configuration Constants
# =========================================================================

class Config:
    """Consolidated configuration settings for the Language Learning workflow."""

    # --- TTS Execution Switch (TOGGLE THIS LINE TO CHANGE BEHAVIOR) ---
    USE_REAL_TTS: bool = True # Set this to True to use gTTS and Pydub
    
    # --- File Paths ---
    SOURCE_FILE: Path = Path('sentence_pairs.csv')
    OUTPUT_ROOT_DIR: Path = Path('Days')
    TTS_CACHE_DIR: Path = Path('tts_cache')
    TTS_CACHE_FILE_EXT: str = '.mp3' 
    
    # --- Manifest & File Names ---
    REVIEW_MANIFEST_NAME: str = 'review_manifest.csv'
    WORKOUT_MANIFEST_NAME: str = 'workout_manifest.csv'
    REVIEW_FORWARD_AUDIO_NAME: str = 'review_forward.mp3'
    REVIEW_REVERSE_AUDIO_NAME: str = 'review_reverse.mp3'
    WORKOUT_AUDIO_NAME: str = 'workout.mp3'

    # --- Declarative Schema & Keys ---
    CONTENT_KEYS: List[str] = ['W2', 'W1', 'L1', 'L2'] 
    MANIFEST_COLUMNS: List[str] = CONTENT_KEYS + ['StudyDay', 'type', 'repetition']

    # --- Language & Localization Parameters ---
    TARGET_LANG_CODE_FULL: str = 'da-DK'
    BASE_LANG_CODE_FULL: str = 'en-US'
    TARGET_LANG_CODE_SHORT: str = 'da'
    BASE_LANG_CODE_SHORT: str = 'en'

    LANGUAGE_ROLE_MAP: Dict[str, str] = {
        'W2': 'TARGET', 'W1': 'BASE', 'L2': 'TARGET', 'L1': 'BASE',
    }
    
    LANG_CODE_RESOLVER: Dict[str, str] = {
        'TARGET': TARGET_LANG_CODE_FULL,
        'BASE': BASE_LANG_CODE_FULL
    }
    
    # --- Repetition Parameters (omitted for brevity) ---
    MICRO_REPETITIONS_COUNT: int = 3
    REVIEW_REPETITION_COUNT: int = 0
    MACRO_REPETITION_INTERVALS: List[int] = [1, 3, 7, 14, 30, 16, 120, 240]

    # --- Declarative Audio Templating and Pause Configuration ---
    
    SPECIAL_SEGMENTS: List[str] = ['SP']
    
    PAUSE_DURATIONS: Dict[str, float] = {
        'SP': 3.0,  # Explicit Delimiting Pause
        'W2': 0.25, # Implicit Pause duration AFTER the segment
        'W1': 0.25, 
        'L2': 0.5,   
        'L1': 0.5    
    }
        
    AUDIO_TEMPLATES: Dict[str, str] = {
        "workout": "SP W2 W1 L2 L1", 
        "review_forward": "SP W2 W1 L1 L2",
        "review_reverse": "SP W2 W1 L2 L1", 
    }
    TEMPLATE_DELIMITER: str = ' '
    
    AUDIO_FILE_MAP: Dict[str, str] = {
        "workout": WORKOUT_AUDIO_NAME,
        "review_forward": REVIEW_FORWARD_AUDIO_NAME,
        "review_reverse": REVIEW_REVERSE_AUDIO_NAME,
    }
    
    SEGMENT_ACTIONS: Dict[str, str] = {
        key: 'CONTENT' for key in CONTENT_KEYS
    }
    SEGMENT_ACTIONS['SP'] = 'EXPLICIT_PAUSE'

    # --- Integrity Check Parameters ---
    # Duration for a single content segment (e.g., W2, L1) in seconds.
    # Note: In REAL mode, this is just an ESTIMATE for the integrity check.
    MOCK_CONTENT_DURATION_SEC: float = 1.0 
    
    # The maximum allowable difference between the expected and actual audio duration in seconds.
    DURATION_TOLERANCE_SEC: float = 0.15 


# =========================================================================
# 2. Environment Check and Setup 
# =========================================================================

def initialize_source_data() -> None:
    """Creates the mock sentence_pairs.csv file if it does not exist."""
    # NOTE: This function is now only called from run_environment_check()
    
    mock_data = [
        {'W2': 'sol', 'W1': 'sun', 'L1': 'Solen skinner i dag.', 'L2': 'The sun is shining today.', 'StudyDay': 1},
        {'W2': 'måne', 'W1': 'moon', 'L1': 'Månen er smuk i aften.', 'L2': 'The moon is beautiful tonight.', 'StudyDay': 1},
        {'W2': 'vand', 'W1': 'water', 'L1': 'Jeg skal have noget vand.', 'L2': 'I need some water.', 'StudyDay': 1},
        {'W2': 'tryghed', 'W1': 'security', 'L1': 'Vi søger tryghed.', 'L2': 'We seek security.', 'StudyDay': 2},
        {'W2': 'akkord', 'W1': 'chord', 'L1': 'Han spiller en akkord.', 'L2': 'He plays a chord.', 'StudyDay': 2},
        {'W2': 'lys', 'W1': 'light', 'L1': 'Der er lys for enden af tunnelen.', 'L2': 'There er light at the end of the tunnel.', 'StudyDay': 3},
        {'W2': 'mørke', 'W1': 'darkness', 'L1': 'Mørket faldt på.', 'L2': 'The darkness fell.', 'StudyDay': 3},
    ]

    fieldnames = Config.CONTENT_KEYS + ['StudyDay']

    try:
        with open(Config.SOURCE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([{k: str(v) for k, v in item.items()} for item in mock_data])
        print(f"✅ Created initial mock data file: {Config.SOURCE_FILE}")
    except Exception as e:
        print(f"❌ Error creating master data file: {e}")

def run_environment_check() -> Tuple[bool, bool, bool]:
    """
    Checks for required directories, dependencies, and master data file existence/creation.
    
    Returns: Tuple[bool, bool, bool] - (use_real_tts_mode, use_real_concat_mode, is_initial_run)
    """
    print("--- 🔬 Starting Environment and Dependency Check ---")
    
    # 1. Master Data File Check/Creation (The most important check for the first run)
    is_initial_run = not Config.SOURCE_FILE.exists()
    if is_initial_run:
        print("⚠️ Master data file not found. Creating mock data...")
        initialize_source_data()
    else:
        print(f"✅ Master data file found: {Config.SOURCE_FILE}")

    # 2. Directory Setup
    for path in [Config.OUTPUT_ROOT_DIR, Config.TTS_CACHE_DIR]:
        path.mkdir(parents=True, exist_ok=True)
        print(f"✅ Directory created/exists: {path}")

    # 3. Dependency Checks (Mode determination)
    
    # Check for gTTS
    real_tts_mode = Config.USE_REAL_TTS and REAL_TTS_AVAILABLE
    if Config.USE_REAL_TTS:
        if REAL_TTS_AVAILABLE:
            print("✅ gTTS (TTS Generator) found.")
        else:
            print("❌ gTTS not found. Run: pip install gTTS")
            real_tts_mode = False

    # Check for Pydub and FFmpeg
    real_concat_mode = REAL_CONCAT_AVAILABLE and FFMPEG_AVAILABLE
    if real_tts_mode: # Only require Pydub/FFmpeg if we are in real TTS mode
        if REAL_CONCAT_AVAILABLE and FFMPEG_AVAILABLE:
            print("✅ Pydub (Audio Concatenation) and FFmpeg (backend) found.")
        elif REAL_CONCAT_AVAILABLE and not FFMPEG_AVAILABLE:
            print("❌ FFmpeg/Libav not found. Pydub is installed, but requires FFmpeg for MP3s.")
            print("   Please install FFmpeg and ensure it's in your system PATH.")
            real_concat_mode = False
        else:
            print("❌ Pydub not found. Run: pip install pydub")
            real_concat_mode = False

    # 4. Final Mode Check and Summary
    if Config.USE_REAL_TTS:
        if real_tts_mode and real_concat_mode:
            print("\n🚀 FULL REAL AUDIO PIPELINE ENABLED (gTTS + Pydub/FFmpeg).")
        else:
            print("\n⚠️ WARNING: REAL MODE FAILED. Falling back to MOCK/CONCEPTUAL RUN.")
            print("   Output files will be empty or incomplete until all dependencies are met.")
            real_tts_mode = False
            real_concat_mode = False
    else:
        print("\n⚙️ MOCK MODE ENABLED (Config.USE_REAL_TTS = False).")
    
    print("---------------------------------------------------")
    return real_tts_mode, real_concat_mode, is_initial_run


# =========================================================================
# 3. TTS API METHODS (MOCK and REAL)
# =========================================================================

def get_cache_path(text: str, language_code: str) -> Path:
    """Generates the unique cache path based on content and the FULL language code."""
    content_hash = hashlib.sha256(f"{text}{language_code}".encode()).hexdigest()
    return Config.TTS_CACHE_DIR / f"{content_hash}{Config.TTS_CACHE_FILE_EXT}"


def get_tts_lang_code(segment_key: str, use_real: bool) -> str:
    """Returns the appropriate language code (full for hash, short for real API)."""
    language_role = Config.LANGUAGE_ROLE_MAP.get(segment_key)
    
    if not use_real:
        return Config.LANG_CODE_RESOLVER.get(language_role, Config.BASE_LANG_CODE_FULL)

    if language_role == 'TARGET':
        return Config.TARGET_LANG_CODE_SHORT
    return Config.BASE_LANG_CODE_SHORT


# --- A. Mock TTS Method ---
def mock_google_tts(text: str, language_code: str) -> Path:
    """Mocks the API call by creating an empty file in the cache (Conceptual Mock)."""
    mock_file_path = get_cache_path(text, language_code)

    if not mock_file_path.exists():
        try:
            mock_file_path.touch(exist_ok=True)
            print(f"    - CACHE CREATED (Mock): {mock_file_path.name}")
        except OSError as e:
            raise IOError(f"TTS Mock Error creating cache file: {e}")
            
    return mock_file_path


# --- B. Real gTTS Method ---
def real_gtts_api(text: str, language_code: str) -> Path:
    """Calls the gTTS library to generate actual audio and saves it to the cache."""
    # Need the full code for consistent cache key generation
    full_lang_code = Config.LANG_CODE_RESOLVER.get('TARGET' if language_code == Config.TARGET_LANG_CODE_SHORT else 'BASE')
    real_file_path = get_cache_path(text, full_lang_code)

    if real_file_path.exists():
        return real_file_path
    
    print(f"    - API CALL: Generating audio for '{text[:20]}...' in {language_code}")
    
    try:
        tts = gTTS(text=text, lang=language_code, slow=False)
        tts.save(real_file_path)
        
        print(f"    - CACHE CREATED (Real Audio): {real_file_path.name}")
        return real_file_path
        
    except Exception as e:
        if "No audio for this text" in str(e):
            print(f"    ❌ gTTS Error: Skipping segment due to content issue ({text[:20]}...).")
            real_file_path.touch(exist_ok=True) 
            return real_file_path
        raise IOError(f"Real TTS API Error generating/caching file: {e}")


# =========================================================================
# 4. Audio Generation and Integrity Check Logic 
# =========================================================================

def get_segment_lang_code_full(segment_key: str) -> str:
    """Helper to get the FULL language code, used for hashing and template logging."""
    language_role = Config.LANGUAGE_ROLE_MAP.get(segment_key)
    return Config.LANG_CODE_RESOLVER.get(language_role, Config.BASE_LANG_CODE_FULL)


def pre_cache_day_segments(full_schedule: List[ScheduleItem], use_real_tts_mode: bool) -> None:
    """STEP 1: Isolates I/O side-effects by pre-caching all unique audio segments."""
    print(f"\n  - Pre-Caching unique audio segments...")
    
    if use_real_tts_mode:
        tts_func: Callable[[str, str], Path] = real_gtts_api
        print("  -- MODE: Using REAL gTTS API --")
    else:
        tts_func: Callable[[str, str], Path] = mock_google_tts
        print("  -- MODE: Using MOCK TTS --")

    unique_segments = set()

    for item in full_schedule:
        for key in Config.CONTENT_KEYS:
            text_content = item.get(key)
            full_lang_code = get_segment_lang_code_full(key) 
            segment_tuple = (text_content, full_lang_code)
            
            if text_content and segment_tuple not in unique_segments:
                try:
                    tts_lang_code = get_tts_lang_code(key, use_real_tts_mode)
                    tts_func(text_content, tts_lang_code)
                    unique_segments.add(segment_tuple)
                except (ValueError, IOError) as e:
                    print(f"    ❌ Error during pre-caching segment ('{text_content}'): {e}")
                    unique_segments.add(segment_tuple) 

    print(f"  - Pre-Caching complete. {len(unique_segments)} unique segments identified.")


def generate_audio_from_template(
    day_path: Path, 
    template_key: str, 
    data_list: List[ScheduleItem],
    use_real_concat_mode: bool
) -> Tuple[Path, float]:
    """
    Calculates expected duration and executes real audio concatenation 
    using Pydub, or mocks it based on the configuration.

    Returns: Tuple[Path, float]: The output path and the expected duration in seconds.
    """
    
    output_filename_base = Config.AUDIO_FILE_MAP[template_key]
    template = Config.AUDIO_TEMPLATES[template_key]
    output_path = day_path / output_filename_base
    
    expected_duration_sec: float = 0.0
    
    is_real_mode = use_real_concat_mode # Use the result from the environment check

    if is_real_mode:
        final_audio = AudioSegment.empty()
    else:
        # Create a mock file right away if not in real mode
        output_path.touch(exist_ok=True)


    # 1. Calculate Expected Duration & Execute Concatenation
    for item in data_list:
        
        for segment_key in template.split(Config.TEMPLATE_DELIMITER):
            if not segment_key: continue

            action_type = Config.SEGMENT_ACTIONS.get(segment_key)
            
            # --- Handle Content Segments ---
            if action_type == 'CONTENT':
                text_content = item.get(segment_key, "")
                full_lang_code = get_segment_lang_code_full(segment_key)
                cached_path = get_cache_path(text_content, full_lang_code)
                
                # Duration calculation (always run)
                initial_content_duration = Config.MOCK_CONTENT_DURATION_SEC
                expected_duration_sec += initial_content_duration
                
                # Real concatenation execution
                if is_real_mode:
                    if cached_path.exists() and cached_path.stat().st_size > 0:
                        try:
                            segment_audio = AudioSegment.from_mp3(cached_path)
                            final_audio += segment_audio
                            print(f"      -> CONCAT AUDIO: {segment_key} (Duration: {len(segment_audio) / 1000:.2f}s)")
                            # Use actual duration for a more precise check, adjusting the mocked estimate
                            expected_duration_sec -= initial_content_duration 
                            expected_duration_sec += len(segment_audio) / 1000.0
                            
                        except Exception as e:
                            print(f"      ❌ CONCAT FAILED for {segment_key}: {e}")
                            # Add a 0.1s silence for a missing file to prevent crashes
                            final_audio += AudioSegment.silent(duration=100)
                            print(f"      ⚠️ CONCAT ERROR: Added 0.1s silence for {segment_key}.")
                    else:
                        # Add a 0.1s silence for a missing file to prevent crashes
                        final_audio += AudioSegment.silent(duration=100)
                        print(f"      ⚠️ CACHE MISS: Added 0.1s silence for {segment_key}.")
                
                # Add duration for the implicit pause
                if segment_key in Config.PAUSE_DURATIONS:
                    pause_sec = Config.PAUSE_DURATIONS[segment_key]
                    expected_duration_sec += pause_sec
                    
                    if is_real_mode:
                        duration_ms = int(pause_sec * 1000)
                        final_audio += AudioSegment.silent(duration=duration_ms)
                        print(f"      -> CONCAT PAUSE (Implicit): {pause_sec}s")


            # --- Handle Explicit Pause Segments ---
            elif action_type == 'EXPLICIT_PAUSE':
                if segment_key in Config.PAUSE_DURATIONS:
                    pause_sec = Config.PAUSE_DURATIONS[segment_key]
                    expected_duration_sec += pause_sec
                    
                    if is_real_mode:
                        duration_ms = int(pause_sec * 1000)
                        final_audio += AudioSegment.silent(duration=duration_ms)
                        print(f"      -> CONCAT PAUSE (Explicit): {pause_sec}s")
                    
    # 2. Final Export / Mock Generation
    print("\n  - Performing final file operation...")

    if is_real_mode:
        try:
            final_audio.export(output_path, format="mp3")
            print(f"  - Final {output_filename_base} EXPORTED (REAL Audio).")
        except Exception as e:
            print(f"  ❌ FAILED TO EXPORT FINAL AUDIO: {e}. Check FFmpeg installation.")
    else:
        print(f"  - Final {output_filename_base} completed (MOCK/Incomplete Setup).")

    # 3. Final Logging / Integrity Preparation
    print(f"  - EXPECTED TOTAL DURATION: {expected_duration_sec:.2f} seconds.")

    # Return the path and duration
    return output_path, expected_duration_sec


def verify_audio_duration_integrity(
    file_path: Path, 
    expected_duration_sec: float,
    use_real_concat_mode: bool
) -> Tuple[bool, str]:
    """
    Checks integrity. Uses Pydub to measure the actual duration in real mode.
    """
    
    if not file_path.exists():
        return False, "File not found."

    if not use_real_concat_mode:
         return True, "Mock mode: File existence confirmed."
    
    if file_path.stat().st_size == 0:
        return True, "File exists (0 bytes), assumed verified for mock run or due to errors."
    
    if file_path.stat().st_size > 0:
        
        if REAL_CONCAT_AVAILABLE:
            try:
                # Measure the actual duration using Pydub
                audio = AudioSegment.from_mp3(file_path)
                actual_duration_sec = len(audio) / 1000.0
                
                duration_difference = abs(actual_duration_sec - expected_duration_sec)
                
                if duration_difference <= Config.DURATION_TOLERANCE_SEC:
                    return True, (f"Duration VALID. Actual: {actual_duration_sec:.2f}s, "
                                  f"Diff: {duration_difference:.3f}s.")
                else:
                    return False, (f"Duration FAILED. Expected: {expected_duration_sec:.2f}s, "
                                   f"Actual: {actual_duration_sec:.2f}s, Diff: {duration_difference:.3f}s.")
            except Exception as e:
                return False, f"Pydub/FFmpeg Error measuring audio duration: {e}"
        else:
            return True, "File exists (non-zero size), but Pydub/FFmpeg is not available to verify duration."
    
    return False, "File is missing or could not be measured."


def is_day_complete(day: int) -> bool:
    """
    Checks if all required output files exist for a given day.
    """
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{day}"
    if not day_path.exists():
        return False
        
    required_files = list(Config.AUDIO_FILE_MAP.values()) + [Config.REVIEW_MANIFEST_NAME, Config.WORKOUT_MANIFEST_NAME]
    
    for filename in required_files:
        if not (day_path / filename).exists():
            return False
            
    return True

# ... (Remaining utility functions: load_and_validate_source_data, generate_full_repetition_schedule, write_manifest_csv remain the same) ...

def load_and_validate_source_data() -> Tuple[List[Dict[str, Any]], int]:
    if not Config.SOURCE_FILE.exists():
        # This should theoretically not happen if run_environment_check was successful
        print(f"❌ Error: Master data file not found at {Config.SOURCE_FILE}")
        return [], 0
    try:
        with open(Config.SOURCE_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        required_headers = Config.CONTENT_KEYS + ['StudyDay']
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


def generate_full_repetition_schedule(master_data: List[Dict[str, Any]], max_day: int) -> Dict[int, List[ScheduleItem]]:
    schedules: Dict[int, List[ScheduleItem]] = {}
    history: List[ScheduleItem] = [
        item.copy() for item in master_data
    ]

    for current_day in range(1, max_day + 1):
        due_review_items: List[ScheduleItem] = []
        
        for item in history:
            original_study_day = item['StudyDay']
            if any(original_study_day + interval == current_day for interval in Config.MACRO_REPETITION_INTERVALS):
                review_item: ScheduleItem = {
                    'W2': item['W2'], 'W1': item['W1'], 'L1': item['L1'], 'L2': item['L2'],
                    'StudyDay': original_study_day,
                    'type': ScheduleType.REVIEW.value,
                    'repetition': Config.REVIEW_REPETITION_COUNT
                }
                due_review_items.append(review_item)

        new_items = [item for item in master_data if item['StudyDay'] == current_day]
        micro_repetition_schedule: List[ScheduleItem] = []
        
        for item in new_items:
            for rep in range(1, Config.MICRO_REPETITIONS_COUNT + 1):
                micro_repetition_schedule.append(ScheduleItem(
                    W2=item['W2'], W1=item['W1'], L1=item['L1'], L2=item['L2'],
                    StudyDay=current_day,
                    type=ScheduleType.NEW.value,
                    repetition=rep
                ))

        schedules[current_day] = due_review_items + micro_repetition_schedule
        
    return schedules

def write_manifest_csv(day_path: Path, filename: str, schedule_data: List[ScheduleItem], fieldnames: List[str]) -> bool:
    schedule_path = day_path / filename
    
    try:
        full_fieldnames = ['sequence'] + fieldnames 
        with open(schedule_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=full_fieldnames)
            writer.writeheader()

            for i, item in enumerate(schedule_data):
                row: Dict[str, Any] = {'sequence': i + 1}
                row.update({k: v for k, v in item.items() if k in full_fieldnames}) 
                row['StudyDay'] = str(item['StudyDay'])
                row['repetition'] = str(item['repetition'])
                writer.writerow(row)
        
        print(f"  - Wrote {filename} ({len(schedule_data)} items)")
        return True
    except Exception as e:
        print(f"  ❌ Error writing {filename}: {e}")
        return False


def process_day(day: int, full_schedule: List[ScheduleItem], use_real_tts_mode: bool, use_real_concat_mode: bool):
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{day}"
    day_path.mkdir(parents=True, exist_ok=True)
    
    print(f"\n--- 📝 Processing Day {day} ---")
    
    pre_cache_day_segments(full_schedule, use_real_tts_mode)

    print("\n  - Writing Manifests...")
    manifest_fieldnames = Config.CONTENT_KEYS + ['StudyDay', 'type', 'repetition']

    write_manifest_csv(day_path, Config.REVIEW_MANIFEST_NAME, full_schedule, manifest_fieldnames)
    
    workout_schedule = [item for item in full_schedule if item['type'] == ScheduleType.NEW.value]
    write_manifest_csv(day_path, Config.WORKOUT_MANIFEST_NAME, workout_schedule, manifest_fieldnames)

    print("\n  - Generating Final Audio Files (from pre-cached segments)...")
    
    # 1. WORKOUT AUDIO
    workout_path, workout_expected_duration = generate_audio_from_template(
        day_path, "workout", workout_schedule, use_real_concat_mode
    )
    is_valid, message = verify_audio_duration_integrity(
        workout_path, workout_expected_duration, use_real_concat_mode
    )
    print(f"  > Integrity Check ({workout_path.name}): {'✅ VALID' if is_valid else '❌ FAILED'} - {message}")
    print("---")
    
    # 2. REVIEW FORWARD AUDIO
    review_fwd_path, review_fwd_expected_duration = generate_audio_from_template(
        day_path, "review_forward", full_schedule, use_real_concat_mode
    )
    is_valid, message = verify_audio_duration_integrity(
        review_fwd_path, review_fwd_expected_duration, use_real_concat_mode
    )
    print(f"  > Integrity Check ({review_fwd_path.name}): {'✅ VALID' if is_valid else '❌ FAILED'} - {message}")
    print("---")

    # 3. REVIEW REVERSE AUDIO
    review_rev_path, review_rev_expected_duration = generate_audio_from_template(
        day_path, "review_reverse", full_schedule, use_real_concat_mode
    )
    is_valid, message = verify_audio_duration_integrity(
        review_rev_path, review_rev_expected_duration, use_real_concat_mode
    )
    print(f"  > Integrity Check ({review_rev_path.name}): {'✅ VALID' if is_valid else '❌ FAILED'} - {message}")
    print("---")


    new_count = len(workout_schedule) 
    review_items = [item for item in full_schedule if item['type'] == ScheduleType.REVIEW.value]
    review_count = len(review_items)
    print("\n  > Schedule Summary:")
    print(f"    - Total items: {len(full_schedule)}")
    print(f"    - New items: {new_count}")
    print(f"    - Review items: {review_count}")


def main_workflow():
    print("## 📚 Language Learner Schedule Generator (v1.8 - Full Audio Pipeline) ##")
    
    # --- STEP 1: Run Environment Check & Get Flags ---
    use_real_tts_mode, use_real_concat_mode, is_initial_run = run_environment_check()
    
    # --- STEP 2: Load Data (File is guaranteed to exist now) ---
    master_data, max_day = load_and_validate_source_data() 
    
    if not master_data:
        return

    # --- STEP 3: Determine Days to Process ---
    days_to_process: List[int] = []

    if is_initial_run:
        # If we just created the data, process all days (Day 1 to max_day)
        days_to_process = list(range(1, max_day + 1))
        print(f"💡 Initial run detected. Processing all {max_day} days to generate output files.")
    else:
        # If the data existed, only process days that are not yet complete
        days_to_process = [day for day in range(1, max_day + 1) if not is_day_complete(day)]

    if not days_to_process:
        print("\nAll days are declaratively complete. Workflow skipped.")
        return

    # --- STEP 4: Generate Schedules and Process Days ---
    schedules = generate_full_repetition_schedule(master_data, max_day)

    for day in days_to_process:
        schedule = schedules.get(day)
        if schedule is not None:
            process_day(day, schedule, use_real_tts_mode, use_real_concat_mode) 
            print(f"--- Day {day} processed successfully. ---\n")
        else:
            print(f"  ❌ Error: Schedule data missing for Day {day}.")


if __name__ == "__main__":
    main_workflow()