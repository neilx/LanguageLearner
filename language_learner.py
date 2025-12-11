import csv
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set 
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
# 0. Global Memory Cache
# =========================================================================

# The key is the full path to the cached MP3 file.
AUDIO_SEGMENT_CACHE: Dict[Path, AudioSegment] = {}

# =========================================================================
# 0. Declarative Types and Enums
# =========================================================================

# A ScheduleItem holds a single row of data (W1, L2, StudyDay, type, etc.)
ScheduleItem = Dict[str, Any]

class ScheduleType(Enum):
    """Defines the two types of items that appear in the schedule."""
    NEW = 'new'
    REVIEW = 'review'

# =========================================================================
# 1. Configuration Constants
# =========================================================================

class Config:
    """Consolidated configuration settings."""

    # --- TTS Execution Switch ---
    USE_REAL_TTS: bool = True

    # --- File Paths ---
    SOURCE_FILE: Path = Path('sentence_pairs.csv')
    OUTPUT_ROOT_DIR: Path = Path('Days')
    TTS_CACHE_DIR: Path = Path('tts_cache')
    TTS_CACHE_FILE_EXT: str = '.mp3'

    # --- Language Codes (The source of truth for language deduction) ---
    TARGET_LANG_CODE: str = 'da'
    BASE_LANG_CODE: str = 'en-GB'

    # --- Repetition Parameters ---
    MACRO_REPETITION_INTERVALS: List[int] = [1, 3, 7, 14, 30, 60, 120, 240]
    MICRO_SPACING_INTERVALS: List[int] = [0, 3, 7, 14, 28]

    # --- CORE TEMPLATE SOURCE ---
    # Key: (Pattern String, Repetition Count, Use Filtered Data Only?)
    AUDIO_TEMPLATES: Dict[str, Tuple[str, int, bool]] = {
        "workout": ("SP W2 W1 L1 L2", 1, False),
        "review_forward": ("SP W2 W1 L1 L2", 1, False),
        "review_reverse": ("SP W2 W1 L2 L1", 1, False),
    }

    TEMPLATE_DELIMITER: str = ' '

    # --- Audio Timing Logic (Dynamic) ---
    # These segments are explicitly NOT content keys.
    SPECIAL_SEGMENTS: List[str] = ['SP']

    # 1. Padding added to the length of the word/sentence (Optimized for quick repetition)
    CONTENT_PAUSE_BUFFER_SEC: float = 0.3

    # 2. Fixed duration for explicit 'SP' (Optimized for deep retrieval/thinking time)
    EXPLICIT_PAUSE_SEC: float = 1.0

    # Dynamic dictionary to hold actions (Initialized below)
    SEGMENT_ACTIONS: Dict[str, str] = {}

    # --- Integrity Check Parameters ---
    MOCK_AVG_FILE_DURATION_SEC: float = 1.0
    DURATION_TOLERANCE_SEC: float = 0.5

    # --- DYNAMICALLY DERIVED CONTENT KEYS METHOD ---
    @staticmethod
    def get_content_keys() -> List[str]:
        """
        Dynamically generates the exhaustive list of content keys by inspecting all templates
        and filtering out special segments. Used for establishing CSV schema (column existence).
        """
        all_segments: Set[str] = set()
        for pattern, _, _ in Config.AUDIO_TEMPLATES.values():
            all_segments.update(pattern.split(Config.TEMPLATE_DELIMITER))
        
        # Remove empty strings and special segments, then sort for consistent CSV checking
        content_keys = [
            key for key in all_segments
            if key and key not in Config.SPECIAL_SEGMENTS
        ]
        return sorted(content_keys)

    # --- DEDUCTION LOGIC: Language code is deduced from the key's suffix. ---
    @staticmethod
    def get_lang_code(segment_key: str) -> str:
        """
        Deduces the language code based on the convention:
        - Ends in '1' -> Base Language (e.g., W1, L1)
        - Ends in '2' -> Target Language (e.g., W2, L2)
        """
        if segment_key.endswith('1'):
            return Config.BASE_LANG_CODE
        elif segment_key.endswith('2'):
            return Config.TARGET_LANG_CODE
        # Fallback/Safety
        return Config.BASE_LANG_CODE


# --- Configuration Initialization ---
# Setup SEGMENT_ACTIONS dynamically using the new content key derivation method
Config.SEGMENT_ACTIONS.update({
    key: 'CONTENT' for key in Config.get_content_keys()
})
Config.SEGMENT_ACTIONS['SP'] = 'EXPLICIT_PAUSE'
# --- End Configuration Initialization ---


# =========================================================================
# 2. Environment Check and Setup
# =========================================================================

def initialize_source_data() -> None:
    """Creates the mock sentence_pairs.csv file if it does not exist."""

    mock_data = [
        {'W2': 'sol', 'W1': 'sun', 'L1': 'The sun is shining today.', 'L2': 'Solen skinner i dag.', 'StudyDay': 1},
        {'W2': 'måne', 'W1': 'moon', 'L1': 'The moon is beautiful tonight.', 'L2': 'Månen er smuk i aften.', 'StudyDay': 1},
        {'W2': 'vand', 'W1': 'water', 'L1': 'I need some water.', 'L2': 'Jeg skal have noget vand.', 'StudyDay': 1},
        {'W2': 'tryghed', 'W1': 'security', 'L1': 'We seek security.', 'L2': 'Vi søger tryghed.', 'StudyDay': 2},
        {'W2': 'akkord', 'W1': 'chord', 'L1': 'He plays a chord.', 'L2': 'Han spiller en akkord.', 'StudyDay': 2},
        {'W2': 'lys', 'W1': 'light', 'L1': 'There is light at the end of the tunnel.', 'L2': 'Der er lys for enden af tunnelen.', 'StudyDay': 3},
        {'W2': 'mørke', 'W1': 'darkness', 'L1': 'The darkness fell.', 'L2': 'Mørket faldt på.', 'StudyDay': 3},
    ]

    fieldnames = Config.get_content_keys() + ['StudyDay']

    try:
        with open(Config.SOURCE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([{k: str(v) for k, v in item.items()} for item in mock_data])
        print(f"✅ Created initial mock data file: {Config.SOURCE_FILE}")
    except Exception as e:
        print(f"❌ Error creating master data file: {e}")

def run_environment_check() -> Tuple[bool, bool, bool]:
    """Checks dependencies and file existence."""
    print("--- 🔬 Starting Environment and Dependency Check ---")

    is_initial_run = not Config.SOURCE_FILE.exists()
    if is_initial_run:
        initialize_source_data()
    else:
        print(f"✅ Master data file found: {Config.SOURCE_FILE}")

    for path in [Config.OUTPUT_ROOT_DIR, Config.TTS_CACHE_DIR]:
        path.mkdir(parents=True, exist_ok=True)

    real_tts_mode = Config.USE_REAL_TTS and REAL_TTS_AVAILABLE
    real_concat_mode = REAL_CONCAT_AVAILABLE and FFMPEG_AVAILABLE

    if Config.USE_REAL_TTS and not real_tts_mode:
        print("❌ gTTS not found. Run: pip install gTTS")

    if Config.USE_REAL_TTS and not real_concat_mode:
        if not REAL_CONCAT_AVAILABLE:
            print("❌ Pydub not found. Run: pip install pydub")
        if not FFMPEG_AVAILABLE:
            print("❌ FFmpeg not found.")

    return real_tts_mode, real_concat_mode, is_initial_run


# =========================================================================
# 3. TTS API METHODS
# =========================================================================

def get_cache_path(text: str, language_code: str) -> Path:
    content_hash = hashlib.sha256(f"{text}{language_code}".encode()).hexdigest()
    return Config.TTS_CACHE_DIR / f"{content_hash}{Config.TTS_CACHE_FILE_EXT}"

def mock_google_tts(text: str, language_code: str, cache_hits: List[int], api_calls: List[int]) -> Path:
    mock_file_path = get_cache_path(text, language_code)
    if mock_file_path.exists():
        cache_hits[0] += 1
    else:
        try:
            mock_file_path.touch(exist_ok=True)
            api_calls[0] += 1
        except OSError:
            pass
    return mock_file_path

def real_gtts_api(text: str, language_code: str, cache_hits: List[int], api_calls: List[int]) -> Path:
    real_file_path = get_cache_path(text, language_code)
    if real_file_path.exists():
        cache_hits[0] += 1
        return real_file_path

    api_calls[0] += 1
    try:
        tts = gTTS(text=text, lang=language_code, slow= False)
        tts.save(real_file_path)
        return real_file_path
    except Exception:
        print(f"    ❌ gTTS Error: ({text[:15]}...)")
        real_file_path.touch(exist_ok=True)
        return real_file_path


# =========================================================================
# 4. Audio Generation & Scheduling Logic
# =========================================================================

def generate_interleaved_schedule(items: List[ScheduleItem], repetitions: int, intervals: List[int]) -> List[ScheduleItem]:
    if not items or repetitions <= 0:
        return []

    if repetitions > len(intervals):
        use_intervals = intervals[:]
    else:
        use_intervals = intervals[:repetitions]

    arrays: Dict[int, List[ScheduleItem]] = {}

    for item_position, item in enumerate(items, 1):
        indices = [item_position + use_intervals[0]]
        for i in range(1, len(use_intervals)):
            indices.append(indices[-1] + use_intervals[i])

        for idx in indices:
            arrays.setdefault(idx, []).append(item)

    concatenated: List[ScheduleItem] = []
    for key in sorted(arrays):
        concatenated.extend(arrays[key])

    return concatenated


def pre_cache_day_segments(full_schedule: List[ScheduleItem], use_real_tts_mode: bool) -> None:
    print("\n  - Pre-Caching unique audio segments...")
    cache_hits = [0]
    api_calls = [0]

    tts_func = real_gtts_api if use_real_tts_mode else mock_google_tts
    unique_segments: Set[Tuple[str, str]] = set()

    content_keys = Config.get_content_keys()

    for item in full_schedule:
        for key in content_keys:
            text_content = item.get(key)
            full_lang_code = Config.get_lang_code(key)
            segment_tuple = (text_content, full_lang_code)

            if text_content is not None and isinstance(text_content, str) and segment_tuple not in unique_segments:
                try:
                    tts_func(text_content, full_lang_code, cache_hits, api_calls)
                    unique_segments.add(segment_tuple)
                except Exception as e:
                    print(f"    ❌ Error pre-caching: {e}")
                    unique_segments.add(segment_tuple)

    print(f"  - Pre-Caching complete. Cache Hits: {cache_hits[0]}, API Calls: {api_calls[0]}")


def generate_audio_from_template(
    day_path: Path,
    template_name: str,
    pattern_string: str,
    data_list: List[ScheduleItem],
    use_real_concat_mode: bool
) -> Tuple[Path, float]:

    output_filename = f"{template_name}.mp3"
    output_path = day_path / output_filename
    expected_duration_sec: float = 0.0 # Correct variable initialized here
    is_real_mode = use_real_concat_mode

    if is_real_mode:
        final_audio = AudioSegment.empty()
    else:
        output_path.touch(exist_ok=True)

    # Calculate Expected Duration & Execute Concatenation
    for item in data_list:
        for segment_key in pattern_string.split(Config.TEMPLATE_DELIMITER):
            if not segment_key: continue

            action_type = Config.SEGMENT_ACTIONS.get(segment_key)

            if action_type == 'CONTENT':
                text_content = item.get(segment_key, "")
                full_lang_code = Config.get_lang_code(segment_key)
                cached_path = get_cache_path(text_content, full_lang_code)

                segment_duration_ms = Config.MOCK_AVG_FILE_DURATION_SEC * 1000.0

                if is_real_mode:

                    # --- MEMORY CACHE LOOKUP ---
                    if cached_path in AUDIO_SEGMENT_CACHE:
                        segment_audio = AUDIO_SEGMENT_CACHE[cached_path]
                        # print(f"    [Cache Hit in RAM: {segment_key}]")

                    elif cached_path.exists() and cached_path.stat().st_size > 0:
                        try:
                            # Load from Disk (Cache Miss)
                            segment_audio = AudioSegment.from_mp3(cached_path)
                            AUDIO_SEGMENT_CACHE[cached_path] = segment_audio # Store in RAM
                            # print(f"    [Cache Miss, Loaded from Disk: {segment_key}]")
                        except Exception:
                            # Fallback if corrupt
                            segment_audio = AudioSegment.silent(duration=100)
                    else:
                        segment_audio = AudioSegment.silent(duration=100)


                    # --- CONCATENATION ---
                    final_audio += segment_audio
                    segment_duration_ms = float(len(segment_audio))

                # DYNAMIC CALCULATION: Pause = Audio Length + Buffer
                pause_duration_ms = segment_duration_ms + (Config.CONTENT_PAUSE_BUFFER_SEC * 1000.0)

                # Add to total expected duration
                expected_duration_sec += (segment_duration_ms + pause_duration_ms) / 1000.0

                if is_real_mode:
                    final_audio += AudioSegment.silent(duration=int(pause_duration_ms))


            elif action_type == 'EXPLICIT_PAUSE':
                # Fixed pause for 'SP'
                pause_ms = Config.EXPLICIT_PAUSE_SEC * 1000.0
                expected_duration_sec += Config.EXPLICIT_PAUSE_SEC

                if is_real_mode:
                    final_audio += AudioSegment.silent(duration=int(pause_ms))

    print("\n  - Exporting file...")
    if is_real_mode:
        try:
            final_audio.export(output_path, format="mp3")
            print(f"  - Final {output_filename} EXPORTED.")
        except Exception as e:
            print(f"  ❌ FAILED TO EXPORT FINAL AUDIO: {e}")

    # FIX: Corrected variable name from 'expected_dur' to 'expected_duration_sec'
    return output_path, expected_duration_sec


def verify_audio_duration_integrity(file_path: Path, expected_duration_sec: float, use_real_concat_mode: bool) -> Tuple[bool, str]:
    if not file_path.exists(): return False, "File not found."
    if not use_real_concat_mode: return True, "Mock mode verified."
    if file_path.stat().st_size == 0: return True, "0 bytes (Mock/Error)."

    if REAL_CONCAT_AVAILABLE:
        try:
            # We don't use the memory cache here as we are loading the FINAL large file
            audio = AudioSegment.from_mp3(file_path)
            actual_duration_sec = len(audio) / 1000.0
            diff = abs(actual_duration_sec - expected_duration_sec)

            if diff <= Config.DURATION_TOLERANCE_SEC:
                return True, f"Valid. Diff: {diff:.3f}s"
            return False, f"Duration mismatch. Exp: {expected_duration_sec:.2f}, Act: {actual_duration_sec:.2f}"
        except Exception as e:
            return False, f"Check Error: {e}"
    return True, "No Pydub to verify."

# =========================================================================
# 5. Data Processing & Main Workflow
# =========================================================================

def load_and_validate_source_data() -> Tuple[List[ScheduleItem], int]:
    if not Config.SOURCE_FILE.exists(): return [], 0
    try:
        with open(Config.SOURCE_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        
        # Get content keys dynamically (sorted for consistent check)
        content_keys = Config.get_content_keys()
        
        # Determine all expected header fields
        expected_fields = set(content_keys + ['StudyDay'])
        
        if not data:
            if Config.SOURCE_FILE.stat().st_size > 0:
                print(f"    ⚠️ Data file is empty after header. Required fields: {list(expected_fields)}")
            return [], 0
        
        actual_fields = set(data[0].keys())

        if not expected_fields.issubset(actual_fields):
            print(f"    ❌ CSV Header Mismatch! Required fields derived from templates: {list(expected_fields)}")
            print(f"    ❌ Actual fields in CSV: {list(actual_fields)}")
            return [], 0

        # Ensure all required keys exist and cast StudyDay to int
        validated_data: List[ScheduleItem] = []
        for item in data:
            if all(key in item for key in content_keys) and 'StudyDay' in item:
                try:
                    item['StudyDay'] = int(item['StudyDay'])
                    validated_data.append(item)
                except ValueError:
                    print(f"    ⚠️ Skipping invalid row due to non-integer StudyDay: {item}")
            else:
                print(f"    ⚠️ Skipping invalid row (missing content keys or StudyDay): {item}")
        data = validated_data
        
        max_day = max(item['StudyDay'] for item in data) if data else 0
        return data, max_day
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return [], 0

def generate_full_repetition_schedule(master_data: List[ScheduleItem], max_day: int) -> Dict[int, List[ScheduleItem]]:
    """
    Generates the pool of items available for a specific day.
    Separates them by type (Review vs New).
    """
    schedules: Dict[int, List[ScheduleItem]] = {}
    history = [item.copy() for item in master_data]

    content_keys = Config.get_content_keys()

    for current_day in range(1, max_day + 1):
        day_items: List[ScheduleItem] = []

        # 1. Identify Review Items
        for item in history:
            original_study_day = item['StudyDay']
            if any(original_study_day + interval == current_day for interval in Config.MACRO_REPETITION_INTERVALS):
                review_item: ScheduleItem = {
                    'StudyDay': original_study_day,
                    'type': ScheduleType.REVIEW.value,
                    'repetition': 0
                }
                # Dynamically add content keys
                review_item.update({key: item.get(key, '') for key in content_keys})
                day_items.append(review_item)

        # 2. Identify New Items
        new_items_raw = [item for item in master_data if item['StudyDay'] == current_day]
        for item in new_items_raw:
            new_item: ScheduleItem = {
                'StudyDay': current_day,
                'type': ScheduleType.NEW.value,
                'repetition': 0
            }
            # Dynamically add content keys
            new_item.update({key: item.get(key, '') for key in content_keys})
            day_items.append(new_item)

        schedules[current_day] = day_items

    return schedules

def write_manifest_csv(day_path: Path, filename: str, schedule_data: List[ScheduleItem], pattern_string: str) -> bool:
    schedule_path = day_path / filename
    try:
        # 1. Get the content keys from the pattern string in the correct order
        content_fieldnames = [
            key for key in pattern_string.split(Config.TEMPLATE_DELIMITER)
            if key and key not in Config.SPECIAL_SEGMENTS
        ]
        
        # 2. Build the full fieldnames list
        fieldnames = ['sequence'] + content_fieldnames + ['StudyDay', 'type']
        
        with open(schedule_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, item in enumerate(schedule_data):
                row = {'sequence': i + 1}
                # Ensure only relevant fields from item are written
                row.update({k: v for k, v in item.items() if k in fieldnames})
                writer.writerow(row)
        return True
    except Exception as e:
        print(f"  ❌ Error writing {filename}: {e}")
        return False

def process_day(day: int, full_schedule: List[ScheduleItem], use_real_tts_mode: bool, use_real_concat_mode: bool):
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{day}"
    day_path.mkdir(parents=True, exist_ok=True)

    print(f"\n--- 📝 Processing Day {day} ---")
    pre_cache_day_segments(full_schedule, use_real_tts_mode)

    # --- INTEGRATED TEMPLATE LOOP ---
    for template_name, (pattern, repetitions, use_filtered_data) in Config.AUDIO_TEMPLATES.items():

        print(f"\n  > Processing Template: {template_name.upper()}")

        # 1. Filter Data
        if use_filtered_data:
            source_items = [item for item in full_schedule if item['type'] == ScheduleType.NEW.value]
        else:
            source_items = full_schedule

        # 2. Apply Spacing/Interleaving Logic
        sequenced_items = generate_interleaved_schedule(
            source_items,
            repetitions,
            Config.MICRO_SPACING_INTERVALS
        )

        if not sequenced_items:
            print(f"    ⚠️ No items found for this template on Day {day}. Skipping.")
            continue

        # 3. Write Manifest
        manifest_name = f"{template_name}_manifest.csv"
        write_manifest_csv(day_path, manifest_name, sequenced_items, pattern)

        # 4. Generate Audio
        audio_path, expected_dur = generate_audio_from_template(
            day_path, template_name, pattern, sequenced_items, use_real_concat_mode
        )

        # 5. Verify
        is_valid, msg = verify_audio_duration_integrity(audio_path, expected_dur, use_real_concat_mode)
        print(f"    - Integrity: {'✅ VALID' if is_valid else '❌ CHECK'} ({msg})")


def is_day_complete(day: int) -> bool:
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{day}"
    if not day_path.exists(): return False
    # Check for presence of files for all templates
    for tmpl in Config.AUDIO_TEMPLATES.keys():
        if not (day_path / f"{tmpl}.mp3").exists(): return False
    return True

def main_workflow():
    print("## 📚 Language Learner Schedule Generator (v4.2 - NameError Fix) ##")

    use_real_tts_mode, use_real_concat_mode, is_initial_run = run_environment_check()
    master_data, max_day = load_and_validate_source_data()

    if not master_data: return

    days_to_process = []
    if is_initial_run:
        days_to_process = list(range(1, max_day + 1))
        print(f"💡 Initial run. Processing all {max_day} days.")
    else:
        days_to_process = [d for d in range(1, max_day + 1) if not is_day_complete(d)]

    if not days_to_process:
        print("\n✅ All days are complete.")
        return

    schedules = generate_full_repetition_schedule(master_data, max_day)

    for day in days_to_process:
        schedule = schedules.get(day)
        if schedule:
            process_day(day, schedule, use_real_tts_mode, use_real_concat_mode)
            print(f"--- Day {day} Complete ---\n")
        else:
            print(f"  ❌ No data for Day {day}.")


if __name__ == "__main__":
    main_workflow()