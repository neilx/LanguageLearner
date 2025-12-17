import csv
import hashlib
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set
from enum import Enum

try:
    from google.cloud import texttospeech
    from google.api_core.client_options import ClientOptions
    
    TTS_CLIENT = None
    CLOUD_TTS_AVAILABLE = True
except ImportError:
    CLOUD_TTS_AVAILABLE = False
    
try:
    from pydub import AudioSegment
    REAL_CONCAT_AVAILABLE = True
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

ScheduleItem = Dict[str, Any]

class ScheduleType(Enum):
    NEW = 'new'
    REVIEW = 'review'

# =========================================================================
# 1. Configuration Constants
# =========================================================================

class Config:
    USE_REAL_TTS: bool = True
    SOURCE_FILE: Path = Path('sentence_pairs.csv')
    OUTPUT_ROOT_DIR: Path = Path('Days')
    TTS_CACHE_DIR: Path = Path('tts_cache')
    TTS_CACHE_FILE_EXT: str = '.mp3'

    TARGET_LANG_CODE: str = 'da-DK'
    BASE_LANG_CODE: str = 'en-GB'
    
    TARGET_VOICE_NAME: str = 'da-DK-Neural2-D' 
    BASE_VOICE_NAME: str = 'en-GB-Standard-B' 
    
    MACRO_REPETITION_INTERVALS: List[int] = [1, 3, 7, 14, 30, 60, 120, 240]
    MICRO_SPACING_INTERVALS: List[int] = [0, 3, 7, 14, 28]

    AUDIO_TEMPLATES: Dict[str, Tuple[str, int, float]] = {
        "workout": ("SP W2 W1 L1 L2", 1, 0.7),
        "review_forward": ("SP W2 W1 L1 L2", 1, 1.0),
        "review_reverse": ("SP W2 W1 L2 L1", 1, 1.0),
    }

    TEMPLATE_DELIMITER: str = ' '
    SPECIAL_SEGMENTS: List[str] = ['SP']
    CONTENT_PAUSE_BUFFER_SEC: float = 0.3
    EXPLICIT_PAUSE_SEC: float = 1.0
    SEGMENT_ACTIONS: Dict[str, str] = {}
    MOCK_AVG_FILE_DURATION_SEC: float = 1.0
    DURATION_TOLERANCE_SEC: float = 0.5

    @staticmethod
    def get_content_keys() -> List[str]:
        all_segments: Set[str] = set()
        for pattern, _, _ in Config.AUDIO_TEMPLATES.values():
            all_segments.update(pattern.split(Config.TEMPLATE_DELIMITER))
        return sorted([k for k in all_segments if k and k not in Config.SPECIAL_SEGMENTS])

    @staticmethod
    def get_lang_config(segment_key: str) -> Tuple[str, str]:
        if segment_key.endswith('1'):
            return Config.BASE_LANG_CODE, Config.BASE_VOICE_NAME
        return Config.TARGET_LANG_CODE, Config.TARGET_VOICE_NAME

Config.SEGMENT_ACTIONS.update({key: 'CONTENT' for key in Config.get_content_keys()})
Config.SEGMENT_ACTIONS['SP'] = 'EXPLICIT_PAUSE'

# =========================================================================
# 2. TTS & Caching Logic
# =========================================================================

AUDIO_SEGMENT_CACHE: Dict[Path, AudioSegment] = {}

def get_cache_path(text: str, language_code: str, voice_name: str, speed: float = 1.0) -> Path:
    content_hash = hashlib.sha256(f"{text}{language_code}{voice_name}{speed}".encode()).hexdigest()
    return Config.TTS_CACHE_DIR / f"{content_hash}{Config.TTS_CACHE_FILE_EXT}"

def real_google_cloud_api(text: str, language_code: str, voice_name: str, cache_hits: List[int], api_calls: List[int], speed: float = 1.0) -> Path:
    global TTS_CLIENT
    real_file_path = get_cache_path(text, language_code, voice_name, speed)
    if real_file_path.exists():
        cache_hits[0] += 1
        return real_file_path

    if TTS_CLIENT is None:
        real_file_path.touch(exist_ok=True)
        return real_file_path

    api_calls[0] += 1
    synthesis_input = texttospeech.SynthesisInput(text=text)
    voice = texttospeech.VoiceSelectionParams(language_code=language_code, name=voice_name)
    audio_config = texttospeech.AudioConfig(audio_encoding=texttospeech.AudioEncoding.MP3, speaking_rate=speed)

    try:
        response = TTS_CLIENT.synthesize_speech(input=synthesis_input, voice=voice, audio_config=audio_config)
        with open(real_file_path, "wb") as out:
            out.write(response.audio_content)
        return real_file_path
    except Exception as e:
        print(f"    ❌ TTS Error: {e}")
        real_file_path.touch(exist_ok=True)
        return real_file_path

def mock_google_tts(text: str, language_code: str, voice_name: str, cache_hits: List[int], api_calls: List[int], speed: float = 1.0) -> Path:
    mock_file_path = get_cache_path(text, language_code, voice_name, speed)
    if not mock_file_path.exists():
        mock_file_path.touch(exist_ok=True)
        api_calls[0] += 1
    else:
        cache_hits[0] += 1
    return mock_file_path

# =========================================================================
# 3. Generation Logic
# =========================================================================

def pre_cache_day_segments(full_schedule: List[ScheduleItem], use_real_tts_mode: bool) -> None:
    print("  - Pre-Caching unique audio segments...")
    cache_hits, api_calls = [0], [0]
    tts_func = real_google_cloud_api if use_real_tts_mode else mock_google_tts
    unique_requests: Set[Tuple[str, str, str, float]] = set()

    required_speeds = set(speed for _, _, speed in Config.AUDIO_TEMPLATES.values())

    for item in full_schedule:
        for key in Config.get_content_keys():
            text = item.get(key)
            lang, voice = Config.get_lang_config(key)
            if key == 'L2':
                for s in required_speeds: unique_requests.add((text, lang, voice, s))
            else:
                unique_requests.add((text, lang, voice, 1.0))

    for text, lang, voice, speed in unique_requests:
        if text: tts_func(text, lang, voice, cache_hits, api_calls, speed)

def generate_audio_from_template(day_path: Path, day_num: int, template_name: str, pattern: str, data: List[ScheduleItem], use_concat: bool, template_speed: float) -> Tuple[Path, float]:
    # Refactored: Filename now padded to 3 digits (e.g., 001_workout.mp3)
    padded_day = str(day_num).zfill(3)
    output_path = day_path / f"{padded_day}_{template_name}.mp3"
    
    expected_duration = 0.0
    final_audio = AudioSegment.empty() if use_concat else None

    for item in data:
        for seg_key in pattern.split(Config.TEMPLATE_DELIMITER):
            if not seg_key: continue
            action = Config.SEGMENT_ACTIONS.get(seg_key)

            if action == 'CONTENT':
                text = item.get(seg_key, "")
                lang, voice = Config.get_lang_config(seg_key)
                speed = template_speed if seg_key == 'L2' else 1.0
                cached_path = get_cache_path(text, lang, voice, speed)

                dur_ms = Config.MOCK_AVG_FILE_DURATION_SEC * 1000.0
                if use_concat:
                    if cached_path not in AUDIO_SEGMENT_CACHE:
                        AUDIO_SEGMENT_CACHE[cached_path] = AudioSegment.from_mp3(cached_path) if cached_path.stat().st_size > 0 else AudioSegment.silent(duration=100)
                    seg = AUDIO_SEGMENT_CACHE[cached_path]
                    final_audio += seg
                    dur_ms = float(len(seg))

                pause_ms = dur_ms + (Config.CONTENT_PAUSE_BUFFER_SEC * 1000.0)
                expected_duration += (dur_ms + pause_ms) / 1000.0
                if use_concat: final_audio += AudioSegment.silent(duration=int(pause_ms))

            elif action == 'EXPLICIT_PAUSE':
                expected_duration += Config.EXPLICIT_PAUSE_SEC
                if use_concat: final_audio += AudioSegment.silent(duration=int(Config.EXPLICIT_PAUSE_SEC * 1000))

    if use_concat: final_audio.export(output_path, format="mp3")
    else: output_path.touch()
    return output_path, expected_duration

# =========================================================================
# 4. Workflow Helpers
# =========================================================================

def process_day(day: int, full_schedule: List[ScheduleItem], use_tts: bool, use_concat: bool):
    # Refactored: Folder now padded to 3 digits (e.g., day_001)
    padded_day = str(day).zfill(3)
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{padded_day}"
    
    day_path.mkdir(parents=True, exist_ok=True)
    print(f"\n--- 📝 Day {padded_day} ---")
    pre_cache_day_segments(full_schedule, use_tts)

    for name, (pattern, reps, speed) in Config.AUDIO_TEMPLATES.items():
        is_filtered = (speed != 1.0)
        source = [i for i in full_schedule if i['type'] == ScheduleType.NEW.value] if is_filtered else full_schedule
        if not source: continue

        sequenced = generate_interleaved_schedule(source, reps, Config.MICRO_SPACING_INTERVALS)
        
        # Manifest renamed for consistency: 00n_name_manifest.csv
        write_manifest_csv(day_path, f"{padded_day}_{name}_manifest.csv", sequenced, pattern)
        
        path, dur = generate_audio_from_template(day_path, day, name, pattern, sequenced, use_concat, speed)
        print(f"    - {padded_day}_{name.upper()} Generated (L2 Speed: {speed})")

def generate_interleaved_schedule(items: List[ScheduleItem], repetitions: int, intervals: List[int]) -> List[ScheduleItem]:
    if not items or repetitions <= 0: return []
    use_ints = intervals[:repetitions]
    arrays: Dict[int, List[ScheduleItem]] = {}
    for pos, item in enumerate(items, 1):
        indices = [pos + use_ints[0]]
        for i in range(1, len(use_ints)): indices.append(indices[-1] + use_ints[i])
        for idx in indices: arrays.setdefault(idx, []).append(item)
    return [item for key in sorted(arrays) for item in arrays[key]]

def write_manifest_csv(day_path: Path, filename: str, data: List[ScheduleItem], pattern: str):
    fields = ['sequence'] + [k for k in pattern.split(Config.TEMPLATE_DELIMITER) if k and k not in Config.SPECIAL_SEGMENTS] + ['StudyDay', 'type']
    with open(day_path / filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for i, item in enumerate(data): writer.writerow({'sequence': i + 1, **item})

def load_and_validate_source_data() -> Tuple[List[ScheduleItem], int]:
    if not Config.SOURCE_FILE.exists(): return [], 0
    with open(Config.SOURCE_FILE, 'r', encoding='utf-8') as f:
        data = list(csv.DictReader(f))
    for i in data: i['StudyDay'] = int(i['StudyDay'])
    return data, max((int(i['StudyDay']) for i in data), default=0)

def generate_full_repetition_schedule(master: List[ScheduleItem], max_day: int) -> Dict[int, List[ScheduleItem]]:
    schedules = {}
    for d in range(1, max_day + 1):
        items = []
        for i in master:
            if i['StudyDay'] == d: items.append({**i, 'type': ScheduleType.NEW.value})
            elif any(i['StudyDay'] + interval == d for interval in Config.MACRO_REPETITION_INTERVALS):
                items.append({**i, 'type': ScheduleType.REVIEW.value})
        schedules[d] = items
    return schedules

def is_day_complete(day: int) -> bool:
    padded_day = str(day).zfill(3)
    path = Config.OUTPUT_ROOT_DIR / f"day_{padded_day}"
    # Looks for the new padded filename format
    return all((path / f"{padded_day}_{t}.mp3").exists() for t in Config.AUDIO_TEMPLATES)

def run_environment_check():
    if not Config.SOURCE_FILE.exists():
        with open(Config.SOURCE_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=Config.get_content_keys() + ['StudyDay'])
            writer.writeheader()
            writer.writerow({'W2': 'sol', 'W1': 'sun', 'L1': 'The sun shines.', 'L2': 'Solen skinner.', 'StudyDay': 1})
    for p in [Config.OUTPUT_ROOT_DIR, Config.TTS_CACHE_DIR]: p.mkdir(exist_ok=True, parents=True)
    return (Config.USE_REAL_TTS and CLOUD_TTS_AVAILABLE), (REAL_CONCAT_AVAILABLE and FFMPEG_AVAILABLE)

def main_workflow():
    global TTS_CLIENT
    use_tts, use_concat = run_environment_check()
    if use_tts: TTS_CLIENT = texttospeech.TextToSpeechClient(client_options=ClientOptions(api_key=os.getenv('GOOGLE_API_KEY')))
    master, max_d = load_and_validate_source_data()
    if not master: return
    schedules = generate_full_repetition_schedule(master, max_d)
    for d in range(1, max_d + 1):
        if not is_day_complete(d): process_day(d, schedules.get(d, []), use_tts, use_concat)

if __name__ == "__main__":
    main_workflow()