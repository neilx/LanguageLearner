import argparse
import csv
import hashlib
import os
import sys
import zipfile
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

def _is_pause_token(token: str) -> bool:
    if not token.endswith('s'):
        return False
    try:
        float(token[:-1])
        return True
    except ValueError:
        return False

def _parse_pause_sec(token: str) -> float:
    return float(token[:-1])

# =========================================================================
# 1. Configuration Constants
# =========================================================================

class Config:
    # --- iCloud Directory Configuration ---
    ICLOUD_BASE = Path(r'C:\Users\neil_\iCloudDrive\LanguageLearnerData')
    
    USE_REAL_TTS: bool = True
    SOURCE_FILE: Path = ICLOUD_BASE / 'sentence_pairs_simple.csv'
    OUTPUT_ROOT_DIR: Path = ICLOUD_BASE / 'Days_simple'
    
    # Keeping the cache local to the GitHub folder for speed
    TTS_CACHE_DIR: Path = Path('tts_cache')
    TTS_CACHE_FILE_EXT: str = '.mp3'

    TARGET_LANG_CODE: str = 'da-DK'
    BASE_LANG_CODE: str = 'en-GB'
    
    TARGET_VOICE_NAME: str = 'da-DK-Neural2-D' 
    BASE_VOICE_NAME: str = 'en-GB-Standard-B' 
    
    MACRO_REPETITION_INTERVALS: List[int] = [1, 3, 7, 14, 30, 60, 120, 240]
    MICRO_SPACING_INTERVALS: List[int] = [0, 3, 7, 14, 28]

    TEMPLATES: Dict[str, Tuple[str, int, float, str]] = {
        "workout_forward_transcribe!": ("L1 1.0s L2", 1, 0.7, "audio"),
        "workout_reverse_transcribe!": ("L2 1.0s L1", 1, 0.7, "audio"),
        "review_forward_transcribe":   ("L1 1.0s L2", 1, 1.0, "audio"),
        "review_reverse_transcribe":   ("L2 1.0s L1", 1, 1.0, "audio"),

        "workout_forward_tur": ("L1 1.0s L2 L2 L2 L1 L2", 1, 0.7, "audio"),
        "workout_reverse_tur": ("L2 L2 1.0s L1 L2 L1 L2", 1, 0.7, "audio"),
        "review_forward_tur":  ("L1 L2 1.0s L2 L2 L1 L2", 1, 1.0, "audio"),
        "review_reverse_tur":  ("L2 L2 1.0s L1 L2 L1 L2", 1, 1.0, "audio"),
        "vocab_list": ("L1 L2", 1, 1.0, "csv"),


    }

    TEMPLATE_DELIMITER: str = ' '
    CONTENT_PAUSE_BUFFER_SEC: float = 0.3
    SEGMENT_ACTIONS: Dict[str, str] = {}
    MOCK_AVG_FILE_DURATION_SEC: float = 1.0

    @staticmethod
    def get_content_keys() -> List[str]:
        all_segments: Set[str] = set()
        for pattern, _, _, _ in Config.TEMPLATES.values():
            all_segments.update(pattern.split(Config.TEMPLATE_DELIMITER))
        return sorted([k for k in all_segments if k and not _is_pause_token(k)])

    @staticmethod
    def get_lang_config(segment_key: str) -> Tuple[str, str]:
        if segment_key.endswith('1'):
            return Config.BASE_LANG_CODE, Config.BASE_VOICE_NAME
        return Config.TARGET_LANG_CODE, Config.TARGET_VOICE_NAME

Config.SEGMENT_ACTIONS.update({key: 'CONTENT' for key in Config.get_content_keys()})

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

def pre_cache_day_segments(full_schedule: List[ScheduleItem], use_real_tts_mode: bool) -> Tuple[int, int]:
    cache_hits, api_calls = [0], [0]
    tts_func = real_google_cloud_api if use_real_tts_mode else mock_google_tts
    unique_requests: Set[Tuple[str, str, str, float]] = set()
    required_speeds = set(speed for _, _, speed, ot in Config.TEMPLATES.values() if ot == 'audio')

    audio_keys: Set[str] = set(
        k for _, (pattern, _, _, ot) in Config.TEMPLATES.items()
        if ot == 'audio'
        for k in pattern.split(Config.TEMPLATE_DELIMITER)
        if k and not _is_pause_token(k)
    )
    for item in full_schedule:
        for key in audio_keys:
            text = item.get(key)
            lang, voice = Config.get_lang_config(key)
            if not text: continue
            if key == 'L2':
                for s in required_speeds: unique_requests.add((text, lang, voice, s))
            else:
                unique_requests.add((text, lang, voice, 1.0))

    for text, lang, voice, speed in unique_requests:
        tts_func(text, lang, voice, cache_hits, api_calls, speed)
    
    return cache_hits[0], api_calls[0]

def _pydub_export_format(audio_format: str) -> str:
    return 'mp4' if audio_format == 'm4a' else audio_format

def generate_audio_from_template(day_path: Path, day_num: int, template_name: str, pattern: str, data: List[ScheduleItem], use_concat: bool, template_speed: float, audio_format: str = 'mp3') -> Tuple[Path, float]:
    padded_day = str(day_num).zfill(3)
    output_path = day_path / f"{padded_day}_{template_name}.{audio_format}"
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

            elif _is_pause_token(seg_key):
                pause_sec = _parse_pause_sec(seg_key)
                expected_duration += pause_sec
                if use_concat: final_audio += AudioSegment.silent(duration=int(pause_sec * 1000))

    if use_concat: final_audio.export(output_path, format=_pydub_export_format(audio_format))
    else: output_path.touch()
    return output_path, expected_duration

# =========================================================================
# 4. Workflow Helpers
# =========================================================================

def generate_csv_from_template(day_path: Path, day_num: int, template_name: str, pattern: str, data: List[ScheduleItem]) -> Path:
    padded_day = str(day_num).zfill(3)
    output_path = day_path / f"{padded_day}_{template_name}.csv"
    seen: Set[str] = set()
    content_keys = []
    for k in pattern.split(Config.TEMPLATE_DELIMITER):
        if k and not _is_pause_token(k) and k not in seen:
            content_keys.append(k)
            seen.add(k)
    extra = [f for f in ('StudyDay', 'Imagery') if data and f in data[0]]
    fields = content_keys + extra
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for item in data:
            writer.writerow(item)
    return output_path

def process_day(day: int, full_schedule: List[ScheduleItem], use_tts: bool, use_concat: bool, audio_format: str = 'mp3') -> float:
    padded_day = str(day).zfill(3)
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{padded_day}"
    day_path.mkdir(parents=True, exist_ok=True)

    new_count = sum(1 for i in full_schedule if i['type'] == ScheduleType.NEW.value)
    rev_count = len(full_schedule) - new_count
    print(f"\n--- 📝 Day {padded_day} ({new_count} New, {rev_count} Review) ---")

    hits, calls = pre_cache_day_segments(full_schedule, use_tts)
    total_segments = hits + calls
    hit_rate = (hits / total_segments * 100) if total_segments > 0 else 0
    print(f"    - TTS Cache: {hit_rate:.1f}% hit rate ({calls} new calls)")

    day_total_duration = 0.0
    for name, (pattern, reps, speed, output_type) in Config.TEMPLATES.items():
        if output_type == 'csv':
            path = generate_csv_from_template(day_path, day, name, pattern, full_schedule)
            print(f"    - {path.name:25} | {len(full_schedule)} rows")
            continue

        is_filtered = (speed != 1.0)
        source = [i for i in full_schedule if i['type'] == ScheduleType.NEW.value] if is_filtered else full_schedule
        if not source: continue

        sequenced = generate_interleaved_schedule(source, reps, Config.MICRO_SPACING_INTERVALS)
        write_manifest_csv(day_path, f"{padded_day}_{name}_manifest.csv", sequenced, pattern)

        path, dur = generate_audio_from_template(day_path, day, name, pattern, sequenced, use_concat, speed, audio_format)
        day_total_duration += dur

        m, s = divmod(int(dur), 60)
        print(f"    - {path.name:25} | {m:02d}:{s:02d}")

    return day_total_duration

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
    # 1. Identify the dynamic language segments (e.g., W1, L1, L2) from the pattern
    content_keys = [k for k in pattern.split(Config.TEMPLATE_DELIMITER) if k and not _is_pause_token(k)]
    
    # 2. Define exactly which fields we want in the CSV
    # Added 'Imagery' and removed 'sequence' and 'type'
    fields = content_keys + ['StudyDay', 'Imagery']
    
    with open(day_path / filename, 'w', newline='', encoding='utf-8') as f:
        # 'extrasaction=ignore' ensures that if 'type' or other keys exist in the data, 
        # they won't be written to the CSV if they aren't in our 'fields' list.
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for item in data:
            writer.writerow(item)

# --- ROBUST CSV LOADING ---
def load_and_validate_source_data() -> Tuple[List[ScheduleItem], int]:
    if not Config.SOURCE_FILE.exists(): return [], 0
    
    # Use 'utf-8-sig' to automatically strip Excel's BOM marks
    with open(Config.SOURCE_FILE, 'r', encoding='utf-8-sig') as f:
        # Sniff for delimiter (handles comma vs semicolon)
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=',;')
        except:
            dialect = 'excel'
            
        reader = csv.DictReader(f, dialect=dialect)
        # Strip invisible spaces from headers
        if reader.fieldnames:
            reader.fieldnames = [n.strip() for n in reader.fieldnames]
            
        data = []
        for row in reader:
            # Strip spaces from the values themselves
            cleaned_row = {k.strip(): v.strip() for k, v in row.items() if k}
            data.append(cleaned_row)

    if not data:
        return [], 0

    try:
        for i in data: 
            i['StudyDay'] = int(i['StudyDay'])
        return data, max((int(i['StudyDay']) for i in data), default=0)
    except KeyError:
        print(f"\n❌ ERROR: Key 'StudyDay' not found in your CSV.")
        print(f"   Detected Columns: {list(data[0].keys())}")
        print(f"   Check your column headers in: {Config.SOURCE_FILE}")
        sys.exit(1)

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

def is_day_complete(day: int, audio_format: str = 'mp3') -> bool:
    padded_day = str(day).zfill(3)
    path = Config.OUTPUT_ROOT_DIR / f"day_{padded_day}"
    for name, (_, _, _, output_type) in Config.TEMPLATES.items():
        ext = 'csv' if output_type == 'csv' else audio_format
        if not (path / f"{padded_day}_{name}.{ext}").exists():
            return False
    return True

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LanguageLearner audio generator")
    parser.add_argument('--mode', choices=['sr', 'pairs'], default='sr',
                        help="Workflow: 'sr' = spaced repetition (default), 'pairs' = sentence pairs")
    parser.add_argument('--source', type=Path,
                        help='Override source CSV path')
    parser.add_argument('--output', type=Path,
                        help='Override output directory')
    parser.add_argument('--base-lang', dest='base_lang',
                        help='Base language code (e.g. en-GB)')
    parser.add_argument('--base-voice', dest='base_voice',
                        help='Base voice name (e.g. en-GB-Standard-B)')
    parser.add_argument('--target-lang', dest='target_lang',
                        help='Target language code (e.g. da-DK)')
    parser.add_argument('--target-voice', dest='target_voice',
                        help='Target voice name (e.g. da-DK-Neural2-D)')
    parser.add_argument('--format', choices=['mp3', 'm4a'], default='mp3', dest='audio_format',
                        help='Output audio format (default: mp3)')
    parser.add_argument('--zip', action='store_true',
                        help='Package all output files into a zip archive')
    parser.add_argument('--template', default=next(iter(Config.TEMPLATES)),
                        help=f"Template name for pairs mode (default: {next(iter(Config.TEMPLATES))}). Available: {', '.join(Config.TEMPLATES)}")
    parser.add_argument('--mock', action='store_true',
                        help='Force mock TTS — exercises logic and cache paths without real API calls')
    return parser.parse_args()

def apply_arg_overrides(args: argparse.Namespace) -> None:
    if args.source:       Config.SOURCE_FILE = args.source
    if args.output:       Config.OUTPUT_ROOT_DIR = args.output
    if args.base_lang:    Config.BASE_LANG_CODE = args.base_lang
    if args.base_voice:   Config.BASE_VOICE_NAME = args.base_voice
    if args.target_lang:  Config.TARGET_LANG_CODE = args.target_lang
    if args.target_voice: Config.TARGET_VOICE_NAME = args.target_voice
    if args.mock:         Config.USE_REAL_TTS = False

def zip_output_dir(output_dir: Path) -> Path:
    zip_path = output_dir.parent / f"{output_dir.name}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in output_dir.rglob('*'):
            if f.is_file():
                zf.write(f, f.relative_to(output_dir))
    print(f"  📦 Packaged: {zip_path}")
    return zip_path

def load_sentence_pairs(source_file: Path) -> List[ScheduleItem]:
    if not source_file.exists():
        print(f"❌ Source file not found: {source_file}")
        return []
    with open(source_file, 'r', encoding='utf-8-sig') as f:
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=',;')
        except Exception:
            dialect = 'excel'
        reader = csv.DictReader(f, dialect=dialect)
        if reader.fieldnames:
            reader.fieldnames = [n.strip() for n in reader.fieldnames]
        data = [{k.strip(): v.strip() for k, v in row.items() if k} for row in reader]
    return data

def sentence_pairs_workflow(template_name: str, use_tts: bool, use_concat: bool, audio_format: str, do_zip: bool) -> None:
    pairs = load_sentence_pairs(Config.SOURCE_FILE)
    if not pairs:
        print("❌ Error: No sentence pairs found.")
        return

    if template_name not in Config.TEMPLATES:
        available = ', '.join(Config.TEMPLATES.keys())
        print(f"❌ Unknown template '{template_name}'.\n   Available: {available}")
        return

    pattern, _, speed, _ = Config.TEMPLATES[template_name]
    Config.OUTPUT_ROOT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"--- 🎧 Sentence Pairs Mode ---")
    print(f"Template : {template_name}  ({pattern})")
    print(f"Pairs    : {len(pairs)}")

    hits, calls = pre_cache_day_segments(pairs, use_tts)
    total_segments = hits + calls
    hit_rate = (hits / total_segments * 100) if total_segments > 0 else 0
    print(f"TTS Cache: {hit_rate:.1f}% hit rate ({calls} new calls)\n")

    content_keys = sorted(set(
        k for k in pattern.split(Config.TEMPLATE_DELIMITER)
        if k and not _is_pause_token(k)
    ))
    manifest_rows: List[Dict[str, str]] = []

    for idx, pair in enumerate(pairs, 1):
        _, dur = generate_audio_from_template(
            Config.OUTPUT_ROOT_DIR, idx, template_name, pattern,
            [pair], use_concat, speed, audio_format
        )
        padded = str(idx).zfill(3)
        out_filename = f"{padded}_{template_name}.{audio_format}"
        row = {k: pair.get(k, '') for k in content_keys}
        row['filename'] = out_filename
        manifest_rows.append(row)
        m, s = divmod(int(dur), 60)
        print(f"  {out_filename} | {m:02d}:{s:02d}")

    manifest_path = Config.OUTPUT_ROOT_DIR / "manifest.csv"
    fields = content_keys + ['filename']
    with open(manifest_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(manifest_rows)

    print(f"\n✅ Generated {len(pairs)} files → {Config.OUTPUT_ROOT_DIR}")
    if do_zip:
        zip_output_dir(Config.OUTPUT_ROOT_DIR)

def run_environment_check():
    Config.ICLOUD_BASE.mkdir(exist_ok=True, parents=True)
    Config.OUTPUT_ROOT_DIR.mkdir(exist_ok=True, parents=True)
    Config.TTS_CACHE_DIR.mkdir(exist_ok=True, parents=True)

    use_tts = (Config.USE_REAL_TTS and CLOUD_TTS_AVAILABLE)
    use_concat = (REAL_CONCAT_AVAILABLE and FFMPEG_AVAILABLE)
    
    print(f"--- 🚀 Environment Ready ---")
    print(f"Engine: {'[LIVE] Google Cloud' if use_tts else '[MOCK] Logic-Only'}")
    print(f"Audio:  {'[ENABLED] Merging MP3s' if use_concat else '[DISABLED] Metadata only'}")
    print(f"Target: {Config.TARGET_LANG_CODE} ({Config.TARGET_VOICE_NAME})")
    print(f"Storage: {Config.ICLOUD_BASE}\n")
    
    return use_tts, use_concat

def main_workflow():
    global TTS_CLIENT
    args = parse_args()
    apply_arg_overrides(args)

    use_tts, use_concat = run_environment_check()

    if args.audio_format == 'm4a' and not use_concat:
        print("⚠️  m4a requested but ffmpeg/pydub unavailable — falling back to mp3")
        args.audio_format = 'mp3'

    if use_tts:
        TTS_CLIENT = texttospeech.TextToSpeechClient(client_options=ClientOptions(api_key=os.getenv('GOOGLE_API_KEY')))

    if args.mode == 'pairs':
        sentence_pairs_workflow(args.template, use_tts, use_concat, args.audio_format, args.zip)
        return

    master, max_d = load_and_validate_source_data()
    if not master:
        print("❌ Error: No source data found.")
        return

    schedules = generate_full_repetition_schedule(master, max_d)
    days_processed = 0
    total_session_duration = 0.0

    for d in range(1, max_d + 1):
        if not is_day_complete(d, args.audio_format):
            day_dur = process_day(d, schedules.get(d, []), use_tts, use_concat, args.audio_format)
            total_session_duration += day_dur
            days_processed += 1

    if days_processed == 0:
        print(f"✅ All {max_d} days are up to date in iCloud.")
    else:
        total_m, total_s = divmod(int(total_session_duration), 60)
        print(f"\n--- ✅ Session Complete ---")
        print(f"Days Generated: {days_processed}")
        print(f"Total Audio:    {total_m}m {total_s}s")

    if args.zip:
        zip_output_dir(Config.OUTPUT_ROOT_DIR)

if __name__ == "__main__":
    main_workflow()