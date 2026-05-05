import argparse
import csv
import hashlib
import os
import random
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Set, Tuple
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
# 1. Progress logging
# GUI replaces this via set_log_callback() before calling main_workflow().
# =========================================================================

_log: Callable[[str], None] = print

def set_log_callback(fn: Callable[[str], None]) -> None:
    global _log
    _log = fn

# =========================================================================
# 2. Run configuration
# CLI builds this from argparse; GUI builds it directly.
# =========================================================================

@dataclass
class RunConfig:
    mode: str = 'sr'
    audio_format: str = 'mp3'
    do_zip: bool = False
    template: str = ''

# =========================================================================
# 3. Configuration Constants
# =========================================================================

class Config:
    ICLOUD_BASE = Path(r'C:\Users\neil_\Documents\GitHub\LanguageLearner')

    USE_REAL_TTS: bool = True
    SOURCE_FILE: Path = ICLOUD_BASE / 'sentence_pairs_simple.csv'
    OUTPUT_ROOT_DIR: Path = ICLOUD_BASE / 'Days_simple'

    TTS_CACHE_DIR: Path = Path('tts_cache')
    TTS_CACHE_FILE_EXT: str = '.mp3'

    TARGET_LANG_CODE: str = 'da-DK'
    BASE_LANG_CODE: str = 'en-GB'

    TARGET_VOICE_NAME: str = 'da-DK-Neural2-D'
    BASE_VOICE_NAME: str = 'en-GB-Standard-B'

    MACRO_REPETITION_INTERVALS: List[int] = [1, 3, 7, 14, 30, 60, 120, 240]
    TEMPLATES: Dict[str, Tuple[str, int, float, str]] = {
        # -------------------
        # TODAY (active learning)
        # -------------------
        "today":       ("L1 1.0s L2",          1, 0.7, "audio"),
        "today_r":     ("L2 1.0s L1",          1, 0.7, "audio"),

        # FLOW (passive repetition of today)
        "today_flow":   ("L1 1.5s L2 L2 L2 L2", 1, 0.7, "audio"),
        "today_flow_r": ("L2 1.5s L1 L2 L2 L2", 1, 0.7, "audio"),

        # -------------------
        # REVIEW (CSV-based spaced repetition)
        # -------------------
        "review":       ("L1 1.0s L2",          1, 1.0, "audio"),
        "review_r":     ("L2 1.0s L1",          1, 1.0, "audio"),

        "review_flow":   ("L1 1.0s L2 L2 L2 L2", 1, 1.0, "audio"),
        "review_flow_r": ("L2 1.0s L1 L2 L2 L2", 1, 1.0, "audio"),

        # -------------------
        # VOCAB (CSV source only)
        # -------------------
        "review":        ("L1 L2",               1, 1.0, "csv"),
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
# 4. TTS & Caching Logic
# =========================================================================

AUDIO_SEGMENT_CACHE: Dict[Path, Any] = {}

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
        _log(f"    ❌ TTS Error: {e}")
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
# 5. Generation Logic
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
# 6. Workflow Helpers
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
    extra = [f for f in (data[0].keys() if data else []) if f not in seen]
    fields = content_keys + extra
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for item in data:
            writer.writerow(item)
    return output_path

def process_day(day: int, full_schedule: List[ScheduleItem], use_tts: bool, use_concat: bool, audio_format: str = 'mp3') -> float:
    padded_day = str(day).zfill(3)
    day_path = Config.OUTPUT_ROOT_DIR / f"day_{padded_day}"
    day_path.mkdir(parents=True, exist_ok=True)

    missing = []
    for name, (_, _, speed, ot) in Config.TEMPLATES.items():
        ext = 'csv' if ot == 'csv' else audio_format
        if (day_path / f"{padded_day}_{name}.{ext}").exists():
            continue
        if ot != 'csv':
            target_type = ScheduleType.NEW.value if speed != 1.0 else ScheduleType.REVIEW.value
            if not any(i['type'] == target_type for i in full_schedule):
                continue
        missing.append(name)
    if not missing:
        return 0.0

    new_count = sum(1 for i in full_schedule if i['type'] == ScheduleType.NEW.value)
    rev_count = len(full_schedule) - new_count
    _log(f"\n--- 📝 Day {padded_day} ({new_count} New, {rev_count} Review) ---")

    hits, calls = pre_cache_day_segments(full_schedule, use_tts)
    total_segments = hits + calls
    hit_rate = (hits / total_segments * 100) if total_segments > 0 else 0
    _log(f"    - TTS Cache: {hit_rate:.1f}% hit rate ({calls} new calls)")

    review_items = [i for i in full_schedule if i['type'] == ScheduleType.REVIEW.value]
    shuffled_review = random.Random(day).sample(review_items, len(review_items))

    day_total_duration = 0.0
    for name, (pattern, _, speed, output_type) in Config.TEMPLATES.items():
        ext = 'csv' if output_type == 'csv' else audio_format
        output_file = day_path / f"{padded_day}_{name}.{ext}"
        if output_file.exists():
            continue

        if output_type == 'csv':
            path = generate_csv_from_template(day_path, day, name, pattern, shuffled_review)
            _log(f"    - {path.name:25} | {len(shuffled_review)} rows")
            continue

        target_type = ScheduleType.NEW.value if speed != 1.0 else ScheduleType.REVIEW.value
        source = [i for i in full_schedule if i['type'] == target_type]
        if not source: continue

        sequenced = shuffled_review if target_type == ScheduleType.REVIEW.value else list(source)

        path, dur = generate_audio_from_template(day_path, day, name, pattern, sequenced, use_concat, speed, audio_format)
        day_total_duration += dur

        m, s = divmod(int(dur), 60)
        _log(f"    - {path.name:25} | {m:02d}:{s:02d}")

    return day_total_duration

def load_and_validate_source_data() -> Tuple[List[ScheduleItem], int]:
    if not Config.SOURCE_FILE.exists():
        return [], 0

    with open(Config.SOURCE_FILE, 'r', encoding='utf-8-sig') as f:
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

    if not data:
        return [], 0

    try:
        for i in data:
            i['StudyDay'] = int(i['StudyDay'])
        return data, max((int(i['StudyDay']) for i in data), default=0)
    except KeyError:
        raise ValueError(
            f"Key 'StudyDay' not found in your CSV.\n"
            f"Detected columns: {list(data[0].keys())}\n"
            f"Source file: {Config.SOURCE_FILE}"
        )

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

def zip_output_dir(output_dir: Path, extra_files: List[Path] = []) -> Path:
    zip_path = output_dir.parent / f"{output_dir.name}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in output_dir.rglob('*'):
            if f.is_file():
                zf.write(f, f.relative_to(output_dir))
        for f in extra_files:
            if f.is_file():
                zf.write(f, f.name)
    _log(f"  📦 Packaged: {zip_path}")
    return zip_path

def load_sentence_pairs(source_file: Path) -> List[ScheduleItem]:
    if not source_file.exists():
        raise ValueError(f"Source file not found: {source_file}")
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

def sentence_pairs_workflow(run_config: RunConfig, use_tts: bool, use_concat: bool) -> None:
    pairs = load_sentence_pairs(Config.SOURCE_FILE)
    if not pairs:
        raise ValueError("No sentence pairs found in source file.")

    if run_config.template not in Config.TEMPLATES:
        raise ValueError(
            f"Unknown template '{run_config.template}'.\n"
            f"Available: {', '.join(Config.TEMPLATES.keys())}"
        )

    pattern, _, speed, _ = Config.TEMPLATES[run_config.template]
    Config.OUTPUT_ROOT_DIR.mkdir(parents=True, exist_ok=True)

    _log(f"--- 🎧 Sentence Pairs Mode ---")
    _log(f"Template : {run_config.template}  ({pattern})")
    _log(f"Pairs    : {len(pairs)}")

    hits, calls = pre_cache_day_segments(pairs, use_tts)
    total_segments = hits + calls
    hit_rate = (hits / total_segments * 100) if total_segments > 0 else 0
    _log(f"TTS Cache: {hit_rate:.1f}% hit rate ({calls} new calls)\n")

    content_keys = sorted(set(
        k for k in pattern.split(Config.TEMPLATE_DELIMITER)
        if k and not _is_pause_token(k)
    ))
    manifest_rows: List[Dict[str, str]] = []

    for idx, pair in enumerate(pairs, 1):
        _, dur = generate_audio_from_template(
            Config.OUTPUT_ROOT_DIR, idx, run_config.template, pattern,
            [pair], use_concat, speed, run_config.audio_format
        )
        padded = str(idx).zfill(3)
        out_filename = f"{padded}_{run_config.template}.{run_config.audio_format}"
        row = {k: pair.get(k, '') for k in content_keys}
        row['filename'] = out_filename
        manifest_rows.append(row)
        m, s = divmod(int(dur), 60)
        _log(f"  {out_filename} | {m:02d}:{s:02d}")

    manifest_path = Config.OUTPUT_ROOT_DIR / "manifest.csv"
    fields = content_keys + ['filename']
    with open(manifest_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(manifest_rows)

    _log(f"\n✅ Generated {len(pairs)} files → {Config.OUTPUT_ROOT_DIR}")
    if run_config.do_zip:
        zip_output_dir(Config.OUTPUT_ROOT_DIR, extra_files=[Config.SOURCE_FILE])

def run_environment_check() -> Tuple[bool, bool]:
    Config.ICLOUD_BASE.mkdir(exist_ok=True, parents=True)
    Config.OUTPUT_ROOT_DIR.mkdir(exist_ok=True, parents=True)
    Config.TTS_CACHE_DIR.mkdir(exist_ok=True, parents=True)

    use_tts = (Config.USE_REAL_TTS and CLOUD_TTS_AVAILABLE)
    use_concat = (REAL_CONCAT_AVAILABLE and FFMPEG_AVAILABLE)

    _log(f"--- 🚀 Environment Ready ---")
    _log(f"Engine: {'[LIVE] Google Cloud' if use_tts else '[MOCK] Logic-Only'}")
    _log(f"Audio:  {'[ENABLED] Merging MP3s' if use_concat else '[DISABLED] Metadata only'}")
    _log(f"Target: {Config.TARGET_LANG_CODE} ({Config.TARGET_VOICE_NAME})")
    _log(f"Storage: {Config.ICLOUD_BASE}\n")

    return use_tts, use_concat

# =========================================================================
# 7. Entry Points
# =========================================================================

def main_workflow(run_config: RunConfig = None) -> None:
    """Main entry point for both CLI and GUI. Raises ValueError on bad input."""
    global TTS_CLIENT
    if run_config is None:
        run_config = RunConfig()

    AUDIO_SEGMENT_CACHE.clear()
    TTS_CLIENT = None

    use_tts, use_concat = run_environment_check()

    if run_config.audio_format == 'm4a' and not use_concat:
        _log("⚠️  m4a requested but ffmpeg/pydub unavailable — falling back to mp3")
        run_config.audio_format = 'mp3'

    if use_tts:
        TTS_CLIENT = texttospeech.TextToSpeechClient(client_options=ClientOptions(api_key=os.getenv('GOOGLE_API_KEY')))

    if run_config.mode == 'pairs':
        sentence_pairs_workflow(run_config, use_tts, use_concat)
        return

    master, max_d = load_and_validate_source_data()
    if not master:
        raise ValueError("No source data found.")

    schedules = generate_full_repetition_schedule(master, max_d)
    days_processed = 0
    total_session_duration = 0.0

    for d in range(1, max_d + 1):
        if not is_day_complete(d, run_config.audio_format):
            day_dur = process_day(d, schedules.get(d, []), use_tts, use_concat, run_config.audio_format)
            if day_dur > 0:
                total_session_duration += day_dur
                days_processed += 1

    if days_processed == 0:
        _log(f"✅ All {max_d} days are up to date in iCloud.")
    else:
        total_m, total_s = divmod(int(total_session_duration), 60)
        _log(f"\n--- ✅ Session Complete ---")
        _log(f"Days Generated: {days_processed}")
        _log(f"Total Audio:    {total_m}m {total_s}s")

    if run_config.do_zip:
        zip_output_dir(Config.OUTPUT_ROOT_DIR, extra_files=[Config.SOURCE_FILE])

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LanguageLearner audio generator")
    parser.add_argument('--mode', choices=['sr', 'pairs'], default='sr')
    parser.add_argument('--source', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--base-lang', dest='base_lang')
    parser.add_argument('--base-voice', dest='base_voice')
    parser.add_argument('--target-lang', dest='target_lang')
    parser.add_argument('--target-voice', dest='target_voice')
    parser.add_argument('--format', choices=['mp3', 'm4a'], default='mp3', dest='audio_format')
    parser.add_argument('--zip', action='store_true')
    parser.add_argument('--template', default=next(iter(Config.TEMPLATES)))
    parser.add_argument('--mock', action='store_true')
    return parser.parse_args()

def apply_arg_overrides(args: argparse.Namespace) -> None:
    if args.source:       Config.SOURCE_FILE = args.source
    if args.output:       Config.OUTPUT_ROOT_DIR = args.output
    if args.base_lang:    Config.BASE_LANG_CODE = args.base_lang
    if args.base_voice:   Config.BASE_VOICE_NAME = args.base_voice
    if args.target_lang:  Config.TARGET_LANG_CODE = args.target_lang
    if args.target_voice: Config.TARGET_VOICE_NAME = args.target_voice
    if args.mock:         Config.USE_REAL_TTS = False

def cli_main() -> None:
    args = parse_args()
    apply_arg_overrides(args)
    run_config = RunConfig(
        mode=args.mode,
        audio_format=args.audio_format,
        do_zip=args.zip,
        template=args.template,
    )
    try:
        main_workflow(run_config)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)

if __name__ == "__main__":
    cli_main()
