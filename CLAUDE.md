# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the script

```bash
python language_learner.py                        # default SR mode (spaced repetition)
python language_learner.py --mock                 # logic-only, no real API calls
python language_learner.py --mode pairs --template workout_forward_transcribe!
python language_learner.py --zip                  # package output into a zip archive
python language_learner.py --format m4a           # output m4a instead of mp3
```

All CLI flags: `--source`, `--output`, `--base-lang`, `--base-voice`, `--target-lang`, `--target-voice`, `--format`, `--zip`, `--template`, `--mock`.

## Dependencies

- `google-cloud-texttospeech` — live TTS; gracefully absent (falls back to mock)
- `pydub` + system `ffmpeg` — audio concatenation; gracefully absent (creates placeholder files)
- `GOOGLE_API_KEY` environment variable — required for live TTS calls

## Architecture

Everything lives in `language_learner.py`. There are no other modules.

### Config class (line ~59)

Single source of truth for all configuration. Key fields:

- `SOURCE_FILE` / `OUTPUT_ROOT_DIR` — both live under `iCloudDrive/LanguageLearnerData/`; TTS cache stays local in `tts_cache/`
- `TEMPLATES` — dict of `name → (pattern, reps, speed, output_type)`
- `MACRO_REPETITION_INTERVALS` — across-day review schedule `[1, 3, 7, 14, 30, 60, 120, 240]`

### Template system

Each template entry is `(pattern, reps, speed, output_type)`:

- **pattern**: space-delimited tokens. `L1`/`L2` = content segments; `Xs` (e.g. `1.0s`) = silent pause
- **speed**: `!= 1.0` → workout template consuming **NEW** items; `== 1.0` → review template consuming **REVIEW** items
- **output_type**: `'audio'` generates an MP3/M4A; `'csv'` generates a CSV — CSVs are never produced automatically, they require an explicit template entry

CSV templates always receive `shuffled_review` (the same randomly-ordered list used by all review audio templates for that day), ensuring the printed list matches what the user hears.

### Spaced repetition workflow (`sr` mode)

1. `load_and_validate_source_data()` — reads the source CSV; requires `StudyDay` (int), `L1`, `L2` columns
2. `generate_full_repetition_schedule()` — for each calendar day, collects NEW items (StudyDay == day) and REVIEW items (StudyDay + macro_interval == day)
3. `process_day()` — generates missing files only; skips days where all generatable files exist; to force a full rebuild, delete the day folder
4. Review items are pooled across all due StudyDays and shuffled once; that same order is used for every review audio template and the vocab_list CSV

### TTS caching

Cache key = SHA-256 of `(text, language_code, voice_name, speed)` → `tts_cache/{hash}.mp3`. The cache is local (not iCloud) for speed. In mock mode, empty placeholder files are touched so cache-hit logic works correctly.

### Incremental output

`is_day_complete()` gates whether `process_day()` is called. Inside `process_day()`, each template checks for its output file individually and skips if present. Days with no applicable source data (e.g. Day 1 has no review items) return early silently without printing.

### `pairs` mode

Generates one audio file per row in the source CSV using a single named template. Produces a `manifest.csv` alongside the audio files. No spaced repetition scheduling.

### Zip packaging

`--zip` packages `OUTPUT_ROOT_DIR` and also includes the source CSV at the root of the archive, so the data and audio travel together.

## Source data format

The source CSV (`sentence_pairs_simple.csv`) requires: `L1` (base language sentence), `L2` (target language sentence), `StudyDay` (integer). `Imagery` is optional. The loader handles BOM, comma/semicolon delimiters, and whitespace in headers automatically.

New vocabulary is generated via the AI prompt in `AI Prompt for new words.txt`.
