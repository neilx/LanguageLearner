import pandas as pd
import numpy as np
from pathlib import Path
import os
import hashlib
from typing import List, Dict, Any
import logging 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# --- External Audio Libraries ---
try:
    from gtts import gTTS
    from pydub import AudioSegment
except ImportError:
    logger.error("Error: Required libraries (gTTS, pydub) not found.")
    logger.error("Please ensure you installed them in your environment.")
    exit()

# --- CRITICAL: FFmpeg Path Configuration ---
try:
    pass 
except:
    pass

class LanguageLearnerApp:
    """
    Orchestrates the language learning workflow, including data loading,
    SRS scheduling, and MP3 generation with declarative recovery.
    """
    
    # --- 1. Class Constants (Configuration) ---
    WORKFLOW_VERSION = 'v0.027' 
    REVIEW_RATIO = 5 
    EXPECTED_FILE_COUNT = 4 
    L2_SPEED_FACTOR = 1.4
    PAUSE_L1_MS = 500
    PAUSE_L2_MS = 1000
    PAUSE_SHADOW_MS = 2500

    # --- 2. NFR-6 IMMUTABLE NAMES & PATHS ---
    MASTER_DATA_FILE = Path('sentence_pairs.csv')
    OUTPUT_DIR = Path('output') 
    CACHE_DIR = Path('cache') 
    
    EXPECTED_FILES = [
        'workflow.mp3', 'review.mp3', 'reverse.mp3', 'schedule.csv'
    ]
    SCHEDULE_COLUMNS = [
        'item_id', 'w2', 'w1', 'l1_text', 'l2_text', 'study_day'
    ]

    def __init__(self):
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        self.CACHE_DIR.mkdir(exist_ok=True) 
        
        self.master_df = pd.DataFrame()
        self.schedule_by_day = {}
        self.max_day = 0
        
        # Attribute to collect cache miss logs during processing
        self.cache_miss_log: List[str] = [] 

    # --- A. Test Runner Helper Functions (Static/Exposed) ---

    @staticmethod
    def master_csv_path() -> Path:
        return LanguageLearnerApp.MASTER_DATA_FILE

    @staticmethod
    def day_dir(day: int) -> Path:
        return LanguageLearnerApp.OUTPUT_DIR / f'day_{day}'

    @staticmethod
    def output_paths(day: int) -> Dict[str, Path]:
        day_folder = LanguageLearnerApp.day_dir(day)
        return {
            'schedule_csv': day_folder / 'schedule.csv',
            'workflow_mp3': day_folder / 'workflow.mp3',
            'review_mp3': day_folder / 'review.mp3',
            'reverse_mp3': day_folder / 'reverse.mp3',
        }
        
    # --- B. Cache and Audio Utility Methods ---

    def _get_cached_audio_path(self, text: str) -> Path:
        hash_object = hashlib.md5(text.strip().lower().encode('utf-8'))
        filename = f"{hash_object.hexdigest()}.mp3"
        return self.CACHE_DIR / filename

    @staticmethod
    def _apply_speed_change(segment: AudioSegment, speed: float) -> AudioSegment:
        if speed == 1.0:
            return segment
        return segment.set_frame_rate(int(segment.frame_rate * speed))

    def _tts_generate_and_cache(self, text: str, lang: str, is_l2: bool) -> AudioSegment:
        """Generates TTS audio, uses the cache if available, and applies speed factor."""
        cache_path = self._get_cached_audio_path(text)
        
        if cache_path.is_file():
            audio_segment = AudioSegment.from_mp3(cache_path)
        else:
            # Collect simple log entry: Language and synthesis status
            log_msg = f"{lang.upper()} (Synthesized)" 
            self.cache_miss_log.append(log_msg)
            
            temp_file_path = self.CACHE_DIR / f'temp_{cache_path.name}'
            
            tts = gTTS(text=text, lang=lang, slow=False)
            tts.save(temp_file_path)
            
            audio_segment = AudioSegment.from_mp3(temp_file_path)
            
            if is_l2:
                audio_segment = self._apply_speed_change(audio_segment, self.L2_SPEED_FACTOR)

            audio_segment.export(cache_path, format="mp3")
            
            os.remove(temp_file_path)

        return audio_segment

    # --- C. Core Workflow Methods (Step 1 & 2) ---

    def _load_or_generate_master_data(self) -> None:
        """1. Load Master Data. **Requires sentence_pairs.csv to exist.**"""
        logger.info(f"Loading master data from {self.MASTER_DATA_FILE}...")
        
        if self.MASTER_DATA_FILE.exists():
            self.master_df = pd.read_csv(self.MASTER_DATA_FILE)
        else:
            logger.error("FATAL: Master data file not found (sentence_pairs.csv). Cannot run workflow.")
            raise FileNotFoundError(f"Required master data file not found at {self.MASTER_DATA_FILE}")

        self.master_df['study_day'] = self.master_df['study_day'].astype(int)
        logger.info(f"Master data loaded with {len(self.master_df)} items.")

    def _generate_srs_schedule(self) -> None:
        """2. Generate SRS Schedule and store in self.schedule_by_day."""
        logger.info("Generating SRS schedule by study_day...")
        schedule_by_day = self.master_df.groupby('study_day')
        self.schedule_by_day = {day: df for day, df in schedule_by_day}
        self.max_day = max(self.schedule_by_day.keys()) if self.schedule_by_day else 0
        
    # --- D. Recovery and Review Methods (Step 3 & 4 Helpers) ---

    def _determine_days_to_process(self) -> List[int]:
        """3. Determine Days to Process (Declarative Filter)."""
        logger.info("Determining incomplete days using declarative file check...")
        days_to_process = []
        
        for day in range(1, self.max_day + 1):
            day_folder = self.day_dir(day)
            
            all_files_exist = all(
                (day_folder / expected_file).is_file() for expected_file in self.EXPECTED_FILES
            )

            if not all_files_exist:
                days_to_process.append(day)
                
        logger.info(f"Days identified for processing: {days_to_process}")
        return days_to_process

    def _get_all_past_items(self, processed_days: List[int]) -> pd.DataFrame:
        """Retrieves all items from schedule.csv files for completely processed days."""
        all_past_items = []
        
        for day in processed_days:
            schedule_path = self.day_dir(day) / 'schedule.csv'
            if schedule_path.is_file():
                try:
                    df = pd.read_csv(schedule_path)
                    all_past_items.append(df)
                except pd.errors.EmptyDataError:
                    pass
        
        if all_past_items:
            # Only include columns that are guaranteed to exist
            return pd.concat(all_past_items, ignore_index=True)[self.SCHEDULE_COLUMNS]
        else:
            return pd.DataFrame(columns=self.SCHEDULE_COLUMNS)

    # CORRECTED: Fixed NameError by assigning segment variables inside the loop
    def _generate_daily_mp3s(self, day_df: pd.DataFrame, day: int) -> None:
        """Generates and saves the three required MP3 files for the day."""
        day_folder = self.day_dir(day)
        logger.info(f"  > Generating final MP3 outputs for Day {day}...")
        
        self.cache_miss_log = [] 
        
        pause_l1 = AudioSegment.silent(duration=self.PAUSE_L1_MS) 
        pause_l2 = AudioSegment.silent(duration=self.PAUSE_L2_MS) 
        pause_shadow = AudioSegment.silent(duration=self.PAUSE_SHADOW_MS)

        workflow_audio = AudioSegment.empty()
        review_audio = AudioSegment.empty()
        reverse_audio = AudioSegment.empty()
        
        total_items = len(day_df)
        total_segments = total_items * 2

        for _, row in day_df.iterrows():
            # FIX: Assign the segment variables
            l1_segment = self._tts_generate_and_cache(row['l1_text'], 'en', is_l2=False)
            l2_segment = self._tts_generate_and_cache(row['l2_text'], 'da', is_l2=True)

            # Concatenation now works
            workflow_audio += l1_segment + pause_l1 + l2_segment + pause_l2
            review_audio += l2_segment + pause_shadow
            reverse_audio += l2_segment + pause_l2 + l1_segment + pause_l2

        # Log only the count
        miss_count = len(self.cache_miss_log)
        hit_count = total_segments - miss_count
        
        logger.info(f"  > Audio Segments: {total_segments} total. Hits: {hit_count}. Misses: {miss_count} (Synthesized).")

        # 3. Export the final combined tracks
        workflow_audio.export(day_folder / 'workflow.mp3', format='mp3')
        review_audio.export(day_folder / 'review.mp3', format='mp3')
        reverse_audio.export(day_folder / 'reverse.mp3', format='mp3')
        
        logger.info(f"  ✅ Successfully generated 3 MP3 files in {day_folder.name}/")


    def _process_day(self, day: int, new_items_df: pd.DataFrame, processed_days: List[int]) -> None:
        """4. Processes one incomplete day."""
        day_folder = self.day_dir(day)
        day_folder.mkdir(exist_ok=True) 

        logger.info(f"\n--- ⏳ Processing Day {day} (v{self.WORKFLOW_VERSION}) ---")
        
        # 1. Select New Items and Review Items
        new_count = len(new_items_df)
        review_count = new_count // self.REVIEW_RATIO
        past_items_df = self._get_all_past_items(processed_days)
        
        if len(past_items_df) > 0 and review_count > 0:
            review_items_df = past_items_df.sample(n=min(review_count, len(past_items_df)))
        else:
            review_items_df = pd.DataFrame(columns=self.SCHEDULE_COLUMNS)

        logger.info(f"  > New: {new_count} items. Review: {len(review_items_df)} items.")

        # 2. Combine and Interleave (Shuffle)
        daily_schedule_df = pd.concat([new_items_df, review_items_df], ignore_index=True)
        daily_schedule_df = daily_schedule_df.sample(frac=1).reset_index(drop=True)
        
        # 3. Generate schedule.csv
        schedule_path = day_folder / 'schedule.csv'
        daily_schedule_df.to_csv(schedule_path, index=False, columns=self.SCHEDULE_COLUMNS)
        logger.info(f"  > Saved daily schedule (Total items: {len(daily_schedule_df)}) to {schedule_path.name}")
        
        # 4. Generate MP3s 
        self._generate_daily_mp3s(daily_schedule_df, day)
        
        # 5. Validation
        current_file_count = len([f for f in day_folder.iterdir() if f.is_file() and not f.name.startswith('.')])
        if current_file_count == self.EXPECTED_FILE_COUNT:
            logger.info(f"  ✅ Day {day} processing **SUCCESS**. All {self.EXPECTED_FILE_COUNT} outputs found.")
        else:
            logger.error(f"  ❌ Day {day} processing **FAILED**. Found {current_file_count}/{self.EXPECTED_FILE_COUNT} files.")


    # --- E. Main Orchestration Method ---
    
    def run_orchestration(self) -> None:
        """Orchestrates the four-step application flow."""
        logger.info(f"## Language Learner Workflow v{self.WORKFLOW_VERSION} ##")
        
        try:
            self._load_or_generate_master_data()
        except FileNotFoundError:
            logger.error("Exiting workflow due to missing master data file.")
            return

        self._generate_srs_schedule()
        
        all_scheduled_days = set(self.schedule_by_day.keys())
        
        days_to_process = self._determine_days_to_process()
        
        processed_days = sorted(list(all_scheduled_days - set(days_to_process)))
        logger.info(f"Days currently considered 'complete' for review item selection: {processed_days}")

        if not days_to_process:
            logger.info("\n--- ✅ All Scheduled Days Are Complete ---")
            return

        for day in sorted(days_to_process):
            if day in self.schedule_by_day:
                new_items_df = self.schedule_by_day[day]
                self._process_day(day, new_items_df, processed_days)
                
                current_day_folder = self.day_dir(day)
                current_file_count = len([f for f in current_day_folder.iterdir() if f.is_file() and not f.name.startswith('.')])
                if current_file_count == self.EXPECTED_FILE_COUNT:
                        processed_days.append(day)
                        processed_days = sorted(list(set(processed_days)))
            else:
                logger.warning(f"Warning: Day {day} was marked for processing but has no new items in master data.")


if __name__ == '__main__':
    app = LanguageLearnerApp()
    app.run_orchestration()