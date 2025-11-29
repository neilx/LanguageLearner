import os
import shutil
import pandas as pd # <-- NEW: Import pandas for data generation
from language_learner import LanguageLearnerApp

# --- Configuration for Performance Testing ---
DAILY_ITEM_COUNTS = {1: 5, 2: 6, 3: 7} # Day 1: 5 items, Day 2: 6 items, Day 3: 7 items
MAX_DAY = 3

# Import static helper methods directly from the class
_day_dir = LanguageLearnerApp.day_dir
_output_paths = LanguageLearnerApp.output_paths
_master_csv_path = LanguageLearnerApp.master_csv_path

def reset_environment():
    """Wipes the output, cache directories, and master CSV."""
    if os.path.exists(LanguageLearnerApp.OUTPUT_DIR):
        shutil.rmtree(LanguageLearnerApp.OUTPUT_DIR)
    if os.path.exists(LanguageLearnerApp.CACHE_DIR):
        shutil.rmtree(LanguageLearnerApp.CACHE_DIR)
    if os.path.exists(_master_csv_path()):
        os.remove(_master_csv_path())

def generate_test_data():
    """Generates the sentence_pairs.csv using custom counts."""
    print(f"Generating custom test data for {MAX_DAY} days...")
    
    total_items = sum(DAILY_ITEM_COUNTS.values())
    
    data = {
        'item_id': list(range(1, total_items + 1)),
        'w2': [f'danish_word_{i}' for i in range(1, total_items + 1)],
        'w1': [f'english_word_{i}' for i in range(1, total_items + 1)],
        'l1_text': [f'This is English sentence {i}.' for i in range(1, total_items + 1)],
        'l2_text': [f'Dette er dansk sætning {i}.' for i in range(1, total_items + 1)],
        'study_day': []
    }
    
    # Assign study days based on the custom counts
    for day, count in DAILY_ITEM_COUNTS.items():
        data['study_day'].extend([day] * count)
        
    master_df = pd.DataFrame(data, columns=LanguageLearnerApp.SCHEDULE_COLUMNS)
    master_df.to_csv(LanguageLearnerApp.master_csv_path(), index=False)
    print(f"Test data saved with {total_items} items.")


def scenario_1_clean_start():
    print("\n[TEST] Scenario 1: Clean Start")
    reset_environment()
    generate_test_data() # <-- Step 1: Create the data file
    
    # 2. Run the application
    app = LanguageLearnerApp()
    app.run_orchestration()
    
    # 3. Assert all outputs for all 3 days exist
    assert os.path.exists(_master_csv_path()), "Master CSV not created"
    for day in range(1, MAX_DAY + 1):
        paths = _output_paths(day)
        for p in paths.values():
            assert os.path.exists(p), f"Missing {p}"
            
    print("[PASS] Scenario 1")

def scenario_2_no_work():
    print("\n[TEST] Scenario 2: No Work")
    # The environment is fully populated from Scenario 1
    app = LanguageLearnerApp()
    app.run_orchestration()
    
    print("[PASS] Scenario 2 (no days to process)")

def scenario_3_partial_recovery():
    print("\n[TEST] Scenario 3: Partial Recovery")
    
    # 1. Set up the failure state: Remove one file from the last scheduled day (Day 3)
    paths = _output_paths(MAX_DAY) # MAX_DAY is 3
    
    assert os.path.exists(paths["workflow_mp3"]), f"Pre-condition fail: workflow.mp3 must exist for day {MAX_DAY}"
    os.remove(paths["workflow_mp3"])
    
    # 2. Run the application (should detect missing file for Day 3 and re-process)
    app = LanguageLearnerApp()
    app.run_orchestration()
    
    # 3. Assert all outputs are back
    paths_day_to_check = _output_paths(MAX_DAY)
    for p in paths_day_to_check.values():
        assert os.path.exists(p), f"Missing {p} after recovery"
        
    print("[PASS] Scenario 3")

def run_all():
    scenario_1_clean_start()
    scenario_2_no_work()
    scenario_3_partial_recovery()

if __name__ == "__main__":
    run_all()