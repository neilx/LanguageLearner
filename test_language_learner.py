import os
import shutil
import time
from language_learner import LanguageLearnerApp, _day_dir, _output_paths, _master_csv_path

def reset_environment():
    if os.path.exists("data"):
        shutil.rmtree("data")

def scenario_1_clean_start():
    print("\n[TEST] Scenario 1: Clean Start")
    reset_environment()
    app = LanguageLearnerApp()
    app.run_orchestration()
    assert os.path.exists(_master_csv_path()), "Master CSV not created"
    for day in range(1, 5):
        paths = _output_paths(day)
        for p in paths.values():
            assert os.path.exists(p), f"Missing {p}"
    print("[PASS] Scenario 1")

def scenario_2_no_work():
    print("\n[TEST] Scenario 2: No Work")
    app = LanguageLearnerApp()
    app.run_orchestration()
    print("[PASS] Scenario 2 (no days to process)")

def scenario_3_partial_recovery():
    print("\n[TEST] Scenario 3: Partial Recovery")
    # Remove one file from day 3
    paths = _output_paths(3)
    os.remove(paths["workflow_mp3"])
    app = LanguageLearnerApp()
    app.run_orchestration()
    for p in paths.values():
        assert os.path.exists(p), f"Missing {p}"
    print("[PASS] Scenario 3")

def run_all():
    scenario_1_clean_start()
    scenario_2_no_work()
    scenario_3_partial_recovery()

if __name__ == "__main__":
    run_all()
