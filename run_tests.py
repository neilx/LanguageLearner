import os
import shutil
import pandas as pd
from pathlib import Path

# IMPORTANT: Import the main execution function from the application script.
try:
    # This imports the 'main' function and aliases it to 'run_language_learner_workflow'
    from language_learner import main as run_language_learner_workflow
except ImportError as e:
    # Adjusted the error message to reflect the expected import: 'main' function
    print(f"FATAL: Could not import the main function from language_learner.py. Error: {e}")
    exit(1)

# --- Global Test Configuration ---
TEST_MASTER_CSV = 'sentence_pairs.csv' # Updated to remove the 'test_' prefix
# Renamed folders as requested:
TEST_OUTPUT_DIR = Path('output')
TEST_CACHE_DIR = Path('cache')

# Mock Data (30 items)
MOCK_DATA = """W2,W1,L1,L2,StudyDay
øve,practice,I try to practice guitar scales every day.,Jeg forsøger at øve guitar skalaer hver dag.,1
pension,retirement,He took early retirement last year to travel.,Han gik tidligt på pension sidste år for at rejse.,1
forbedre,improve,I want to improve my skills in spontaneous jamming.,Jeg ønsker at forbedre mine evner i spontan jamming.,1
bøf,steak,"My favorite meal is a thick, well-seasoned ribeye steak.","Mit yndlingsmåltid er en tyk, velkrydret ribeye bøf.",1
dagligvare,grocery,We need to buy basic groceries for the week.,Vi er nødt til at købe basale dagligvarer til ugen.,1
tryghed,security,"After retiring, financial security is very important.",Efter at have trukket mig tilbage er økonomisk tryghed meget vigtig.,2
akkord,chord,The guitarist played a complex diminished chord.,Guitaristen spillede en kompleks formindsket akkord.,2
mangle,lack,I realize I am missing one of my essential kitchen tools.,"Jeg er klar over, at jeg mangler et af mine essentielle køkkenredskaber.",2
tilpasning,adaptation,Successful improvisation relies on quick adaptation.,Succesfuld improvisation afhænger af hurtig tilpasning.,2
indkøb,shopping,I will handle the shopping for the household today.,Jeg tager mig af indkøbene til husholdningen i dag.,2
mæt,full,The high-fat diet keeps me feeling full for hours.,Den fedtrige kost holder me mæt i timevis.,2
guitarkasse,guitar case,I store my vintage instrument safely in its guitar case.,Jeg opbevarer mit vintage instrument sikkert i guitarkassen.,3
spare,save,We decided to save money for a big summer vacation.,Vi besluttede at spare penge op til en stor sommerferie.,3
ærlig,honest,I need you to be honest with me about the results.,"Jeg har brug for, at du er ærlig over for mig omkring resultaterne.",3
stemning,mood,"The blues music created a powerful, soulful mood.","Bluesmusikken skabte en stærk, sjælfuld stemning.",3
træning,training,I focus on weight training several times a week.,Jeg fokuserer på vægttræning flere gange ugentligt.,3
vente,wait,"We will have to wait for the bus, it's running late.","Vi bliver nødt til at vente på bussen, den er forsinket.",3
tilfældighed,chance,Musical inspiration often happens by chance or accident.,Musisk inspiration sker ofte ved tilfældighed eller uheld.,4
udfordring,challenge,The new lifestyle is proving to be a nutritional challenge.,Den nye livsstil viser sig at være en ernæringsmæssig udfordring.,4
aftale,agreement,We made an agreement to meet next Tuesday at noon.,Vi lavede en aftale om at mødes næste tirsdag middag.,4
ønske,wish,I wish I could play the guitar just like that master.,"Jeg ønsker, at jeg kunne spille guitar ligesom den mester.",4
rolig,calm,"After years of working, retirement should be a calm phase.",Efter mange års arbejde burde pensionisttilværelsen være en rolig fase.,4
frasering,phrasing,Good improvisation is all about melodic phrasing.,God improvisation handler om melodisk frasering.,4
smør,butter,I add a tablespoon of butter to my coffee every morning.,Jeg tilføjer en spiseskefuld smør til min kaffe hver morgen.,4
arbejdsplads,workplace,"I miss my old workplace, but not the stress.","Jeg savner min gamle arbejdsplads, men ikke stresset.",5
spørge,ask,"If you are unsure, you must ask the receptionist.","Hvis du er usikker, skal du spørge receptionisten.",5
teknik,technique,Mastering the slide guitar technique takes many hours.,At mestre slide guitar teknikken tager mange timer.,5
resultat,result,The test results showed a positive change in blood work.,Testresultaterne viste en positiv ændring i blodprøverne.,5
plads,space,Is there enough space left in the car for this box?,Er der plads nok tilbage i bilen for denne kasse?,5
glemme,forget,Please don't forget your guitar picks before you leave.,"Glem venligst ikke dine guitarplektre, før du tager af sted.",5
"""

# --- Helper Functions for Test Environment Setup/Teardown ---

def setup_test_environment():
    """Initial setup for paths and mock data."""
    print("--- Setting up Test Environment ---")
    
    # 1. Write the mock data to a temporary CSV file
    with open(TEST_MASTER_CSV, 'w', encoding='utf-8') as f:
        f.write(MOCK_DATA)

    # 2. Configure the application with test paths
    LanguageLearnerApp.OUTPUT_DIR = TEST_OUTPUT_DIR
    LanguageLearnerApp.CACHE_DIR = TEST_CACHE_DIR
    LanguageLearnerApp.MASTER_DATA_FILE = Path(TEST_MASTER_CSV)

    # 3. Clean environment before starting the first scenario
    if TEST_OUTPUT_DIR.exists():
        shutil.rmtree(TEST_OUTPUT_DIR)
    if TEST_CACHE_DIR.exists():
        shutil.rmtree(TEST_CACHE_DIR)
    print(f"✅ Paths set: Output='{TEST_OUTPUT_DIR}', Cache='{TEST_CACHE_DIR}'")
    
def cleanup_test_environment(preserve_output=True):
    """Final cleanup of the environment (only removing temporary files if needed)."""
    print("\n--- Final Test Cleanup ---")
    
    # Requirement: Preserving master data file
    print(f"✅ Preserving master data file: '{TEST_MASTER_CSV}'.")
    
    # Requirement: Preserving output and cache folders for inspection
    print(f"⚠️ **Preserving** final artifacts in '{TEST_OUTPUT_DIR}' and '{TEST_CACHE_DIR}' for inspection.")

# --- SCENARIO FUNCTIONS ---

def scenario_cleanup_on_audio_exception():
    """Scenario 1: Verify cleanup when a non-schedule exception is triggered."""
    print("\n## SCENARIO 1: Cleanup on Audio/Workflow Exception (Forced Failure) ##")
    app = LanguageLearnerApp()
    day_to_test = 1
    day_dir_path = app.day_dir(day_to_test) # Use app's helper for path

    # 1. Setup schedule and state
    try:
        app._load_or_generate_master_data()
        app._generate_srs_schedule()
        app.max_day = day_to_test
    except Exception as e:
        print(f"SETUP ERROR: {e}")
        return False

    # 2. Corrupt the schedule (forcing an early WorkflowException for cleanup test)
    schedule_df = app.daily_schedules[day_to_test]
    try:
        # Drop one item to cause the internal consistency check to fail later
        schedule_df.drop(schedule_df.index[len(schedule_df) - 2], inplace=True)
        app.daily_schedules[day_to_test] = schedule_df
    except IndexError:
        print("FAILURE: Schedule corruption index out of bounds.")
        return False

    # 3. Create the output directory BEFORE failure to ensure cleanup has something to delete
    day_dir_path.mkdir(parents=True, exist_ok=True)
    if not day_dir_path.exists():
        print("SETUP FAILED: Day 1 directory was not created.")
        return False
    
    print(f"  > Output folder '{day_dir_path}' created for cleanup test.")
    
    # 4. Run the process, expecting an exception
    try:
        app._process_day(day_to_test, app.daily_schedules[day_to_test])
        print("  ❌ FAIL: Expected WorkflowException was NOT raised.")
        return False
    except WorkflowException as e:
        print(f"  ✅ PASS: Caught expected WorkflowException: {e}")
        if not day_dir_path.exists():
            print(f"  ✅ PASS: Cleanup successful. Directory '{day_dir_path.name}/' removed.")
            return True
        else:
            print(f"  ❌ FAIL: Cleanup failed. Directory '{day_dir_path.name}/' still exists.")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: Caught unexpected exception: {e}")
        return False


def scenario_internal_consistency_failure_triggers_cleanup():
    """Scenario 2: Verify cleanup when internal consistency check fails."""
    print("\n## SCENARIO 2: Internal Consistency Failure Triggers Cleanup (Forced Failure) ##")
    app = LanguageLearnerApp()
    day_to_test = 1
    day_dir_path = app.day_dir(day_to_test) # Use app's helper for path

    # 1. Setup schedule and state
    try:
        app._load_or_generate_master_data()
        app._generate_srs_schedule()
        app.max_day = day_to_test
    except Exception as e:
        print(f"SETUP ERROR: {e}")
        return False

    # 2. Corrupt the schedule
    schedule_df = app.daily_schedules[day_to_test]
    try:
        # Drop a different item to create a sequence gap
        schedule_df.drop(schedule_df.index[0], inplace=True)
        app.daily_schedules[day_to_test] = schedule_df
    except IndexError:
        print("FAILURE: Schedule corruption index out of bounds.")
        return False

    # 3. Create the output directory BEFORE failure
    day_dir_path.mkdir(parents=True, exist_ok=True)
    if not day_dir_path.exists():
        print("SETUP FAILED: Day 1 directory was not created.")
        return False
    
    print(f"  > Output folder '{day_dir_path}' created for cleanup test.")
    
    # 4. Run the process, expecting an exception
    try:
        app._process_day(day_to_test, app.daily_schedules[day_to_test])
        print("  ❌ FAIL: Expected WorkflowException was NOT raised.")
        return False
    except WorkflowException as e:
        print(f"  ✅ PASS: Caught expected WorkflowException (Consistency Failure): {e}")
        if not day_dir_path.exists():
            print(f"  ✅ PASS: Cleanup successful. Directory '{day_dir_path.name}/' removed.")
            return True
        else:
            print(f"  ❌ FAIL: Cleanup failed. Directory '{day_dir_path.name}/' still exists.")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: Caught unexpected exception: {e}")
        return False


def scenario_full_workflow_and_caching():
    """Scenario 3: Runs 5 days, verifies skip, and verifies cache hit."""
    print("\n## SCENARIO 3: Full Workflow, Skip, and Caching (Success Path) ##")
    
    # --- Part 1: Initial Full Run ---
    app_1 = LanguageLearnerApp()
    print("\n--- 1. FULL RUN: Artifact Generation & Initial Run ---")
    try:
        app_1.run_orchestration()
        initial_calls = app_1._total_tts_api_calls
        if initial_calls == 0:
            print("  ❌ FAIL: Initial run made 0 TTS calls. Data must have been cached unexpectedly.")
            return False
        
        day_5_dir = app_1.day_dir(5)
        if not day_5_dir.exists():
             print("  ❌ FAIL: Day 5 output directory was not created.")
             return False

        print(f"  ✅ PASS: Initial Run Completed. Total TTS calls: {initial_calls}.")
    except Exception as e:
        print(f"  ❌ FAIL: Full Run failed with exception: {e}")
        return False

    # --- Part 2: Zero-Work Skip Verification ---
    app_2 = LanguageLearnerApp()
    print("\n--- 2. ZERO-WORK SKIP VERIFICATION ---")
    try:
        app_2.run_orchestration()
        # ASSERTION: Zero calls on the second run because all output files exist
        if app_2._total_tts_api_calls == 0:
            print("  ✅ PASS: Zero-Work Skip Verified. Second run calls: 0 new calls.")
        else:
            print(f"  ❌ FAIL: Zero-Work Skip failed: App made {app_2._total_tts_api_calls} NEW TTS calls.")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: Skip Verification failed with exception: {e}")
        return False

    # --- Part 3: Tier 1 Cache Hit Verification ---
    app_3 = LanguageLearnerApp()
    new_speed = 1.5 
    app_3.L2_SPEED_FACTOR = new_speed
    
    print("\n--- 3. TIER 1 CACHE HIT VERIFICATION (Forced Re-generation) ---")
    
    # Manually delete ALL output (but keep cache) to force full re-generation
    for day in range(1, 6):
        day_dir = app_3.day_dir(day)
        if day_dir.exists():
            shutil.rmtree(day_dir)
            
    try:
        app_3.run_orchestration()
        total_calls_after_rerun = app_3._total_tts_api_calls
        
        # ASSERTION: Total calls must remain 0 (segments loaded from Tier 1 PCM cache)
        if total_calls_after_rerun == 0:
             print("  ✅ PASS: Two-Tier Cache Hit Verified. Final run calls: 0 new calls.")
             return True
        else:
            print(f"  ❌ FAIL: Tier 1 Cache Failed: Expected 0 new calls, but found {total_calls_after_rerun}.")
            return False
    except Exception as e:
        print(f"  ❌ FAIL: Cache Hit Verification failed with exception: {e}")
        return False


# --- MAIN EXECUTION ---

if __name__ == '__main__':
    print("--- STARTING LANGUAGE LEARNER SCENARIO TESTS ---")
    
    # 1. Setup
    setup_test_environment()
    
    overall_success = True
    
    # 2. Run Scenarios
    
    # The order is important: Run failures first, then the success path which leaves the artifacts.
    if not scenario_cleanup_on_audio_exception():
        overall_success = False

    if not scenario_internal_consistency_failure_triggers_cleanup():
        overall_success = False
        
    if not scenario_full_workflow_and_caching():
        overall_success = False
    
    # 3. Final Cleanup (Preserve master CSV and output files)
    cleanup_test_environment(preserve_output=True)
    
    if overall_success:
        print("\n*** ALL SCENARIOS PASSED ***")
    else:
        print("\n*** ONE OR MORE SCENARIOS FAILED ***")