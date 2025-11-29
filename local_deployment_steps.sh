# ----------------------------------------------------------------------
# Local Deployment Script for LanguageLearner
# This script creates a clean, self-contained directory (local_live_app)
# copies the necessary files, sets up dependencies, and executes the app.
# ----------------------------------------------------------------------

# --- Configuration ---
LIVE_DIR="local_language_learner_app"

echo "1. Creating the local deployment directory: $LIVE_DIR"
mkdir -p "$LIVE_DIR"
cd "$LIVE_DIR"

# --- Copy Core Files ---
echo "2. Copying core application files (excluding run_tests.py)..."

# 2a. Always copy the application code (language_learner.py) to get the latest fixes.
cp ../language_learner.py .
echo "    -> Updated application code (language_learner.py)."

# 2b. Conditional copy for the data file: ONLY copy if it does NOT exist.
# This prevents overwriting the user's manually updated live data on subsequent runs.
if [ ! -f "sentence_pairs.csv" ]; then
    cp ../sentence_pairs.csv .
    echo "    -> Copied sentence_pairs.csv for first-time setup."
else
    echo "    -> **SKIPPING** copy of sentence_pairs.csv to preserve live data."
    echo "       (Ensure you are editing the CSV directly in this '$LIVE_DIR' folder.)"
fi


# --- Setup Environment (Mandatory dependencies for the current mock version) ---
echo -e "\n3. Setting up Python Virtual Environment and dependencies (pandas is required)..."
python -m venv venv
source venv/Scripts/activate # Use 'source venv/bin/activate' on Linux/macOS
pip install pandas

# Note: While pydub is not strictly needed for the mock,
# it is required for the audio-joining logic when we integrate the real API.
# It's good practice to install it now for a more robust local setup.
# pip install pydub
# You will also need to ensure ffmpeg is installed on your system PATH for pydub to work fully.

# --- Execution ---
echo -e "\n4. Running the Language Learner App from the new location..."
echo "This run will generate the 'output' and 'cache' directories here."
echo "--- Application Output ---"

# The app is designed to run the orchestration when executed directly.
python language_learner.py

# --- Conclusion ---
echo -e "\n--- Script Complete ---"
echo "The Language Learner App has finished running in the '$LIVE_DIR' directory."
echo "You can find your generated schedule and audio artifacts in the 'output/' folder."
echo "To exit the virtual environment, run: deactivate"