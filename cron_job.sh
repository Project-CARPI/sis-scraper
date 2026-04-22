#!/bin/bash

# --- CONFIGURATION ---
TEMP_LOG="/tmp/current_run.log"
SERVER_NAME=$(hostname)

PYTHON_PATH="python3"
SIS_SCRAPER_DIR="scraper/sis_scraper"

SCRAPER_COMMAND="$PYTHON_PATH $SIS_SCRAPER_DIR/main.py scrape 1998 $(date +%Y)"
POSTPROCESS_COMMAND="$PYTHON_PATH $SIS_SCRAPER_DIR/main.py postprocess"
COMMIT_DB_COMMAND="$PYTHON_PATH $SIS_SCRAPER_DIR/main.py commitdb"

run_pipeline() {
    echo "--- Starting SIS Scraper: $(date) ---"

    echo "[Step 1] Scraping SIS..."
    $SCRAPER_COMMAND || return 1

    echo "[Step 2] Postprocessing..."
    $POSTPROCESS_COMMAND || return 1

    echo "[Step 3] Committing to DB..."
    $COMMIT_DB_COMMAND || return 1

    echo "--- SIS Scraper Finished: $(date) ---"
}

# --- EXECUTION ---
# Create log folder if it doesn't exist
mkdir -p $LOGS_DIR

# Run scraper, output to stdout and save to a log file
run_pipeline 2>&1 | tee "$TEMP_LOG"

# Capture exit status of run_pipeline
EXIT_STATUS=${PIPESTATUS[0]}

# --- CHECK FOR FAILURE ---
if [ $EXIT_STATUS -ne 0 ]; then
    ERROR_PREVIEW=$(tail -n 5 "$TEMP_LOG" | sed 's/"/\\"/g' | tr -d '\n')

    PAYLOAD=$(cat <<EOF
{
  "content": "🚨 **SIS Scraper Failed!**",
  "embeds": [{
    "title": "Failure Alert: $SERVER_NAME",
    "color": 15158332,
    "fields": [
      { "name": "Status", "value": "Pipeline exited with error", "inline": false },
      { "name": "Exit Code", "value": "$EXIT_STATUS", "inline": true },
      { "name": "Recent Errors", "value": "\`\`\`$ERROR_PREVIEW\`\`\`", "inline": false }
    ],
    "footer": { "text": "$(date)" }
  }]
}
EOF
)

    curl -H "Content-Type: application/json" -X POST -d "$PAYLOAD" "$SCRAPER_DISCORD_WEBHOOK_URL"
fi