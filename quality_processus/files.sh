#!/usr/bin/env bash

set -e

SESSION_DIR="$1"

if [ -z "$SESSION_DIR" ]; then
  echo "Usage: $0 <session_directory>"
  exit 1
fi

fail() {
  echo "[ERROR] $1"
  exit 1
}

check_file() {
  local path="$1"
  [ -f "$path" ] || fail "Missing file: $path"
}

check_dir() {
  local path="$1"
  [ -d "$path" ] || fail "Missing directory: $path"
}

echo "Checking session: $SESSION_DIR"

# Root checks
check_file "$SESSION_DIR/metadata.json"
check_file "$SESSION_DIR/tracker_positions.csv"
check_file "$SESSION_DIR/gripper_left_data.csv"
check_file "$SESSION_DIR/gripper_right_data.csv"

# Videos directory
VIDEOS_DIR="$SESSION_DIR/videos"
check_dir "$VIDEOS_DIR"

# Expected video files
for cam in head left right; do
  check_file "$VIDEOS_DIR/${cam}.mp4"
  check_file "$VIDEOS_DIR/${cam}.jsonl"
done

echo "[OK] All required files are present"
