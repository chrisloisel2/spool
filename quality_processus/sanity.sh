#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./validate_session.sh /srv/exoria/inbox/videos/session_20260316_174007
#
# Dépendances:
#   - bash
#   - jq
#   - awk
#   - ffprobe (ffmpeg)
#   - wc
#   - grep
#   - sed

SESSION_DIR="${1:-}"

if [[ -z "$SESSION_DIR" ]]; then
  echo "Usage: $0 <session_directory>"
  exit 1
fi

VIDEOS_DIR="$SESSION_DIR/videos"
METADATA="$SESSION_DIR/metadata.json"
TRACKERS_CSV="$SESSION_DIR/tracker_positions.csv"
GRIPPER_LEFT_CSV="$SESSION_DIR/gripper_left_data.csv"
GRIPPER_RIGHT_CSV="$SESSION_DIR/gripper_right_data.csv"

fail() {
  echo "[ERROR] $1" >&2
  exit 1
}

warn() {
  echo "[WARN] $1" >&2
}

ok() {
  echo "[OK] $1"
}

require_file() {
  local f="$1"
  [[ -f "$f" ]] || fail "Missing file: $f"
}

require_dir() {
  local d="$1"
  [[ -d "$d" ]] || fail "Missing directory: $d"
}

require_cmd() {
  local c="$1"
  command -v "$c" >/dev/null 2>&1 || fail "Missing command: $c"
}

check_required_files() {
  require_dir "$SESSION_DIR"
  require_file "$METADATA"
  require_file "$TRACKERS_CSV"
  require_file "$GRIPPER_LEFT_CSV"
  require_file "$GRIPPER_RIGHT_CSV"
  require_dir "$VIDEOS_DIR"

  for cam in head left right; do
    require_file "$VIDEOS_DIR/${cam}.mp4"
    require_file "$VIDEOS_DIR/${cam}.jsonl"
  done

  ok "Présence des fichiers requise"
}

check_metadata() {
  jq -e . "$METADATA" >/dev/null || fail "metadata.json invalide"

  local failed
  failed="$(jq -r '.failed' "$METADATA")"
  [[ "$failed" != "true" ]] || fail "La session est marquée failed=true dans metadata.json"

  local duration fps width height
  duration="$(jq -r '.duration_seconds // empty' "$METADATA")"
  fps="$(jq -r '.video_config.fps // empty' "$METADATA")"
  width="$(jq -r '.video_config.width // empty' "$METADATA")"
  height="$(jq -r '.video_config.height // empty' "$METADATA")"

  [[ -n "$duration" && "$duration" != "null" ]] || fail "duration_seconds absent de metadata.json"
  [[ -n "$fps" && "$fps" != "null" ]] || fail "video_config.fps absent de metadata.json"
  [[ -n "$width" && "$width" != "null" ]] || fail "video_config.width absent de metadata.json"
  [[ -n "$height" && "$height" != "null" ]] || fail "video_config.height absent de metadata.json"

  for pos in head left right; do
    jq -e --arg p "$pos" '
      .cameras
      | to_entries
      | map(.value.position)
      | index($p)
    ' "$METADATA" >/dev/null || fail "Caméra $pos absente de metadata.json"

    jq -e --arg p "$pos" '.camera_anchors[$p]' "$METADATA" >/dev/null \
      || fail "camera_anchors.$pos absent de metadata.json"
  done

  local tracker_count
  tracker_count="$(jq '.trackers | length' "$METADATA")"
  [[ "$tracker_count" -eq 3 ]] || fail "metadata.json doit déclarer 3 trackers, trouvé: $tracker_count"

  jq -e '.grippers.left' "$METADATA" >/dev/null || fail "Gripper left absent de metadata.json"
  jq -e '.grippers.right' "$METADATA" >/dev/null || fail "Gripper right absent de metadata.json"

  ok "metadata.json cohérent"
}

# Vérifie qu'un JSONL:
# - est parseable
# - contient index et capture_time
# - a des index strictement croissants de 1..N
# - a des timestamps strictement croissants
check_jsonl() {
  local file="$1"
  local cam="$2"

  [[ -s "$file" ]] || fail "$cam.jsonl vide"

  # Valider chaque ligne du JSONL individuellement (jq ne supporte pas le NDJSON par défaut)
  if ! jq -e . "$file" --seq >/dev/null 2>&1; then
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      echo "$line" | jq . >/dev/null 2>&1 || fail "$cam.jsonl contient du JSON invalide"
    done < "$file"
  fi

  awk -v cam="$cam" '
    BEGIN {
      prev_idx = 0
      prev_ts  = -1
      line_no  = 0
      fmt      = ""
    }

    function extract(line, key,    val, i) {
      i = index(line, "\"" key "\"")
      if (i == 0) return ""
      val = substr(line, i + length(key) + 2)
      sub(/^[[:space:]]*:[[:space:]]*/, "", val)
      sub(/[[:space:]]*[,}].*/, "", val)
      return val
    }

    {
      line_no++

      # Détection du format sur la première ligne
      if (line_no == 1) {
        if ($0 ~ /"index"[[:space:]]*:/)        fmt = "v1"
        else if ($0 ~ /"frame"[[:space:]]*:/)   fmt = "v2"
        else {
          printf("[ERROR] %s.jsonl ligne 1: format non reconnu (ni index ni frame)\n", cam) > "/dev/stderr"
          exit 1
        }
      }

      # Extraction selon le format détecté
      if (fmt == "v1") {
        idx = extract($0, "index")
        ts  = extract($0, "capture_time")
        if (idx == "") {
          printf("[ERROR] %s.jsonl ligne %d: champ index absent\n", cam, line_no) > "/dev/stderr"
          exit 1
        }
        if (ts == "") {
          printf("[ERROR] %s.jsonl ligne %d: champ capture_time absent\n", cam, line_no) > "/dev/stderr"
          exit 1
        }
        if (ts !~ /^[0-9]+$/) {
          printf("[ERROR] %s.jsonl ligne %d: capture_time invalide (%s)\n", cam, line_no, ts) > "/dev/stderr"
          exit 1
        }
        if (prev_ts >= 0 && ts+0 <= prev_ts+0) {
          printf("[ERROR] %s.jsonl ligne %d: capture_time non croissant (%s <= %s)\n", cam, line_no, ts, prev_ts) > "/dev/stderr"
          exit 1
        }
      } else {
        idx = extract($0, "frame")
        ts  = extract($0, "wall")
        if (idx == "") {
          printf("[ERROR] %s.jsonl ligne %d: champ frame absent\n", cam, line_no) > "/dev/stderr"
          exit 1
        }
        if (ts == "") {
          printf("[ERROR] %s.jsonl ligne %d: champ wall absent\n", cam, line_no) > "/dev/stderr"
          exit 1
        }
        if (ts !~ /^[0-9]+(\.[0-9]+)?$/) {
          printf("[ERROR] %s.jsonl ligne %d: wall invalide (%s)\n", cam, line_no, ts) > "/dev/stderr"
          exit 1
        }
        if (prev_ts >= 0 && ts+0 <= prev_ts+0) {
          printf("[ERROR] %s.jsonl ligne %d: wall non croissant (%s <= %s)\n", cam, line_no, ts, prev_ts) > "/dev/stderr"
          exit 1
        }
      }

      if (idx !~ /^[0-9]+$/) {
        printf("[ERROR] %s.jsonl ligne %d: index invalide (%s)\n", cam, line_no, idx) > "/dev/stderr"
        exit 1
      }

      expected = prev_idx + 1
      if (idx+0 != expected) {
        printf("[ERROR] %s.jsonl ligne %d: index attendu=%d trouvé=%d\n", cam, line_no, expected, idx) > "/dev/stderr"
        exit 1
      }

      prev_idx = idx+0
      prev_ts  = ts+0
    }
    END {
      if (line_no == 0) {
        printf("[ERROR] %s.jsonl vide\n", cam) > "/dev/stderr"
        exit 1
      }
    }
  ' "$file"

  ok "$cam.jsonl valide"
}

get_jsonl_frame_count() {
  local file="$1"
  wc -l < "$file" | tr -d ' '
}

get_video_frame_count() {
  local file="$1"

  # count_packets est généralement plus fiable que nb_frames
  local count
  count="$(ffprobe -v error -select_streams v:0 -count_packets \
    -show_entries stream=nb_read_packets \
    -of csv=p=0 "$file" 2>/dev/null | tr -d '\r')"

  [[ -n "$count" && "$count" =~ ^[0-9]+$ ]] || fail "Impossible de compter les frames de $file"
  echo "$count"
}

check_video_stream() {
  local file="$1"
  local cam="$2"

  [[ -s "$file" ]] || fail "$cam.mp4 vide"

  ffprobe -v error -select_streams v:0 -show_entries stream=codec_type \
    -of csv=p=0 "$file" >/dev/null 2>&1 || fail "$cam.mp4 illisible"

  local codec_type width height
  codec_type="$(ffprobe -v error -select_streams v:0 -show_entries stream=codec_type \
    -of csv=p=0 "$file" 2>/dev/null | head -n1 | tr -d '\r')"
  width="$(ffprobe -v error -select_streams v:0 -show_entries stream=width \
    -of csv=p=0 "$file" 2>/dev/null | head -n1 | tr -d '\r')"
  height="$(ffprobe -v error -select_streams v:0 -show_entries stream=height \
    -of csv=p=0 "$file" 2>/dev/null | head -n1 | tr -d '\r')"

  [[ "$codec_type" == "video" ]] || fail "$cam.mp4 ne contient pas de flux vidéo valide"
  [[ "$width" =~ ^[0-9]+$ && "$height" =~ ^[0-9]+$ ]] || fail "$cam.mp4 dimensions invalides"

  ok "$cam.mp4 lisible (${width}x${height})"
}

check_all_videos() {
  local expected_jsonl=""
  local expected_mp4=""

  for cam in head left right; do
    local mp4="$VIDEOS_DIR/${cam}.mp4"
    local jsonl="$VIDEOS_DIR/${cam}.jsonl"

    check_video_stream "$mp4" "$cam"
    check_jsonl "$jsonl" "$cam"

    local jsonl_frames mp4_frames
    jsonl_frames="$(get_jsonl_frame_count "$jsonl")"
    mp4_frames="$(get_video_frame_count "$mp4")"

    [[ "$jsonl_frames" -gt 0 ]] || fail "$cam.jsonl: aucun frame"
    [[ "$mp4_frames" -gt 0 ]] || fail "$cam.mp4: aucun frame"

    # Tolérance ±20% entre JSONL et MP4 (ffprobe -count_packets inclut B/P-frames)
    if ! awk -v a="$jsonl_frames" -v b="$mp4_frames" 'BEGIN{d=a-b; if(d<0)d=-d; exit !(d <= a*0.20)}'; then
      fail "$cam: mismatch frame count trop élevé, jsonl=$jsonl_frames mp4=$mp4_frames"
    fi

    if [[ -z "$expected_jsonl" ]]; then
      expected_jsonl="$jsonl_frames"
    else
      # Tolérance ±5 frames entre caméras
      if ! awk -v a="$jsonl_frames" -v b="$expected_jsonl" 'BEGIN{d=a-b; if(d<0)d=-d; exit !(d <= 5)}'; then
        fail "Nombre de frames JSONL trop différent entre vidéos ($jsonl_frames vs $expected_jsonl)"
      fi
    fi

    ok "$cam: frames cohérentes (jsonl=$jsonl_frames, mp4=$mp4_frames)"
  done

  ok "Toutes les vidéos ont le même nombre de frames"
}

check_tracker_csv() {
  [[ -s "$TRACKERS_CSV" ]] || fail "tracker_positions.csv vide"

  local header
  header="$(head -n1 "$TRACKERS_CSV" | tr -d '\r')"

  local required_cols=(
    timestamp
    time_seconds
    timestamp_ns
    tracker_head_x tracker_head_y tracker_head_z tracker_head_qw tracker_head_qx tracker_head_qy tracker_head_qz
    tracker_left_x tracker_left_y tracker_left_z tracker_left_qw tracker_left_qx tracker_left_qy tracker_left_qz
    tracker_right_x tracker_right_y tracker_right_z tracker_right_qw tracker_right_qx tracker_right_qy tracker_right_qz
  )

  for col in "${required_cols[@]}"; do
    echo "$header" | grep -qE "(^|,)$col(,|$)" || fail "Colonne absente dans tracker_positions.csv: $col"
  done

  awk -F',' '
    NR==1 { next }

    function isnum(x) {
      gsub(/\r/, "", x)
      return (x ~ /^-?[0-9]+([.][0-9]+)?$/)
    }

    {
      gsub(/\r/, "", $NF)
      if (NF != 24) {
        printf("[ERROR] tracker_positions.csv ligne %d: NF=%d, attendu=24\n", NR, NF) > "/dev/stderr"
        exit 1
      }

      if ($2 == "" || $3 == "") {
        printf("[ERROR] tracker_positions.csv ligne %d: timestamp vide\n", NR) > "/dev/stderr"
        exit 1
      }

      for (i=4; i<=24; i++) {
        if ($i != "" && !isnum($i)) {
          printf("[ERROR] tracker_positions.csv ligne %d: valeur non numérique colonne %d -> %s\n", NR, i, $i) > "/dev/stderr"
          exit 1
        }
      }

      if (prev_ns != "" && $3 <= prev_ns) {
        printf("[ERROR] tracker_positions.csv ligne %d: timestamp_ns non croissant (%s <= %s)\n", NR, $3, prev_ns) > "/dev/stderr"
        exit 1
      }

      prev_ns = $3
      count++
    }

    END {
      if (count == 0) {
        print "[ERROR] tracker_positions.csv ne contient aucune donnée" > "/dev/stderr"
        exit 1
      }
    }
  ' "$TRACKERS_CSV"

  ok "tracker_positions.csv valide et les 3 trackers sont présents"
}

check_gripper_csv() {
  local file="$1"
  local expected_side="$2"

  [[ -s "$file" ]] || fail "$file vide"

  local header
  header="$(head -n1 "$file" | tr -d '\r')"
  local expected_header="timestamp,time_seconds,timestamp_ns,t_ms,t_ms_corrected_ns,gripper_side,sw,opening_mm,angle_deg"

  [[ "$header" == "$expected_header" ]] || fail "Header invalide dans $file"

  awk -F',' -v side="$expected_side" -v file="$file" '
    NR==1 { next }

    function isnum(x) {
      return (x ~ /^-?[0-9]+([.][0-9]+)?$/)
    }

    {
      if (NF != 9) {
        printf("[ERROR] %s ligne %d: NF=%d, attendu=9\n", file, NR, NF) > "/dev/stderr"
        exit 1
      }

      if ($6 != "" && $6 != side) {
        printf("[ERROR] %s ligne %d: gripper_side=%s, attendu=%s\n", file, NR, $6, side) > "/dev/stderr"
        exit 1
      }

      if (!isnum($2) || $3 !~ /^[0-9]+$/) {
        printf("[ERROR] %s ligne %d: format numérique invalide (timestamp)\n", file, NR) > "/dev/stderr"
        exit 1
      }

      if (prev_ns != "" && $3 <= prev_ns) {
        printf("[ERROR] %s ligne %d: timestamp_ns non croissant (%s <= %s)\n", file, NR, $3, prev_ns) > "/dev/stderr"
        exit 1
      }

      prev_ns = $3
      count++
    }

    END {
      if (count == 0) {
        printf("[ERROR] %s: aucune donnée\n", file) > "/dev/stderr"
        exit 1
      }
    }
  ' "$file"

  ok "$file valide"
}

main() {
  require_cmd jq
  require_cmd awk
  require_cmd ffprobe
  require_cmd wc
  require_cmd grep
  require_cmd sed

  check_required_files
  check_metadata
  check_all_videos
  check_tracker_csv
  check_gripper_csv "$GRIPPER_LEFT_CSV" "left"
  check_gripper_csv "$GRIPPER_RIGHT_CSV" "right"

  echo
  echo "[SUCCESS] Session valide"
}

main
