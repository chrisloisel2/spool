#!/usr/bin/env bash

# Usage: ./fix_shit.sh [SPOOL_DIR]
# Répare les petits problèmes réparables des sessions :
#   1) metadata.json absent → recomposé depuis les fichiers disponibles
#   2) JSONL tronqués (dernière ligne incomplète) → crop

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPOOL_DIR="${1:-$(dirname "$SCRIPT_DIR")}"

if [[ -t 1 ]]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RESET='\033[0m'; BOLD='\033[1m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; RESET=''; BOLD=''
fi

fixed_total=0

log_ok()   { echo -e "  ${GREEN}[FIX]${RESET}  $*"; }
log_warn() { echo -e "  ${YELLOW}[WARN]${RESET} $*"; }
log_info() { echo -e "  ${CYAN}[INFO]${RESET} $*"; }
log_err()  { echo -e "  ${RED}[ERR]${RESET}  $*" >&2; }

# ─────────────────────────────────────────────
# 1. FIX JSONL : crop la dernière ligne si elle est invalide (JSON incomplet)
# ─────────────────────────────────────────────
fix_jsonl() {
  local file="$1"
  local cam="$2"

  [[ -f "$file" ]] || { log_warn "$cam.jsonl absent, skip"; return; }

  local last_line
  last_line="$(tail -n1 "$file")"

  # Tester si la dernière ligne est du JSON valide
  if echo "$last_line" | python3 -c "import sys,json; json.loads(sys.stdin.read())" 2>/dev/null; then
    log_info "$cam.jsonl OK (dernière ligne valide)"
    return
  fi

  local total_lines
  total_lines="$(wc -l < "$file" | tr -d ' ')"

  if [[ "$total_lines" -le 1 ]]; then
    log_err "$cam.jsonl n'a qu'une ligne et elle est invalide — impossible de corriger"
    return 1
  fi

  cp "$file" "${file}.bak"

  if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' '$d' "$file"
  else
    sed -i '$d' "$file"
  fi

  local new_lines
  new_lines="$(wc -l < "$file" | tr -d ' ')"
  log_ok "$cam.jsonl : ligne tronquée supprimée ($total_lines → $new_lines lignes) [backup: $(basename "${file}.bak")]"
  (( fixed_total++ )) || true
}

# ─────────────────────────────────────────────
# 2. RECONSTRUCT metadata.json depuis zéro
# ─────────────────────────────────────────────
rebuild_metadata() {
  local session_dir="$1"
  local session_name
  session_name="$(basename "$session_dir")"
  local metadata="$session_dir/metadata.json"
  local videos_dir="$session_dir/videos"

  log_warn "metadata.json absent — tentative de reconstruction"

  # session_id depuis le nom du dossier (session_YYYYMMDD_HHMMSS)
  local session_id="${session_name#session_}"

  # start_time depuis le nom
  local year month day hour min sec start_time
  if [[ "$session_id" =~ ^([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2})$ ]]; then
    year="${BASH_REMATCH[1]}"; month="${BASH_REMATCH[2]}"; day="${BASH_REMATCH[3]}"
    hour="${BASH_REMATCH[4]}"; min="${BASH_REMATCH[5]}";   sec="${BASH_REMATCH[6]}"
    start_time="${year}-${month}-${day}T${hour}:${min}:${sec}.000000+00:00"
  else
    log_err "Impossible de parser la date depuis '$session_name'"
    return 1
  fi

  # width, height, fps, duration depuis le premier mp4 disponible
  local width=0 height=0 fps=30 duration=0
  local ref_mp4=""
  for cam in head left right; do
    [[ -f "$videos_dir/${cam}.mp4" ]] && { ref_mp4="$videos_dir/${cam}.mp4"; break; }
  done

  if [[ -n "$ref_mp4" ]] && command -v ffprobe >/dev/null 2>&1; then
    width="$(ffprobe -v error -select_streams v:0 -show_entries stream=width \
      -of csv=p=0 "$ref_mp4" 2>/dev/null | head -n1 | tr -d '\r')"
    height="$(ffprobe -v error -select_streams v:0 -show_entries stream=height \
      -of csv=p=0 "$ref_mp4" 2>/dev/null | head -n1 | tr -d '\r')"
    local fps_raw
    fps_raw="$(ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate \
      -of csv=p=0 "$ref_mp4" 2>/dev/null | head -n1 | tr -d '\r')"
    fps="$(awk -F'/' '{if($2>0) printf "%d", $1/$2; else print $1}' <<< "$fps_raw")"
    duration="$(ffprobe -v error -show_entries format=duration \
      -of default=nw=1:nk=1 "$ref_mp4" 2>/dev/null | head -n1 | tr -d '\r')"
    [[ "$width"    =~ ^[0-9]+$ ]] || width=0
    [[ "$height"   =~ ^[0-9]+$ ]] || height=0
    [[ "$fps"      =~ ^[0-9]+$ ]] || fps=30
    [[ "$duration" =~ ^[0-9] ]]   || duration=0
  else
    log_warn "Aucun .mp4 ou ffprobe absent — dimensions/durée par défaut"
  fi

  # mono_start de chaque caméra depuis le premier capture_time de son JSONL
  declare -A anchors
  for cam in head left right; do
    anchors[$cam]="null"
    local jfile="$videos_dir/${cam}.jsonl"
    if [[ -s "$jfile" ]]; then
      local ct
      ct="$(head -n1 "$jfile" | python3 -c \
        "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('capture_time','null'))" 2>/dev/null)"
      if [[ "$ct" =~ ^[0-9]+$ ]]; then
        anchors[$cam]="$(python3 -c "print(${ct} / 1e9)")"
      fi
    fi
  done

  # start_time_ns depuis le premier capture_time disponible
  local start_time_ns="null"
  for cam in head left right; do
    local jfile="$videos_dir/${cam}.jsonl"
    if [[ -s "$jfile" ]]; then
      local ct
      ct="$(head -n1 "$jfile" | python3 -c \
        "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('capture_time','null'))" 2>/dev/null)"
      [[ "$ct" =~ ^[0-9]+$ ]] && { start_time_ns="$ct"; break; }
    fi
  done

  # Écriture via python3 pour avoir un JSON propre
  python3 - "$metadata" \
    "$session_id" "$start_time" "$start_time_ns" \
    "$width" "$height" "$fps" "$duration" \
    "${anchors[head]}" "${anchors[left]}" "${anchors[right]}" \
  <<'PYEOF'
import json, sys
from datetime import datetime, timezone, timedelta

metadata_path, session_id, start_time, start_time_ns, \
width, height, fps, duration, \
anchor_head, anchor_left, anchor_right = sys.argv[1:12]

def maybe_float(v):
    try: return float(v)
    except: return None

def maybe_int(v):
    try: return int(v)
    except: return None

dur = maybe_float(duration) or 0.0
try:
    start_dt = datetime.fromisoformat(start_time)
    end_dt   = start_dt + timedelta(seconds=dur)
    end_time = end_dt.isoformat()
except Exception:
    end_time = start_time

data = {
    "session_id":    session_id,
    "scenario":      "unknown",
    "start_time":    start_time,
    "start_time_ns": maybe_int(start_time_ns),
    "video_config": {
        "width":  maybe_int(width)  or 0,
        "height": maybe_int(height) or 0,
        "fps":    maybe_int(fps)    or 30,
    },
    "cameras": {
        "0": {"name": "USB3.0 Camera", "position": "head",  "serial": "unknown"},
        "1": {"name": "USB3.0 Camera", "position": "left",  "serial": "unknown"},
        "2": {"name": "USB3.0 Camera", "position": "right", "serial": "unknown"},
    },
    "camera_anchors": {
        "head":  {"mono_start": maybe_float(anchor_head),  "mono_offset_from_record": 0.0},
        "left":  {"mono_start": maybe_float(anchor_left),  "mono_offset_from_record": 0.0},
        "right": {"mono_start": maybe_float(anchor_right), "mono_offset_from_record": 0.0},
    },
    "trackers": {
        "1": {"serial": "unknown", "model": "VIVE Ultimate Tracker"},
        "2": {"serial": "unknown", "model": "VIVE Ultimate Tracker"},
        "3": {"serial": "unknown", "model": "VIVE Ultimate Tracker"},
    },
    "grippers": {
        "right": {"port": "unknown", "baud": "115200"},
        "left":  {"port": "unknown", "baud": "115200"},
    },
    "precise_timer":    True,
    "end_time":         end_time,
    "duration_seconds": dur,
    "failed":           False,
    "_reconstructed":   True,
}

with open(metadata_path, "w") as f:
    json.dump(data, f, indent=2)

print("OK")
PYEOF

  if [[ $? -eq 0 ]]; then
    log_ok "metadata.json reconstruit (champs inconnus marqués 'unknown', _reconstructed=true)"
    (( fixed_total++ )) || true
  else
    log_err "Échec de la reconstruction du metadata.json"
    return 1
  fi
}

# ─────────────────────────────────────────────
# 3. FIX GRIPPER CSV : pince1/pince2 → gripper_right/gripper_left
#    Convention : pince1 = right, pince2 = left
# ─────────────────────────────────────────────
fix_gripper_names() {
  local session_dir="$1"

  local pince1="$session_dir/pince1_data.csv"
  local pince2="$session_dir/pince2_data.csv"
  local dest_right="$session_dir/gripper_right_data.csv"
  local dest_left="$session_dir/gripper_left_data.csv"

  if [[ -f "$pince1" && ! -f "$dest_right" ]]; then
    cp "$pince1" "${pince1}.bak"
    mv "$pince1" "$dest_right"
    log_ok "pince1_data.csv → gripper_right_data.csv"
    (( fixed_total++ )) || true
  elif [[ -f "$pince1" && -f "$dest_right" ]]; then
    log_warn "pince1_data.csv existe mais gripper_right_data.csv aussi — skip renommage"
  fi

  if [[ -f "$pince2" && ! -f "$dest_left" ]]; then
    cp "$pince2" "${pince2}.bak"
    mv "$pince2" "$dest_left"
    log_ok "pince2_data.csv → gripper_left_data.csv"
    (( fixed_total++ )) || true
  elif [[ -f "$pince2" && -f "$dest_left" ]]; then
    log_warn "pince2_data.csv existe mais gripper_left_data.csv aussi — skip renommage"
  fi
}

# ─────────────────────────────────────────────
# 4. FIX TRACKER CSV : renommer tracker_1/2/3 → tracker_head/left/right
#    Convention : tracker_1 = head, tracker_2 = left, tracker_3 = right
# ─────────────────────────────────────────────
fix_tracker_header() {
  local session_dir="$1"
  local file="$session_dir/tracker_positions.csv"

  [[ -f "$file" ]] || { log_warn "tracker_positions.csv absent"; return; }

  local header
  header="$(head -n1 "$file" | tr -d '\r')"

  # Détecter si le header utilise tracker_1/2/3 au lieu de tracker_head/left/right
  if ! echo "$header" | grep -q "tracker_head_x"; then
    if echo "$header" | grep -q "tracker_1_x"; then
      cp "$file" "${file}.bak"
      if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '' '1s/tracker_1_/tracker_head_/g;
                   1s/tracker_2_/tracker_left_/g;
                   1s/tracker_3_/tracker_right_/g' "$file"
      else
        sed -i '1s/tracker_1_/tracker_head_/g;
                1s/tracker_2_/tracker_left_/g;
                1s/tracker_3_/tracker_right_/g' "$file"
      fi
      log_ok "tracker_positions.csv : header tracker_1/2/3 → tracker_head/left/right"
      (( fixed_total++ )) || true
    else
      log_warn "tracker_positions.csv : header non reconnu — $(echo "$header" | cut -c1-80)"
    fi
  else
    log_info "tracker_positions.csv header OK"
  fi
}

# ─────────────────────────────────────────────
# 5. FIX metadata.json : remet failed=false si la session est marquée failed=true
#    Les sessions enregistrées pendant un crash sont souvent marquées failed=true
#    mais leurs données sont exploitables.
# ─────────────────────────────────────────────
fix_failed_flag() {
  local session_dir="$1"
  local metadata="$session_dir/metadata.json"

  [[ -f "$metadata" ]] || return

  local failed_val
  failed_val="$(python3 -c "import json,sys; d=json.load(open('$metadata')); print(d.get('failed',''))" 2>/dev/null)"

  if [[ "$failed_val" == "True" || "$failed_val" == "true" ]]; then
    cp "$metadata" "${metadata}.bak"
    python3 -c "
import json, sys
with open('$metadata') as f:
    d = json.load(f)
d['failed'] = False
with open('$metadata', 'w') as f:
    json.dump(d, f, indent=2)
" 2>/dev/null && {
      log_ok "metadata.json : failed=true → false (session potentiellement récupérable)"
      (( fixed_total++ )) || true
    } || log_err "Impossible de corriger failed dans metadata.json"
  fi
}

# ─────────────────────────────────────────────
# Traitement d'une session
# ─────────────────────────────────────────────
process_session() {
  local session_dir="$1"
  echo -e "\n${BOLD}$(basename "$session_dir")${RESET}"

  # Fix 1 : metadata manquant
  if [[ ! -f "$session_dir/metadata.json" ]]; then
    rebuild_metadata "$session_dir" || true
  else
    log_info "metadata.json présent"
  fi

  # Fix 5 : failed=true dans metadata.json
  fix_failed_flag "$session_dir" || true

  # Fix 2 : JSONL tronqués
  local videos_dir="$session_dir/videos"
  if [[ -d "$videos_dir" ]]; then
    for cam in head left right; do
      fix_jsonl "$videos_dir/${cam}.jsonl" "$cam" || true
    done
  else
    log_warn "Répertoire videos/ absent — JSONL non vérifiés"
  fi

  # Fix 3 : renommage pince1/2 → gripper_right/left
  fix_gripper_names "$session_dir" || true

  # Fix 4 : header tracker_positions.csv tracker_1/2/3 → head/left/right
  fix_tracker_header "$session_dir" || true
}

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

# Détecte si l'argument est une session directe ou un spool dir
sessions=()
if [[ "$(basename "$SPOOL_DIR")" == session_* && -d "$SPOOL_DIR" ]]; then
  sessions+=("$SPOOL_DIR")
else
  while IFS= read -r s; do sessions+=("$s"); done \
    < <(find "$SPOOL_DIR" -maxdepth 1 -type d -name 'session_*' | sort)
fi

if [[ ${#sessions[@]} -eq 0 ]]; then
  echo "Aucune session trouvée dans $SPOOL_DIR"
  exit 0
fi

echo "Sessions: ${#sessions[@]}  —  Spool: $SPOOL_DIR"

for session_dir in "${sessions[@]}"; do
  process_session "$session_dir"
done

echo -e "\n${GREEN}Terminé. $fixed_total correction(s) appliquée(s).${RESET}"
