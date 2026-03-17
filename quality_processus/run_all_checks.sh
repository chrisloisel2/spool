#!/usr/bin/env bash

# Usage: ./run_all_checks.sh [SPOOL_DIR]
# Pour chaque session_* dans SPOOL_DIR :
#   - lance fix_shit + files + sanity + quality + verify_naming
#   - écrit un rapport complet dans metadata.json sous la clé "quality_report"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPOOL_DIR="${1:-$(dirname "$SCRIPT_DIR")}"

if [[ -t 1 ]]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; RESET='\033[0m'; BOLD='\033[1m'
else
  GREEN=''; RED=''; YELLOW=''; RESET=''; BOLD=''
fi

# ── Extrait les scores par caméra depuis quality.sh ──────────────────────────
extract_quality_details() {
  local session_dir="$1"
  local output
  output="$("$SCRIPT_DIR/quality.sh" "$session_dir" 2>/dev/null)" || { echo "{}"; return; }

  echo "$output" | awk '
    /^VIDEO:/ { cam = $2 }
    /^NOTE FINALE:/ {
      split($0, a, ": "); split(a[2], b, " "); scores[cam] = b[1]
    }
    /nettete/ {
      split($0, a, ": "); split(a[2], b, " "); sharp[cam] = b[1]
    }
    /exposition/ {
      split($0, a, ": "); split(a[2], b, " "); expo[cam] = b[1]
    }
    /clipping/ {
      split($0, a, ": "); split(a[2], b, " "); clip[cam] = b[1]
    }
    /dynamique/ {
      split($0, a, ": "); split(a[2], b, " "); motion[cam] = b[1]
    }
    END {
      n = 0; sum = 0
      for (c in scores) { sum += scores[c]; n++ }
      avg = (n > 0) ? sum/n : 0

      printf "{\"score_avg\":%.1f", avg
      printf ",\"cameras\":{"
      sep = ""
      for (c in scores) {
        printf "%s\"%s\":{\"score\":%.1f,\"sharpness\":%.1f,\"exposure\":%.1f,\"clipping\":%.1f,\"motion\":%.1f}", \
          sep, c, scores[c]+0, sharp[c]+0, expo[c]+0, clip[c]+0, motion[c]+0
        sep = ","
      }
      printf "}}"
    }
  '
}

# ── Collecte les erreurs d'un script avec sortie lisible ─────────────────────
run_check_capture() {
  local script="$1"
  local session_dir="$2"
  local output exit_code=0

  output="$("$script" "$session_dir" 2>&1)" || exit_code=$?

  local errors=()
  local oks=()
  while IFS= read -r line; do
    [[ "$line" =~ \[ERROR\] ]] && errors+=("${line#*\[ERROR\] }")
    [[ "$line" =~ \[OK\] ]]    && oks+=("${line#*\[OK\] }")
  done <<< "$output"

  # Sérialise en JSON inline
  local errors_json oks_json
  errors_json="$(printf '%s\n' "${errors[@]+"${errors[@]}"}" | python3 -c \
    'import sys,json; lines=[l.rstrip() for l in sys.stdin if l.strip()]; print(json.dumps(lines))')"
  oks_json="$(printf '%s\n' "${oks[@]+"${oks[@]}"}" | python3 -c \
    'import sys,json; lines=[l.rstrip() for l in sys.stdin if l.strip()]; print(json.dumps(lines))')"

  echo "{\"ok\":$([ $exit_code -eq 0 ] && echo true || echo false),\"errors\":${errors_json},\"checks_passed\":${oks_json}}"
  return $exit_code
}

process_session() {
  local session_dir="$1"
  local session_name
  session_name="$(basename "$session_dir")"
  local metadata="$session_dir/metadata.json"

  echo -e "\n${BOLD}$session_name${RESET}"

  local checked_at
  checked_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # ── 0. fix_shit ───────────────────────────────────────────────────────────
  local fixes_applied=()
  local fix_output
  fix_output="$("$SCRIPT_DIR/fix_shit.sh" "$session_dir" 2>&1)" || true
  while IFS= read -r line; do
    [[ "$line" =~ \[FIX\] ]] && fixes_applied+=("${line#*\[FIX\]  }")
  done <<< "$fix_output"

  metadata="$session_dir/metadata.json"
  if [[ ! -f "$metadata" ]]; then
    echo -e "  ${RED}SKIP${RESET} metadata.json absent même après fix"
    return
  fi

  # ── 1. files.sh ───────────────────────────────────────────────────────────
  local files_result
  files_result="$(run_check_capture "$SCRIPT_DIR/files.sh" "$session_dir")" || true
  local files_ok
  files_ok="$(echo "$files_result" | python3 -c 'import sys,json; print(json.load(sys.stdin)["ok"])')"

  if [[ "$files_ok" == "False" ]]; then
    echo -e "  ${RED}FAIL${RESET} files.sh"
    _write_report "$metadata" "$checked_at" "blocked_files" 0 \
      "$files_result" '{"ok":null}' '{"score_avg":0,"cameras":{}}' 'false' "$(printf '%s\n' "${fixes_applied[@]+"${fixes_applied[@]}"}" | python3 -c 'import sys,json; print(json.dumps([l.rstrip() for l in sys.stdin if l.strip()]))')"
    return
  fi

  # ── 2. sanity.sh ──────────────────────────────────────────────────────────
  local sanity_result
  sanity_result="$(run_check_capture "$SCRIPT_DIR/sanity.sh" "$session_dir")" || true
  local sanity_ok
  sanity_ok="$(echo "$sanity_result" | python3 -c 'import sys,json; print(json.load(sys.stdin)["ok"])')"

  if [[ "$sanity_ok" == "False" ]]; then
    echo -e "  ${RED}FAIL${RESET} sanity.sh"
    _write_report "$metadata" "$checked_at" "blocked_sanity" 0 \
      "$files_result" "$sanity_result" '{"score_avg":0,"cameras":{}}' 'false' "$(printf '%s\n' "${fixes_applied[@]+"${fixes_applied[@]}"}" | python3 -c 'import sys,json; print(json.dumps([l.rstrip() for l in sys.stdin if l.strip()]))')"
    return
  fi

  # ── 3. quality.sh ─────────────────────────────────────────────────────────
  local quality_details
  quality_details="$(extract_quality_details "$session_dir")"
  local score
  score="$(echo "$quality_details" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("score_avg",0))')"

  # ── 4. verify_naming.sh ───────────────────────────────────────────────────
  local naming_result naming_ok
  naming_result="$(python3 "$SCRIPT_DIR/verify_naming.sh" "$session_dir" 2>/dev/null \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); r=d.get("result",{}); ok=all(v>=0.60 for v in r.values()); print(json.dumps({"ok":ok,"scores":r}))' \
    2>/dev/null)" || naming_result='{"ok":false,"scores":{}}'
  naming_ok="$(echo "$naming_result" | python3 -c 'import sys,json; print(json.load(sys.stdin)["ok"])')"

  # ── Label final ───────────────────────────────────────────────────────────
  local label
  if [[ "$naming_ok" == "False" ]] || awk "BEGIN{exit !($score < 60)}"; then
    label="bad"
  elif awk "BEGIN{exit !($score < 75)}"; then
    label="acceptable"
  elif awk "BEGIN{exit !($score < 90)}"; then
    label="good"
  else
    label="perfect"
  fi

  local fixes_json
  fixes_json="$(printf '%s\n' "${fixes_applied[@]+"${fixes_applied[@]}"}" | python3 -c \
    'import sys,json; print(json.dumps([l.rstrip() for l in sys.stdin if l.strip()]))')"

  _write_report "$metadata" "$checked_at" "ok" "$score" \
    "$files_result" "$sanity_result" "$quality_details" "$naming_result" "$fixes_json" "$label"

  echo -e "  score: ${BOLD}$score / 100${RESET}  label: ${BOLD}$label${RESET}"
}

# ── Écrit le bloc quality_report dans metadata.json ──────────────────────────
_write_report() {
  local metadata="$1"
  local checked_at="$2"
  local status="$3"
  local score="$4"
  local files_result="$5"
  local sanity_result="$6"
  local quality_details="$7"
  local naming_result="$8"
  local fixes_json="$9"
  local label="${10:-bad}"

  python3 - "$metadata" "$checked_at" "$status" "$score" \
    "$files_result" "$sanity_result" "$quality_details" "$naming_result" "$fixes_json" "$label" \
  <<'PYEOF'
import json, sys

meta_path, checked_at, status, score, \
  files_raw, sanity_raw, quality_raw, naming_raw, fixes_raw, label = sys.argv[1:11]

def load(s):
    try: return json.loads(s)
    except: return {}

files   = load(files_raw)
sanity  = load(sanity_raw)
quality = load(quality_raw)
naming  = load(naming_raw)
fixes   = load(fixes_raw)

score_f = float(score) if score else 0.0

report = {
    "status":       status,
    "score":        score_f,
    "label":        label,
    "checked_at":   checked_at,
    "fixes_applied": fixes if isinstance(fixes, list) else [],
    "checks": {
        "files": {
            "passed": files.get("ok", False),
            "checks_passed": files.get("checks_passed", []),
            "errors": files.get("errors", []),
        },
        "sanity": {
            "passed": sanity.get("ok", False),
            "checks_passed": sanity.get("checks_passed", []),
            "errors": sanity.get("errors", []),
        },
        "quality": {
            "score_avg": quality.get("score_avg", 0),
            "cameras":   quality.get("cameras", {}),
        },
        "naming": {
            "passed": naming.get("ok", False),
            "scores": naming.get("scores", {}),
        },
    },
}

with open(meta_path) as f:
    meta = json.load(f)

meta["quality_score"]      = score_f
meta["quality_label"]      = label
meta["quality_checked_at"] = checked_at
meta["quality_report"]     = report

with open(meta_path + ".tmp", "w") as f:
    json.dump(meta, f, indent=2, ensure_ascii=False)

import os
os.replace(meta_path + ".tmp", meta_path)
PYEOF
}

# ── Découverte des sessions ───────────────────────────────────────────────────
sessions=()
while IFS= read -r s; do sessions+=("$s"); done \
  < <(find "$SPOOL_DIR" -maxdepth 1 -type d -name 'session_*' | sort)

if [[ ${#sessions[@]} -eq 0 ]]; then
  echo "Aucune session trouvée dans $SPOOL_DIR"
  exit 0
fi

echo "Sessions: ${#sessions[@]}  —  Spool: $SPOOL_DIR"

for session_dir in "${sessions[@]}"; do
  process_session "$session_dir"
done

echo -e "\n${GREEN}Terminé.${RESET}"
