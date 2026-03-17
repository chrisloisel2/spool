#!/usr/bin/env bash

# Usage: ./run_all_checks.sh [SPOOL_DIR]
# Pour chaque session_* dans SPOOL_DIR, lance les 4 checks et écrit
# le score /100 dans metadata.json sous la clé "quality_score".

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPOOL_DIR="${1:-$(dirname "$SCRIPT_DIR")}"

if [[ -t 1 ]]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; RESET='\033[0m'; BOLD='\033[1m'
else
  GREEN=''; RED=''; YELLOW=''; RESET=''; BOLD=''
fi

# Extrait "NOTE FINALE: XX.X / 100" depuis la sortie de quality.sh
extract_quality_score() {
  local session_dir="$1"
  local output
  output="$("$SCRIPT_DIR/quality.sh" "$session_dir" 2>/dev/null)" || { echo "0"; return; }
  echo "$output" | awk -F': ' '/^NOTE FINALE:/{sum+=$2; n++} END{if(n>0) printf "%.1f", sum/n; else print "0"}'
}

process_session() {
  local session_dir="$1"
  local session_name
  session_name="$(basename "$session_dir")"
  local metadata="$session_dir/metadata.json"

  echo -e "\n${BOLD}$session_name${RESET}"

  [[ -f "$metadata" ]] || { echo -e "  ${RED}SKIP${RESET} metadata.json absent"; return; }

  # 1. files.sh — bloquant
  if ! "$SCRIPT_DIR/files.sh" "$session_dir" >/dev/null 2>&1; then
    echo -e "  ${RED}FAIL${RESET} files.sh"
    jq '. + {"quality_score": 0, "quality_label": "bad", "quality_checked_at": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"}' \
      "$metadata" > "$metadata.tmp" && mv "$metadata.tmp" "$metadata"
    return
  fi

  # 2. sanity.sh — bloquant
  if ! "$SCRIPT_DIR/sanity.sh" "$session_dir" >/dev/null 2>&1; then
    echo -e "  ${RED}FAIL${RESET} sanity.sh"
    jq '. + {"quality_score": 0, "quality_label": "bad", "quality_checked_at": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"}' \
      "$metadata" > "$metadata.tmp" && mv "$metadata.tmp" "$metadata"
    return
  fi

  # 3. quality.sh — score /100
  local score
  score="$(extract_quality_score "$session_dir")"

  # 4. verify_naming.sh — pénalité si échec
  local naming_ok=true
  python3 "$SCRIPT_DIR/verify_naming.sh" "$session_dir" >/dev/null 2>&1 || naming_ok=false

  # Calcul label
  local label
  if [[ "$naming_ok" == "false" ]] || awk "BEGIN{exit !($score < 60)}"; then
    label="bad"
  elif awk "BEGIN{exit !($score < 75)}"; then
    label="acceptable"
  elif awk "BEGIN{exit !($score < 90)}"; then
    label="good"
  else
    label="perfect"
  fi

  # Écriture dans metadata.json
  jq --argjson score "$score" \
     --arg label "$label" \
     --arg checked_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
     '. + {quality_score: $score, quality_label: $label, quality_checked_at: $checked_at}' \
     "$metadata" > "$metadata.tmp" && mv "$metadata.tmp" "$metadata"

  echo -e "  score: ${BOLD}$score / 100${RESET}  label: ${BOLD}$label${RESET}"
}

# Découverte des sessions
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
