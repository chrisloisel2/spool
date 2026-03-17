#!/usr/bin/env bash

# Usage: ./triage.sh [SPOOL_DIR] [--send DIR] [--problem DIR] [--dry-run]
#
# Pour chaque session_* dans SPOOL_DIR :
#   - lance fix_shit + files + sanity
#   - si tout passe → déplace vers SEND_DIR   (défaut: /send)
#   - si ça échoue  → déplace vers PROBLEM_DIR (défaut: /problem)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPOOL_DIR=""
SEND_DIR="/send"
PROBLEM_DIR="/problem"
DRY_RUN=false

# ── Parse args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --send)     SEND_DIR="$2";    shift 2 ;;
    --problem)  PROBLEM_DIR="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=true;     shift   ;;
    *)          SPOOL_DIR="$1";   shift   ;;
  esac
done

[[ -z "$SPOOL_DIR" ]] && SPOOL_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -t 1 ]]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'
  CYAN='\033[0;36m'; GRAY='\033[0;90m'; RESET='\033[0m'; BOLD='\033[1m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; GRAY=''; RESET=''; BOLD=''
fi

count_ok=0
count_problem=0
count_skip=0

log_move() { echo -e "  ${GREEN}→ SEND${RESET}     $1"; }
log_problem() { echo -e "  ${RED}→ PROBLEM${RESET}  $1  ${GRAY}($2)${RESET}"; }
log_dry() { echo -e "  ${YELLOW}[DRY]${RESET} $*"; }

# ── Déplacement sécurisé ─────────────────────────────────────────────────────
move_session() {
  local src="$1"
  local dest_dir="$2"
  local session_name
  session_name="$(basename "$src")"
  local dest="$dest_dir/$session_name"

  if $DRY_RUN; then
    log_dry "mv $src → $dest_dir/"
    return 0
  fi

  mkdir -p "$dest_dir"

  # Si la destination existe déjà, suffixer avec _dup_N
  if [[ -d "$dest" ]]; then
    local n=1
    while [[ -d "${dest}_dup_${n}" ]]; do (( n++ )); done
    dest="${dest}_dup_${n}"
  fi

  mv "$src" "$dest"
}

# ── Traitement d'une session ──────────────────────────────────────────────────
process_session() {
  local session_dir="$1"
  local session_name
  session_name="$(basename "$session_dir")"

  echo -e "\n${BOLD}$session_name${RESET}"

  # 0. fix_shit
  "$SCRIPT_DIR/fix_shit.sh" "$session_dir" >/dev/null 2>&1 || true

  local metadata="$session_dir/metadata.json"
  if [[ ! -f "$metadata" ]]; then
    log_problem "$session_name" "metadata.json absent après fix"
    move_session "$session_dir" "$PROBLEM_DIR"
    (( count_problem++ )) || true
    return
  fi

  # 1. files.sh
  local files_err
  files_err="$("$SCRIPT_DIR/files.sh" "$session_dir" 2>&1 >/dev/null)" || true
  if ! "$SCRIPT_DIR/files.sh" "$session_dir" >/dev/null 2>&1; then
    local reason
    reason="$(echo "$files_err" | grep '\[ERROR\]' | head -1 | sed 's/\[ERROR\] //')"
    [[ -z "$reason" ]] && reason="files.sh failed"
    log_problem "$session_name" "$reason"
    move_session "$session_dir" "$PROBLEM_DIR"
    (( count_problem++ )) || true
    return
  fi

  # 2. sanity.sh
  local sanity_out sanity_err
  sanity_out="$("$SCRIPT_DIR/sanity.sh" "$session_dir" 2>&1)"
  if ! echo "$sanity_out" | grep -q "\[SUCCESS\]"; then
    local reason
    reason="$(echo "$sanity_out" | grep '\[ERROR\]' | head -1 | sed 's/\[ERROR\] //')"
    [[ -z "$reason" ]] && reason="sanity.sh failed"
    log_problem "$session_name" "$reason"
    move_session "$session_dir" "$PROBLEM_DIR"
    (( count_problem++ )) || true
    return
  fi

  # ✔ OK → SEND
  log_move "$session_name"
  move_session "$session_dir" "$SEND_DIR"
  (( count_ok++ )) || true
}

# ── Main ──────────────────────────────────────────────────────────────────────
sessions=()
while IFS= read -r s; do sessions+=("$s"); done \
  < <(find "$SPOOL_DIR" -maxdepth 1 -type d -name 'session_*' | sort)

if [[ ${#sessions[@]} -eq 0 ]]; then
  echo "Aucune session trouvée dans $SPOOL_DIR"
  exit 0
fi

$DRY_RUN && echo -e "${YELLOW}[DRY-RUN] Aucun fichier ne sera déplacé${RESET}"
echo "Sessions: ${#sessions[@]}  —  Spool: $SPOOL_DIR"
echo "Send    : $SEND_DIR"
echo "Problem : $PROBLEM_DIR"

for session_dir in "${sessions[@]}"; do
  process_session "$session_dir"
done

echo -e "\n${BOLD}Résumé${RESET}"
echo -e "  ${GREEN}✔ Send   ${RESET}: $count_ok"
echo -e "  ${RED}✘ Problem${RESET}: $count_problem"
$DRY_RUN && echo -e "\n${YELLOW}(dry-run : rien n'a été déplacé)${RESET}"
