#!/usr/bin/env bash
# Usage: ./quality_view.sh [SPOOL_DIR]
# Affiche un résumé qualité de toutes les sessions dans le terminal.

SPOOL_DIR="${1:-$(dirname "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)")}"

if [[ -t 1 ]]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'
  CYAN='\033[0;36m'; GRAY='\033[0;90m'; RESET='\033[0m'; BOLD='\033[1m'
  BG_RED='\033[41m'; BG_GREEN='\033[42m'; BG_YELLOW='\033[43m'; BG_GRAY='\033[100m'
else
  GREEN=''; RED=''; YELLOW=''; CYAN=''; GRAY=''; RESET=''; BOLD=''
  BG_RED=''; BG_GREEN=''; BG_YELLOW=''; BG_GRAY=''
fi

# ── Helpers ──────────────────────────────────────────────────────────────────

label_color() {
  case "$1" in
    perfect)    echo -e "${BG_GREEN}${BOLD} PERFECT    ${RESET}" ;;
    good)       echo -e "${GREEN}${BOLD} GOOD       ${RESET}" ;;
    acceptable) echo -e "${YELLOW}${BOLD} ACCEPTABLE ${RESET}" ;;
    bad)        echo -e "${RED}${BOLD} BAD        ${RESET}" ;;
    *)          echo -e "${GRAY}${BOLD} N/A        ${RESET}" ;;
  esac
}

score_bar() {
  local score="$1"   # 0..100
  local width=20
  local filled=$(python3 -c "print(round($score / 100 * $width))" 2>/dev/null || echo 0)
  local empty=$(( width - filled ))
  local bar=""
  local i
  for (( i=0; i<filled; i++ )); do bar+="█"; done
  for (( i=0; i<empty;  i++ )); do bar+="░"; done

  local color
  if   (( $(echo "$score >= 90" | bc -l) )); then color="$GREEN"
  elif (( $(echo "$score >= 75" | bc -l) )); then color="$GREEN"
  elif (( $(echo "$score >= 60" | bc -l) )); then color="$YELLOW"
  else color="$RED"; fi

  echo -e "${color}${bar}${RESET} ${BOLD}${score}${RESET}/100"
}

check_icon() {
  # $1 = true/false/null
  case "$1" in
    True|true)   echo -e "${GREEN}✔${RESET}" ;;
    False|false) echo -e "${RED}✘${RESET}" ;;
    *)           echo -e "${GRAY}–${RESET}" ;;
  esac
}

# ── Rendu d'une session ───────────────────────────────────────────────────────

render_session() {
  local meta="$1"
  [[ -f "$meta" ]] || return

  python3 - "$meta" <<'PYEOF'
import json, sys, os

meta_path = sys.argv[1]
session_name = os.path.basename(os.path.dirname(meta_path))

try:
    with open(meta_path) as f:
        d = json.load(f)
except Exception as e:
    print(f"  ERREUR lecture metadata: {e}")
    sys.exit(0)

R   = report = d.get("quality_report")
score  = d.get("quality_score", 0) or 0
label  = d.get("quality_label", "n/a")
at     = d.get("quality_checked_at", "jamais")

# Couleurs ANSI
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
GRAY   = "\033[0;90m"

def label_fmt(l):
    colors = {"perfect": GREEN+BOLD, "good": GREEN, "acceptable": YELLOW, "bad": RED}
    c = colors.get(l, GRAY)
    return f"{c}{l.upper():12}{RESET}"

def bool_icon(v):
    if v is True:  return f"{GREEN}✔{RESET}"
    if v is False: return f"{RED}✘{RESET}"
    return f"{GRAY}–{RESET}"

def score_bar(s, width=20):
    s = float(s or 0)
    filled = round(s / 100 * width)
    bar = "█" * filled + "░" * (width - filled)
    c = GREEN if s >= 75 else YELLOW if s >= 60 else RED
    return f"{c}{bar}{RESET} {BOLD}{s:.1f}{RESET}/100"

# ── Header session ────────────────────────────────────────────────────────────
print(f"\n{'─'*62}")
print(f"  {BOLD}{session_name}{RESET}   {label_fmt(label)}   {score_bar(score)}")
print(f"  {GRAY}vérifié le {at}{RESET}")

if not R:
    print(f"  {GRAY}(pas encore de rapport — lancer run_all_checks.sh){RESET}")
    sys.exit(0)

# ── Fixes appliqués ───────────────────────────────────────────────────────────
fixes = R.get("fixes_applied", [])
if fixes:
    print(f"\n  {YELLOW}Corrections auto :{RESET}")
    for f in fixes:
        print(f"    {YELLOW}⚙{RESET}  {f}")

# ── Checks ────────────────────────────────────────────────────────────────────
checks = R.get("checks", {})
print(f"\n  {BOLD}Checks{RESET}")

# files
fc = checks.get("files", {})
icon = bool_icon(fc.get("passed"))
errs = fc.get("errors", [])
print(f"    {icon} files    ", end="")
if errs:
    print(f"{RED}{errs[0]}{RESET}")
else:
    print(f"{GRAY}{len(fc.get('checks_passed',[]))} fichiers OK{RESET}")

# sanity
sc = checks.get("sanity", {})
icon = bool_icon(sc.get("passed"))
s_errs  = sc.get("errors", [])
s_passe = sc.get("checks_passed", [])
print(f"    {icon} sanity   ", end="")
if s_errs:
    print()
    for e in s_errs:
        print(f"        {RED}✘ {e}{RESET}")
else:
    print(f"{GRAY}{len(s_passe)} vérifications OK{RESET}")

# quality
qc = checks.get("quality", {})
q_score = qc.get("score_avg", 0)
cameras = qc.get("cameras", {})
icon = bool_icon(bool(cameras))
print(f"    {icon} qualité  {score_bar(q_score)}")
for cam, info in cameras.items():
    cs = info.get("score", 0)
    bar = score_bar(cs, width=12)
    sh  = info.get("sharpness", 0)
    ex  = info.get("exposure", 0)
    cl  = info.get("clipping", 0)
    mo  = info.get("motion", 0)
    print(f"        {CYAN}{cam:5}{RESET}  {bar}   "
          f"{GRAY}net:{sh:.0f} exp:{ex:.0f} clip:{cl:.0f} mvt:{mo:.0f}{RESET}")

# naming
nc = checks.get("naming", {})
naming_ok = nc.get("passed", False)
icon = bool_icon(naming_ok)
scores_n = nc.get("scores", {})
print(f"    {icon} nommage  ", end="")
if scores_n:
    parts = [f"{CYAN}{k.split('_')[0]}{RESET}={v:.2f}" for k, v in scores_n.items()]
    print("  ".join(parts))
else:
    print(f"{GRAY}non évalué{RESET}")

PYEOF
}

# ── Résumé global ─────────────────────────────────────────────────────────────

render_summary() {
  python3 - "$SPOOL_DIR" <<'PYEOF'
import json, os, sys, glob

spool = sys.argv[1]
metas = sorted(glob.glob(os.path.join(spool, "session_*", "metadata.json")))

RESET  = "\033[0m"; BOLD = "\033[1m"
GREEN  = "\033[0;32m"; RED = "\033[0;31m"
YELLOW = "\033[1;33m"; GRAY = "\033[0;90m"

counts = {"perfect": 0, "good": 0, "acceptable": 0, "bad": 0, "n/a": 0}
scores = []

for m in metas:
    try:
        d = json.load(open(m))
        label = d.get("quality_label", "n/a") or "n/a"
        s = d.get("quality_score")
        counts[label if label in counts else "n/a"] += 1
        if s is not None:
            scores.append(float(s))
    except Exception:
        counts["n/a"] += 1

total = len(metas)
avg   = sum(scores) / len(scores) if scores else 0

print(f"\n{'═'*62}")
print(f"  {BOLD}RÉSUMÉ  {GRAY}({total} sessions — {spool}){RESET}")
print(f"{'─'*62}")
print(f"  Score moyen   : {BOLD}{avg:.1f}/100{RESET}")
print(f"  {GREEN}Perfect     {RESET}: {counts['perfect']}")
print(f"  {GREEN}Good        {RESET}: {counts['good']}")
print(f"  {YELLOW}Acceptable  {RESET}: {counts['acceptable']}")
print(f"  {RED}Bad         {RESET}: {counts['bad']}")
print(f"  {GRAY}Non évalué  {RESET}: {counts['n/a']}")
print(f"{'═'*62}")
PYEOF
}

# ── Main ──────────────────────────────────────────────────────────────────────

sessions=()
while IFS= read -r s; do sessions+=("$s"); done \
  < <(find "$SPOOL_DIR" -maxdepth 1 -type d -name 'session_*' | sort)

if [[ ${#sessions[@]} -eq 0 ]]; then
  echo "Aucune session trouvée dans $SPOOL_DIR"
  exit 0
fi

for session_dir in "${sessions[@]}"; do
  render_session "$session_dir/metadata.json"
done

render_summary
