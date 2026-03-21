#!/usr/bin/env bash
# find_duplicates.sh — Trouve les sessions présentes dans inbox ET sur le NAS (doublons)
#                      et les supprime localement pour libérer de l'espace.
#
# Usage :
#   ./find_duplicates.sh             # Scan seulement (dry-run)
#   ./find_duplicates.sh --delete    # Supprimer les doublons confirmés
#   ./find_duplicates.sh --delete --force   # Supprimer sans confirmation manuelle
#   ./find_duplicates.sh --dir /srv/exoria/spool   # Scanner un autre répertoire
#   ./find_duplicates.sh --all-local   # Scanne inbox + spool + quarantine
#
# Un "doublon" = session présente localement ET déjà uploadée sur le NAS
# (statut 'done' dans la DB ou manifest présent sur le NAS).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$(command -v python3)"

# ── CONFIG ────────────────────────────────────────────────────────────────────
INBOX_DIR="/srv/exoria/inbox"
SPOOL_DIR="/srv/exoria/spool"
QUARANTINE_DIR="/srv/exoria/quarantine"
DB_PATH="/srv/exoria/queue.db"

NAS_HOST="192.168.88.82"
NAS_PORT="22"
NAS_USER="sftpuser"
NAS_PASS="Exori@2026!"
NAS_BASE="/inbox"
NAS_QUARANTINE="/inbox/quarantine"

# ── ARGS ──────────────────────────────────────────────────────────────────────
DELETE=0
FORCE=0
ALL_LOCAL=0
SCAN_DIRS=("$INBOX_DIR")
LOG_FILE="/tmp/find_duplicates_$(date +%Y%m%d_%H%M%S).log"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete)       DELETE=1 ;;
    --force)        FORCE=1 ;;
    --all-local)    ALL_LOCAL=1 ;;
    --dir)          SCAN_DIRS=("$2"); shift ;;
    --log)          LOG_FILE="$2"; shift ;;
    -h|--help)
      echo "Usage: $0 [--delete] [--force] [--all-local] [--dir PATH]"
      echo ""
      echo "  (sans flag)     Dry-run : liste les doublons sans rien supprimer"
      echo "  --delete        Supprime les sessions locales déjà présentes sur le NAS"
      echo "  --force         Pas de confirmation interactivepar session (utilise avec --delete)"
      echo "  --all-local     Scanne inbox + spool + quarantine"
      echo "  --dir PATH      Scanne uniquement ce répertoire"
      echo "  --log FILE      Fichier log (défaut: /tmp/find_duplicates_DATE.log)"
      exit 0
      ;;
    *) echo "Option inconnue : $1"; exit 1 ;;
  esac
  shift
done

if [[ $ALL_LOCAL -eq 1 ]]; then
  SCAN_DIRS=("$INBOX_DIR" "$SPOOL_DIR" "$QUARANTINE_DIR")
fi

# ── COULEURS ──────────────────────────────────────────────────────────────────
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; C='\033[0;36m'; B='\033[1m'; N='\033[0m'

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
info() { log "  ${C}[INFO]${N} $*"; }
ok()   { log "  ${G}[OK]${N}   $*"; }
warn() { log "  ${Y}[WARN]${N} $*"; }
err()  { log "  ${R}[ERR]${N}  $*"; }

log "${B}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
log "${B}find_duplicates.sh — $(date '+%Y-%m-%d %H:%M:%S')${N}"
log "${B}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
log "Log: $LOG_FILE"
[[ $DELETE -eq 1 ]] && log "${R}MODE: SUPPRESSION ACTIVÉE${N}" || log "${Y}MODE: DRY-RUN (pas de suppression)${N}"
[[ $FORCE -eq 1 ]] && log "${Y}FORCE: confirmations désactivées${N}"
log ""

# ── ÉTAPE 1 : Collecter sessions locales ─────────────────────────────────────
log "${B}━━━ 1. Sessions locales ━━━${N}"

declare -A SESSION_TO_DIR   # session_id → chemin local
TOTAL_LOCAL=0
TOTAL_LOCAL_SIZE=0

for DIR in "${SCAN_DIRS[@]}"; do
  if [[ ! -d "$DIR" ]]; then
    warn "Répertoire inexistant, ignoré : $DIR"
    continue
  fi
  COUNT=0
  while IFS= read -r session_path; do
    session_id=$(basename "$session_path")
    SESSION_TO_DIR["$session_id"]="$session_path"
    COUNT=$((COUNT+1))
    TOTAL_LOCAL=$((TOTAL_LOCAL+1))
  done < <(find "$DIR" -maxdepth 1 -name 'session_*' -type d 2>/dev/null)
  info "$DIR : $COUNT sessions"
done

log "Total sessions locales trouvées : ${B}$TOTAL_LOCAL${N}"

if [[ $TOTAL_LOCAL -eq 0 ]]; then
  log "\nAucune session locale — rien à faire."
  exit 0
fi

# ── ÉTAPE 2 : Vérifier dans la DB SQLite (sessions 'done') ────────────────────
log "\n${B}━━━ 2. Sessions 'done' dans la base SQLite ━━━${N}"

DB_DONE_IDS=()
if [[ -f "$DB_PATH" ]]; then
  mapfile -t DB_DONE_IDS < <("$PYTHON" - <<PYEOF 2>/dev/null
import sqlite3
try:
    c = sqlite3.connect("$DB_PATH", timeout=5)
    rows = c.execute("SELECT session_id FROM jobs WHERE status='done'").fetchall()
    for r in rows:
        print(r[0])
    c.close()
except Exception as e:
    pass
PYEOF
)
  info "Sessions avec status='done' dans DB : ${#DB_DONE_IDS[@]}"
else
  warn "DB non trouvée : $DB_PATH (skip vérification DB)"
fi

# Construire un set des sessions done locales (intersection DB done ∩ local)
declare -A LOCAL_DB_DONE
for sid in "${DB_DONE_IDS[@]}"; do
  if [[ -n "${SESSION_TO_DIR[$sid]+_}" ]]; then
    LOCAL_DB_DONE["$sid"]="${SESSION_TO_DIR[$sid]}"
  fi
done
info "Sessions locales marquées 'done' en DB : ${#LOCAL_DB_DONE[@]}"

# ── ÉTAPE 3 : Vérifier sur le NAS via SFTP ────────────────────────────────────
log "\n${B}━━━ 3. Vérification sur le NAS ━━━${N}"

# On vérifie uniquement les sessions locales qui semblent doublons (dans DB done OU toutes si pas de DB)
if [[ ${#LOCAL_DB_DONE[@]} -gt 0 ]]; then
  SESSIONS_TO_CHECK=("${!LOCAL_DB_DONE[@]}")
  info "Vérification NAS limitée aux ${#SESSIONS_TO_CHECK[@]} sessions marquées 'done'"
else
  SESSIONS_TO_CHECK=("${!SESSION_TO_DIR[@]}")
  warn "Pas de DB ou aucune session done — vérification NAS de toutes les ${#SESSIONS_TO_CHECK[@]} sessions"
fi

NAS_CONFIRMED=()
NAS_NOT_FOUND=()
NAS_ERROR=0

if [[ ${#SESSIONS_TO_CHECK[@]} -gt 0 ]]; then
  # Sérialiser la liste pour Python
  SESSIONS_JSON=$(printf '%s\n' "${SESSIONS_TO_CHECK[@]}" | "$PYTHON" -c "
import sys, json
sessions = [l.strip() for l in sys.stdin if l.strip()]
print(json.dumps(sessions))
")

  NAS_RESULT=$("$PYTHON" - <<PYEOF 2>&1
import json, paramiko, posixpath, sys

sessions = $SESSIONS_JSON
nas_base = "$NAS_BASE"
nas_quarantine = "$NAS_QUARANTINE"

confirmed = []
not_found = []

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        "$NAS_HOST", port=$NAS_PORT,
        username="$NAS_USER", password="$NAS_PASS",
        timeout=15, banner_timeout=30, auth_timeout=20,
        look_for_keys=False, allow_agent=False
    )
    sftp = client.open_sftp()

    def session_exists_on_nas(session_id):
        """Cherche session_id dans bronze (YYYY/MM/DD/session_id) ou quarantine."""
        # Extraire date depuis session_YYYYMMDD_HHMMSS
        parts = session_id.split('_')
        if len(parts) >= 2 and len(parts[1]) == 8:
            d = parts[1]
            year, month, day = d[:4], d[4:6], d[6:8]
            bronze_path = posixpath.join(nas_base, year, month, day, session_id)
            try:
                sftp.stat(bronze_path)
                return "bronze", bronze_path
            except FileNotFoundError:
                pass
        # Chercher dans quarantine
        qpath = posixpath.join(nas_quarantine, session_id)
        try:
            sftp.stat(qpath)
            return "quarantine", qpath
        except FileNotFoundError:
            pass
        return None, None

    for sid in sessions:
        zone, path = session_exists_on_nas(sid)
        if zone:
            confirmed.append({"session_id": sid, "zone": zone, "nas_path": path})
        else:
            not_found.append(sid)

    sftp.close()
    client.close()
    print(json.dumps({"ok": True, "confirmed": confirmed, "not_found": not_found}))

except Exception as e:
    print(json.dumps({"ok": False, "error": str(e), "confirmed": [], "not_found": []}))
PYEOF
)

  NAS_OK=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('ok',False))" 2>/dev/null || echo "False")

  if [[ "$NAS_OK" == "True" ]]; then
    # Parser confirmed et not_found
    mapfile -t NAS_CONFIRMED_JSON < <(echo "$NAS_RESULT" | "$PYTHON" -c "
import sys,json
d=json.loads(sys.stdin.read())
for item in d.get('confirmed',[]):
    print(item['session_id'] + '|' + item['zone'] + '|' + item['nas_path'])
")
    mapfile -t NAS_NOT_FOUND < <(echo "$NAS_RESULT" | "$PYTHON" -c "
import sys,json
d=json.loads(sys.stdin.read())
for s in d.get('not_found',[]):
    print(s)
")
    ok "NAS connecté — ${#NAS_CONFIRMED_JSON[@]} doublons confirmés, ${#NAS_NOT_FOUND[@]} non trouvés sur NAS"
  else
    NAS_ERR=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('error','?'))" 2>/dev/null || echo "$NAS_RESULT")
    err "Impossible de se connecter au NAS : $NAS_ERR"
    err "Fallback : suppression basée uniquement sur la DB (status=done)"
    NAS_CONFIRMED_JSON=()
    NAS_ERROR=1
  fi
else
  NAS_CONFIRMED_JSON=()
  NAS_NOT_FOUND=()
fi

# ── ÉTAPE 4 : Construire la liste finale des doublons ─────────────────────────
log "\n${B}━━━ 4. Doublons identifiés ━━━${N}"

declare -A DUPLICATES   # session_id → "local_path|zone|nas_path"
DUPLICATES_SIZE_BYTES=0

if [[ ${#NAS_CONFIRMED_JSON[@]} -gt 0 ]]; then
  # Mode NAS: doublons confirmés sur le NAS
  for entry in "${NAS_CONFIRMED_JSON[@]}"; do
    IFS='|' read -r sid zone nas_path <<< "$entry"
    local_path="${SESSION_TO_DIR[$sid]:-}"
    if [[ -n "$local_path" && -d "$local_path" ]]; then
      SIZE=$(du -sb "$local_path" 2>/dev/null | awk '{print $1}' || echo 0)
      DUPLICATES["$sid"]="$local_path|$zone|$nas_path|$SIZE"
      DUPLICATES_SIZE_BYTES=$((DUPLICATES_SIZE_BYTES + SIZE))
    fi
  done
elif [[ $NAS_ERROR -eq 0 && ${#LOCAL_DB_DONE[@]} -gt 0 ]]; then
  # NAS non accessible mais DB dispo : utiliser seulement DB done (moins fiable)
  warn "Fallback DB: utilisation du status 'done' sans confirmation NAS"
  for sid in "${!LOCAL_DB_DONE[@]}"; do
    local_path="${LOCAL_DB_DONE[$sid]}"
    if [[ -d "$local_path" ]]; then
      SIZE=$(du -sb "$local_path" 2>/dev/null | awk '{print $1}' || echo 0)
      DUPLICATES["$sid"]="$local_path|db_done|?|$SIZE"
      DUPLICATES_SIZE_BYTES=$((DUPLICATES_SIZE_BYTES + SIZE))
    fi
  done
fi

# Convertir bytes en GB lisible
DUPLICATES_SIZE_GB=$(echo "$DUPLICATES_SIZE_BYTES" | "$PYTHON" -c "import sys; b=int(sys.stdin.read().strip()); print(f'{b/1e9:.2f}')")

if [[ ${#DUPLICATES[@]} -eq 0 ]]; then
  log "\n${G}Aucun doublon trouvé — inbox propre !${N}"
  log ""
  log "Récapitulatif :"
  log "  Sessions locales scannées : $TOTAL_LOCAL"
  log "  Sessions done en DB       : ${#LOCAL_DB_DONE[@]}"
  [[ $NAS_ERROR -eq 0 ]] && log "  Confirmées sur NAS        : 0"
  exit 0
fi

log "Doublons trouvés : ${B}${#DUPLICATES[@]}${N} sessions (${B}${DUPLICATES_SIZE_GB} GB${N} libérables)"
log ""
log "Liste des doublons :"

declare -a DELETE_LIST_PATHS
for sid in $(echo "${!DUPLICATES[@]}" | tr ' ' '\n' | sort); do
  IFS='|' read -r local_path zone nas_path size <<< "${DUPLICATES[$sid]}"
  SIZE_MB=$(echo "$size" | "$PYTHON" -c "import sys; b=int(sys.stdin.read().strip() or 0); print(f'{b/1e6:.0f}')")
  log "  ${Y}${sid}${N}"
  log "    Local  : $local_path (${SIZE_MB} MB)"
  log "    NAS    : $nas_path [$zone]"
  DELETE_LIST_PATHS+=("$local_path")
done

# ── ÉTAPE 5 : Suppression ─────────────────────────────────────────────────────
if [[ $DELETE -eq 0 ]]; then
  log ""
  log "${Y}Dry-run terminé. Pour supprimer : $0 --delete${N}"
  log "Log sauvegardé : $LOG_FILE"
  exit 0
fi

log "\n${B}━━━ 5. Suppression ━━━${N}"

if [[ $FORCE -eq 0 ]]; then
  echo -e "\n${R}${B}ATTENTION :${N} Tu vas supprimer ${#DUPLICATES[@]} sessions (${DUPLICATES_SIZE_GB} GB) localement."
  echo -e "Ces sessions sont déjà présentes sur le NAS — la suppression est sûre."
  echo -ne "Confirmer la suppression ? [y/N] "
  read -r CONFIRM
  [[ "$CONFIRM" =~ ^[yY]$ ]] || { log "Suppression annulée."; exit 0; }
fi

DELETED=0
DELETED_BYTES=0
ERRORS=0

for sid in $(echo "${!DUPLICATES[@]}" | tr ' ' '\n' | sort); do
  IFS='|' read -r local_path zone nas_path size <<< "${DUPLICATES[$sid]}"

  if [[ ! -d "$local_path" ]]; then
    warn "$sid : répertoire déjà supprimé ou introuvable ($local_path)"
    continue
  fi

  log "  Suppression : $sid ($(echo "$size" | "$PYTHON" -c "import sys; b=int(sys.stdin.read().strip() or 0); print(f'{b/1e6:.0f}')") MB)"

  if rm -rf "$local_path" 2>>"$LOG_FILE"; then
    ok "  Supprimé : $local_path"
    DELETED=$((DELETED+1))
    DELETED_BYTES=$((DELETED_BYTES + size))

    # Mettre à jour la DB si on supprime une session done
    if [[ -f "$DB_PATH" ]]; then
      "$PYTHON" - <<PYEOF 2>/dev/null || true
import sqlite3
c = sqlite3.connect("$DB_PATH", timeout=5)
c.execute("UPDATE jobs SET session_dir=NULL, updated_at=datetime('now') WHERE session_id=? AND status='done'", ("$sid",))
c.commit()
c.close()
PYEOF
    fi
  else
    err "  Échec suppression : $local_path"
    ERRORS=$((ERRORS+1))
  fi
done

FREED_GB=$(echo "$DELETED_BYTES" | "$PYTHON" -c "import sys; b=int(sys.stdin.read().strip() or 0); print(f'{b/1e9:.2f}')")

log ""
log "${B}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
log "${B}RÉSULTAT :${N}"
log "  Supprimés   : ${G}${DELETED}${N} sessions"
log "  Espace libéré : ${G}${FREED_GB} GB${N}"
[[ $ERRORS -gt 0 ]] && log "  Erreurs     : ${R}${ERRORS}${N}" || log "  Erreurs     : 0"

DISK_USAGE=$(df -h "$INBOX_DIR" 2>/dev/null | awk 'NR==2 {print $5}')
log "  Disque inbox après nettoyage : $DISK_USAGE"
log "${B}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
log "Log sauvegardé : $LOG_FILE"

[[ $ERRORS -gt 0 ]] && exit 1 || exit 0
