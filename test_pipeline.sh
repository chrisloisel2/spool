#!/usr/bin/env bash
# test_pipeline.sh — Vérifie que tous les composants du pipeline spool fonctionnent.
# Usage : ./test_pipeline.sh [--verbose] [--no-nas] [--no-kafka]
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── CONFIG ────────────────────────────────────────────────────────────────────
INBOX_DIR="/srv/exoria/inbox"
SPOOL_DIR="/srv/exoria/spool"
QUARANTINE_DIR="/srv/exoria/quarantine"
DB_PATH="/srv/exoria/queue.db"
LOG_DIR="/srv/exoria/logs"
NAS_HOST="192.168.88.82"
NAS_PORT="22"
NAS_USER="root"
NAS_PASS="Exori@2026!"
NAS_BASE="/data/INBOX/bronze"
KAFKA_BROKER="192.168.88.4:9092"
RABBITMQ_HOST="192.168.88.246"
PYTHON="$(command -v python3)"

VERBOSE=0
TEST_NAS=1
TEST_KAFKA=1
for arg in "$@"; do
  case "$arg" in
    --verbose) VERBOSE=1 ;;
    --no-nas)  TEST_NAS=0 ;;
    --no-kafka) TEST_KAFKA=0 ;;
  esac
done

# ── COULEURS ──────────────────────────────────────────────────────────────────
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; C='\033[0;36m'; B='\033[1m'; N='\033[0m'

PASS=0; FAIL=0; WARN=0
RESULTS=()

ok()   { echo -e "  ${G}[PASS]${N} $*"; PASS=$((PASS+1)); RESULTS+=("PASS: $*"); }
fail() { echo -e "  ${R}[FAIL]${N} $*"; FAIL=$((FAIL+1)); RESULTS+=("FAIL: $*"); }
warn() { echo -e "  ${Y}[WARN]${N} $*"; WARN=$((WARN+1)); RESULTS+=("WARN: $*"); }
info() { echo -e "  ${C}[INFO]${N} $*"; }
section() { echo -e "\n${B}━━━ $* ━━━${N}"; }

# ── 1. RÉPERTOIRES LOCAUX ─────────────────────────────────────────────────────
section "1. Répertoires locaux"

for dir in "$INBOX_DIR" "$SPOOL_DIR" "$QUARANTINE_DIR" "$LOG_DIR"; do
  if [[ -d "$dir" ]]; then
    ok "Répertoire existe : $dir"
    # Check permissions
    if [[ -r "$dir" && -w "$dir" ]]; then
      [[ $VERBOSE -eq 1 ]] && info "  r/w OK"
    else
      fail "Permissions r/w manquantes : $dir"
    fi
  else
    fail "Répertoire manquant : $dir"
  fi
done

# Espace disque
DISK_USAGE=$(df -h "$INBOX_DIR" 2>/dev/null | awk 'NR==2 {print $5}' | tr -d '%' || echo "0")
if [[ "$DISK_USAGE" -gt 90 ]]; then
  fail "Disque quasi-plein : ${DISK_USAGE}% utilisé sur $INBOX_DIR"
elif [[ "$DISK_USAGE" -gt 75 ]]; then
  warn "Disque à ${DISK_USAGE}% sur $INBOX_DIR"
else
  ok "Espace disque : ${DISK_USAGE}% utilisé"
fi

INBOX_COUNT=$(find "$INBOX_DIR" -maxdepth 1 -name 'session_*' -type d 2>/dev/null | wc -l | tr -d ' ')
SPOOL_COUNT=$(find "$SPOOL_DIR" -maxdepth 1 -name 'session_*' -type d 2>/dev/null | wc -l | tr -d ' ')
QUARANTINE_COUNT=$(find "$QUARANTINE_DIR" -maxdepth 1 -name 'session_*' -type d 2>/dev/null | wc -l | tr -d ' ')
info "Sessions dans inbox: ${INBOX_COUNT}  |  spool: ${SPOOL_COUNT}  |  quarantine: ${QUARANTINE_COUNT}"

# ── 2. BASE DE DONNÉES SQLITE ─────────────────────────────────────────────────
section "2. Base de données SQLite"

if [[ -f "$DB_PATH" ]]; then
  ok "DB existe : $DB_PATH"
  DB_SIZE=$(du -h "$DB_PATH" | awk '{print $1}')
  info "Taille DB : $DB_SIZE"

  # Vérifier que la table jobs existe et lire les stats
  DB_STATS=$("$PYTHON" - <<PYEOF 2>&1
import sqlite3, json
try:
    c = sqlite3.connect("$DB_PATH", timeout=5)
    rows = c.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status").fetchall()
    total = c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    failed = c.execute("SELECT COUNT(*) FROM jobs WHERE status='failed'").fetchone()[0]
    stale = c.execute("""
        SELECT COUNT(*) FROM jobs
        WHERE status='processing'
        AND datetime(updated_at) < datetime('now', '-1 hour')
    """).fetchone()[0]
    print(json.dumps({"ok": True, "stats": dict(rows), "total": total, "failed": failed, "stale": stale}))
    c.close()
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
PYEOF
)

  DB_OK=$(echo "$DB_STATS" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('ok','false'))" 2>/dev/null || echo "false")
  if [[ "$DB_OK" == "True" ]]; then
    ok "DB accessible et lisible"
    TOTAL=$(echo "$DB_STATS" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('total',0))")
    FAILED=$(echo "$DB_STATS" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('failed',0))")
    STALE=$(echo "$DB_STATS" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('stale',0))")
    info "Total jobs : $TOTAL  |  failed : $FAILED  |  processing>1h (stale) : $STALE"
    [[ "$FAILED" -gt 0 ]] && warn "$FAILED jobs en état 'failed' (run: python3 retry.py --status)"
    [[ "$STALE" -gt 0 ]] && warn "$STALE jobs bloqués en 'processing' depuis >1h (run: python3 retry.py --requeue-processing)"
    STATS=$(echo "$DB_STATS" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); [print(f'    {k}: {v}') for k,v in d.get('stats',{}).items()]")
    [[ $VERBOSE -eq 1 ]] && info "Détail:\n$STATS"
  else
    ERR=$(echo "$DB_STATS" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('error','?'))")
    fail "DB inaccessible : $ERR"
  fi
else
  warn "DB inexistante (daemon jamais lancé ?) : $DB_PATH"
fi

# ── 3. SCRIPTS PYTHON / DÉPENDANCES ──────────────────────────────────────────
section "3. Scripts Python et dépendances"

# Vérifier que les scripts clés existent
for f in spool.py retry.py dashboard.py diag.py; do
  if [[ -f "$SCRIPT_DIR/$f" ]]; then
    ok "Script existe : $f"
  else
    fail "Script manquant : $f"
  fi
done

# Dépendances Python
"$PYTHON" - <<PYEOF 2>/dev/null
import sys

modules = {
    "paramiko": "connexions SFTP/SSH",
    "kafka": "Kafka producer (kafka-python)",
    "pika": "RabbitMQ (pika)",
    "curses": "TUI terminal",
    "sqlite3": "base de données",
}

missing = []
for mod, desc in modules.items():
    try:
        __import__(mod)
        print(f"  \033[0;32m[PASS]\033[0m Module Python OK : {mod} ({desc})")
    except ImportError:
        print(f"  \033[0;31m[FAIL]\033[0m Module Python manquant : {mod} ({desc})")
        missing.append(mod)

if missing:
    print(f"\n  \033[1;33m[WARN]\033[0m Installer les modules manquants : pip3 install {' '.join(missing)}")
PYEOF

# ── 4. SCRIPTS QUALITY PROCESSUS ─────────────────────────────────────────────
section "4. Scripts quality_processus"

QC_DIR="$SCRIPT_DIR/quality_processus"
if [[ -d "$QC_DIR" ]]; then
  ok "Répertoire quality_processus existe"
  for script in fix_shit.sh files.sh sanity.sh; do
    if [[ -f "$QC_DIR/$script" ]]; then
      if [[ -x "$QC_DIR/$script" ]]; then
        ok "Script exécutable : $script"
      else
        warn "Script non-exécutable (chmod +x) : $script"
      fi
    else
      fail "Script QC manquant : $script"
    fi
  done
  for script in quality.sh verify_naming.sh; do
    if [[ -f "$QC_DIR/$script" ]]; then
      [[ $VERBOSE -eq 1 ]] && info "Script désactivé présent : $script"
    else
      warn "Script QC optionnel manquant : $script"
    fi
  done
else
  fail "Répertoire quality_processus manquant : $QC_DIR"
fi

# Test rapide fix_shit.sh sur une session de test temporaire
TMP_SESSION=$(mktemp -d /tmp/session_XXXXXX)
SESSION_NAME=$(basename "$TMP_SESSION")
mkdir -p "$TMP_SESSION/videos"
echo '{"scenario":"test","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T00:01:00Z","duration":60,"camera_count":3,"tracker_count":1,"failed":false}' > "$TMP_SESSION/metadata.json"
# Ligne JSONL valide + ligne tronquée pour tester le fix
echo '{"frame":1,"ts":0.0}' > "$TMP_SESSION/videos/head.jsonl"
echo '{"frame":2,"ts":' >> "$TMP_SESSION/videos/head.jsonl"  # tronqué intentionnellement

if [[ -x "$QC_DIR/fix_shit.sh" ]]; then
  FIX_OUT=$("$QC_DIR/fix_shit.sh" "$TMP_SESSION" 2>&1 || true)
  LAST_LINE=$(tail -n1 "$TMP_SESSION/videos/head.jsonl")
  if echo "$LAST_LINE" | "$PYTHON" -c "import sys,json; json.loads(sys.stdin.read())" 2>/dev/null; then
    ok "fix_shit.sh : JSONL tronqué correctement réparé"
    [[ $VERBOSE -eq 1 ]] && info "Output fix_shit.sh:\n$FIX_OUT"
  else
    fail "fix_shit.sh : JSONL non réparé"
  fi
else
  warn "fix_shit.sh non-exécutable, test ignoré"
fi
rm -rf "$TMP_SESSION"

# ── 5. TEST NAS ───────────────────────────────────────────────────────────────
if [[ $TEST_NAS -eq 1 ]]; then
  section "5. Connectivité NAS (SFTP)"

  NAS_RESULT=$("$PYTHON" - <<PYEOF 2>&1
import json, time
try:
    import paramiko
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    t0 = time.time()
    client.connect(
        "$NAS_HOST", port=$NAS_PORT, username="$NAS_USER", password="$NAS_PASS",
        timeout=10, banner_timeout=20, auth_timeout=15,
        look_for_keys=False, allow_agent=False
    )
    latency = (time.time() - t0) * 1000
    sftp = client.open_sftp()

    # Vérifier les répertoires de base
    dirs_ok = []
    dirs_fail = []
    for d in ["$NAS_BASE", "/data/INBOX/quarantine"]:
        try:
            sftp.stat(d)
            dirs_ok.append(d)
        except FileNotFoundError:
            dirs_fail.append(d)

    # Test écriture : créer un fichier de test et le supprimer
    test_path = "$NAS_BASE/_test_spool_connectivity"
    write_ok = False
    try:
        with sftp.file(test_path, 'w') as f:
            f.write("test")
        sftp.remove(test_path)
        write_ok = True
    except Exception as e:
        pass

    # Stats espace NAS
    try:
        stat = sftp.statvfs("$NAS_BASE")
        free_gb = (stat.f_bavail * stat.f_bsize) / (1024**3)
        total_gb = (stat.f_blocks * stat.f_bsize) / (1024**3)
        used_pct = int((1 - stat.f_bavail / stat.f_blocks) * 100) if stat.f_blocks > 0 else 0
    except Exception:
        free_gb = -1; total_gb = -1; used_pct = -1

    sftp.close(); client.close()
    print(json.dumps({
        "ok": True, "latency_ms": round(latency, 1),
        "dirs_ok": dirs_ok, "dirs_fail": dirs_fail,
        "write_ok": write_ok,
        "free_gb": round(free_gb, 1), "total_gb": round(total_gb, 1), "used_pct": used_pct
    }))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
PYEOF
)

  NAS_OK=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('ok',False))" 2>/dev/null || echo "False")
  if [[ "$NAS_OK" == "True" ]]; then
    LATENCY=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('latency_ms','?'))")
    FREE=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('free_gb','?'))")
    TOTAL=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('total_gb','?'))")
    USED_PCT=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('used_pct',-1))")
    WRITE_OK=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('write_ok',False))")
    ok "NAS connecté (${LATENCY}ms) — ${FREE}GB libres / ${TOTAL}GB total"
    [[ "$WRITE_OK" == "True" ]] && ok "NAS : écriture SFTP OK" || fail "NAS : écriture SFTP impossible"
    [[ "$USED_PCT" -gt 90 ]] && fail "NAS quasi-plein : ${USED_PCT}% utilisé" || \
    [[ "$USED_PCT" -gt 75 ]] && warn "NAS à ${USED_PCT}% utilisé" || \
    ok "NAS espace : ${USED_PCT}% utilisé"

    DIRS_FAIL=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(' '.join(d.get('dirs_fail',[])))")
    [[ -n "$DIRS_FAIL" ]] && warn "Répertoires NAS manquants : $DIRS_FAIL"

    # Compter les sessions sur le NAS
    NAS_COUNT=$("$PYTHON" - <<PYEOF2 2>/dev/null || echo "?"
import paramiko
try:
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect("$NAS_HOST", port=$NAS_PORT, username="$NAS_USER", password="$NAS_PASS",
              timeout=10, banner_timeout=20, look_for_keys=False, allow_agent=False)
    sftp = c.open_sftp()
    total = 0
    try:
        for year in sftp.listdir("$NAS_BASE"):
            try:
                for month in sftp.listdir(f"$NAS_BASE/{year}"):
                    try:
                        for day in sftp.listdir(f"$NAS_BASE/{year}/{month}"):
                            try:
                                sessions = sftp.listdir(f"$NAS_BASE/{year}/{month}/{day}")
                                total += len([s for s in sessions if s.startswith('session_')])
                            except: pass
                    except: pass
            except: pass
    except: pass
    sftp.close(); c.close()
    print(total)
except Exception as e:
    print("?")
PYEOF2
)
    info "Sessions sur NAS (bronze) : $NAS_COUNT"
  else
    NAS_ERR=$(echo "$NAS_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('error','?'))" 2>/dev/null || echo "$NAS_RESULT")
    fail "NAS inaccessible : $NAS_ERR"
  fi
fi

# ── 6. KAFKA ──────────────────────────────────────────────────────────────────
if [[ $TEST_KAFKA -eq 1 ]]; then
  section "6. Kafka"

  KAFKA_RESULT=$("$PYTHON" - <<PYEOF 2>&1
import json, time
try:
    from kafka import KafkaProducer, KafkaAdminClient
    from kafka.errors import KafkaError
    t0 = time.time()
    p = KafkaProducer(
        bootstrap_servers=["$KAFKA_BROKER"],
        request_timeout_ms=5000,
        api_version=(2,0,0),
        retries=0
    )
    latency = (time.time() - t0) * 1000
    # Test envoi
    future = p.send("monitoring", json.dumps({"source":"spool_test","ts":time.time(),"step":"connectivity_check"}).encode())
    future.get(timeout=5)
    p.flush(timeout=5)
    p.close()
    print(json.dumps({"ok": True, "latency_ms": round(latency,1)}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
PYEOF
)

  KAFKA_OK=$(echo "$KAFKA_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('ok',False))" 2>/dev/null || echo "False")
  if [[ "$KAFKA_OK" == "True" ]]; then
    KAFKA_LAT=$(echo "$KAFKA_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('latency_ms','?'))")
    ok "Kafka connecté et message envoyé (${KAFKA_LAT}ms)"
  else
    KAFKA_ERR=$(echo "$KAFKA_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('error','?'))" 2>/dev/null || echo "$KAFKA_RESULT")
    fail "Kafka inaccessible : $KAFKA_ERR"
  fi

  # RabbitMQ
  RABBIT_RESULT=$("$PYTHON" - <<PYEOF 2>&1
import json, time
try:
    import pika
    t0 = time.time()
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        host="$RABBITMQ_HOST", port=5672, connection_attempts=1,
        socket_timeout=5, blocked_connection_timeout=5
    ))
    latency = (time.time() - t0) * 1000
    ch = conn.channel()
    ch.queue_declare(queue='ingestion_queue', durable=True, passive=True)
    q = ch.queue_declare(queue='ingestion_queue', durable=True, passive=True)
    msg_count = q.method.message_count
    conn.close()
    print(json.dumps({"ok": True, "latency_ms": round(latency,1), "pending": msg_count}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
PYEOF
)

  RABBIT_OK=$(echo "$RABBIT_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('ok',False))" 2>/dev/null || echo "False")
  if [[ "$RABBIT_OK" == "True" ]]; then
    RABBIT_LAT=$(echo "$RABBIT_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('latency_ms','?'))")
    PENDING=$(echo "$RABBIT_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('pending','?'))")
    ok "RabbitMQ connecté (${RABBIT_LAT}ms) — ingestion_queue: $PENDING messages en attente"
  else
    RABBIT_ERR=$(echo "$RABBIT_RESULT" | "$PYTHON" -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('error','?'))" 2>/dev/null || echo "$RABBIT_RESULT")
    fail "RabbitMQ inaccessible : $RABBIT_ERR"
  fi
fi

# ── 7. DAEMON SPOOL (PID check) ───────────────────────────────────────────────
section "7. Daemon spool"

PID_FILE="/tmp/spool_daemon.pid"
if [[ -f "$PID_FILE" ]]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    ok "Daemon spool en cours (PID $PID)"
    # Vérifier l'âge du log
    if [[ -f "/srv/exoria/logs/spool.log" ]]; then
      LOG_AGE=$(( $(date +%s) - $(stat -c%Y "/srv/exoria/logs/spool.log" 2>/dev/null || echo 0) ))
      if [[ $LOG_AGE -gt 60 ]]; then
        warn "Log spool.log non mis à jour depuis ${LOG_AGE}s — daemon bloqué ?"
      else
        ok "Log spool.log actif (mis à jour il y a ${LOG_AGE}s)"
      fi
    fi
  else
    fail "PID file existe ($PID_FILE) mais process $PID mort — relancer avec ./run.sh"
  fi
else
  warn "Daemon non lancé (pas de PID file) — démarrer avec ./run.sh"
fi

# ── 8. SESSIONS BLOQUÉES ──────────────────────────────────────────────────────
section "8. Sessions bloquées / anormales"

# Chercher des sessions dans inbox modifiées il y a plus de 30 min (pas en cours de copie)
OLD_INBOX=$(find "$INBOX_DIR" -maxdepth 1 -name 'session_*' -type d -mmin +30 2>/dev/null | wc -l | tr -d ' ')
if [[ "$OLD_INBOX" -gt 0 ]]; then
  warn "$OLD_INBOX sessions dans inbox depuis >30 min (daemon trop lent ou arrêté ?)"
  if [[ $VERBOSE -eq 1 ]]; then
    find "$INBOX_DIR" -maxdepth 1 -name 'session_*' -type d -mmin +30 2>/dev/null | while read -r d; do
      info "  $(basename "$d")  $(du -sh "$d" 2>/dev/null | awk '{print $1}')"
    done
  fi
else
  ok "Pas de sessions bloquées dans inbox"
fi

# Chercher des sessions dans spool non uploadées (status=queued dans DB depuis >1h)
if [[ -f "$DB_PATH" ]]; then
  OLD_SPOOL=$("$PYTHON" - <<PYEOF 2>/dev/null || echo "0"
import sqlite3
c = sqlite3.connect("$DB_PATH", timeout=5)
r = c.execute("""
    SELECT COUNT(*) FROM jobs
    WHERE status IN ('queued', 'processing')
    AND datetime(created_at) < datetime('now', '-1 hour')
""").fetchone()[0]
print(r)
c.close()
PYEOF
)
  [[ "$OLD_SPOOL" -gt 0 ]] && warn "$OLD_SPOOL jobs queued/processing depuis >1h" || ok "Pas de jobs anciens bloqués"
fi

# ── 9. INTÉGRITÉ DES SESSIONS INBOX (SAMPLE) ─────────────────────────────────
section "9. Intégrité sessions inbox (échantillon)"

SAMPLE_SESSIONS=$(find "$INBOX_DIR" -maxdepth 1 -name 'session_*' -type d 2>/dev/null | head -5)
if [[ -z "$SAMPLE_SESSIONS" ]]; then
  info "Aucune session dans inbox pour tester"
else
  while IFS= read -r SESSION; do
    SESSION_ID=$(basename "$SESSION")
    ISSUES=()
    # Check fichiers requis
    for f in metadata.json tracker_positions.csv gripper_left_data.csv gripper_right_data.csv \
              videos/head.mp4 videos/left.mp4 videos/right.mp4 \
              videos/head.jsonl videos/left.jsonl videos/right.jsonl; do
      [[ -f "$SESSION/$f" ]] || ISSUES+=("$f manquant")
    done
    # Check metadata parseable
    if [[ -f "$SESSION/metadata.json" ]]; then
      "$PYTHON" -c "import json; json.load(open('$SESSION/metadata.json'))" 2>/dev/null || ISSUES+=("metadata.json invalide")
    fi
    if [[ ${#ISSUES[@]} -eq 0 ]]; then
      ok "$SESSION_ID — OK"
    else
      fail "$SESSION_ID — ${#ISSUES[@]} problème(s): ${ISSUES[*]}"
    fi
  done <<< "$SAMPLE_SESSIONS"
fi

# ── RÉSUMÉ ────────────────────────────────────────────────────────────────────
echo -e "\n${B}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
echo -e "${B}RÉSULTAT : ${G}${PASS} PASS${N}  ${R}${FAIL} FAIL${N}  ${Y}${WARN} WARN${N}"
echo -e "${B}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
if [[ $FAIL -gt 0 ]]; then
  echo -e "\n${R}Erreurs critiques :${N}"
  for r in "${RESULTS[@]}"; do [[ "$r" == FAIL:* ]] && echo "  • ${r#FAIL: }"; done
fi
if [[ $WARN -gt 0 ]]; then
  echo -e "\n${Y}Avertissements :${N}"
  for r in "${RESULTS[@]}"; do [[ "$r" == WARN:* ]] && echo "  • ${r#WARN: }"; done
fi
echo ""
[[ $FAIL -gt 0 ]] && exit 1 || exit 0
