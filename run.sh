#!/usr/bin/env bash
# run.sh — Lance spool comme service permanent, puis affiche le TUI live.
# Usage : ./run.sh [stop|restart|logs|status]
set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPOOL_PY="${SCRIPT_DIR}/spool.py"
PYTHON="$(command -v python3)"
LOG_FILE="/srv/exoria/logs/spool.log"
PID_FILE="/tmp/spool_daemon.pid"

KAFKA_BROKER="192.168.88.4:9092"
KAFKA_TOPIC="monitoring"

# ── KAFKA EMIT ────────────────────────────────────────────────────────────────
kafka_emit() {
    local step="$1" status="$2" extra="${3:-}"
    "$PYTHON" - <<PYEOF 2>/dev/null || true
import json, time, datetime as dt
try:
    from kafka import KafkaProducer
    p = KafkaProducer(
        bootstrap_servers=["${KAFKA_BROKER}"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        api_version=(2, 0, 0),
        retries=3,
        request_timeout_ms=5000,
    )
    ev = {
        "source":  "spool_daemon",
        "ts":      time.time(),
        "ts_iso":  dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "step":    "${step}",
        "status":  "${status}",
    }
    extra = ${extra:-{}}
    ev.update(extra)
    p.send("${KAFKA_TOPIC}", ev)
    p.flush(timeout=5)
    p.close()
except Exception as e:
    pass
PYEOF
}

# Couleurs
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'; C='\033[0;36m'; B='\033[1m'; N='\033[0m'

banner() {
    echo -e "${C}${B}"
    echo "  ╔══════════════════════════════════════╗"
    echo "  ║       ⚡  SPOOL TURBO  — run.sh      ║"
    echo "  ╚══════════════════════════════════════╝"
    echo -e "${N}"
}

info()  { echo -e "  ${G}▶${N} $*"; }
warn()  { echo -e "  ${Y}⚠${N}  $*"; }
err()   { echo -e "  ${R}✗${N}  $*" >&2; }
ok()    { echo -e "  ${G}✓${N}  $*"; }

# ── HELPERS ───────────────────────────────────────────────────────────────────

is_running() {
    [[ -f "$PID_FILE" ]] || return 1
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
    kill -0 "$pid" 2>/dev/null
}

start_daemon() {
    # Tue toutes les instances spool.py existantes avant de démarrer
    local existing
    existing=$(pgrep -f "python.*spool\.py" 2>/dev/null || true)
    if [[ -n "$existing" ]]; then
        warn "Instance(s) existante(s) détectée(s) (pid: $existing) — arrêt forcé..."
        kill $existing 2>/dev/null || true
        sleep 2
        kill -9 $existing 2>/dev/null || true
    fi
    rm -f "$PID_FILE"

    # Crée les répertoires nécessaires
    mkdir -p /srv/exoria/inbox /srv/exoria/spool /srv/exoria/quarantine
    mkdir -p "$(dirname "$LOG_FILE")"
    touch "$LOG_FILE"

    info "Démarrage du daemon spool..."

    # Lance en arrière-plan, stdout/stderr → log
    nohup "$PYTHON" -u "$SPOOL_PY" >> "$LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"

    # Vérifie qu'il a bien démarré (attend 2s)
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        ok "Daemon démarré  (pid=$pid)"
        ok "Logs : $LOG_FILE"
        kafka_emit "daemon" "started" "{\"pid\": $pid, \"log\": \"$LOG_FILE\", \"workers\": 16, \"nas_host\": \"192.168.88.82\"}"
    else
        err "Le daemon a planté au démarrage. Consulte les logs :"
        tail -20 "$LOG_FILE" | sed 's/^/    /'
        kafka_emit "daemon" "start_failed" "{\"log\": \"$LOG_FILE\"}"
        exit 1
    fi
}

stop_daemon() {
    if ! is_running; then
        warn "spool n'est pas en cours d'exécution."
        return 0
    fi
    local pid
    pid=$(cat "$PID_FILE")
    info "Arrêt du daemon (pid=$pid)..."
    kill "$pid" 2>/dev/null || true

    # Attend jusqu'à 10s
    local i=0
    while kill -0 "$pid" 2>/dev/null && [[ $i -lt 10 ]]; do
        sleep 1; (( i++ ))
    done

    if kill -0 "$pid" 2>/dev/null; then
        warn "Le process résiste, envoi de SIGKILL..."
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$PID_FILE"
    ok "Daemon arrêté."
    kafka_emit "daemon" "stopped" "{\"pid\": $pid}"
}

show_status() {
    echo ""
    if is_running; then
        local pid; pid=$(cat "$PID_FILE")
        echo -e "  Statut  : ${G}${B}EN COURS${N}  (pid=$pid)"
    else
        echo -e "  Statut  : ${R}${B}ARRÊTÉ${N}"
    fi
    echo -e "  Log     : $LOG_FILE"
    echo -e "  Python  : $PYTHON"
    echo -e "  Script  : $SPOOL_PY"
    echo ""
}

attach_tui() {
    info "Lancement du TUI live... (q pour quitter le visuel, le daemon continue)"
    echo ""
    sleep 0.5
    "$PYTHON" - << 'PYEOF'
import sys, os, time, curses, sqlite3, datetime as dt

LOG_FILE  = "/srv/exoria/logs/spool.log"
DB_PATH   = "/srv/exoria/queue.db"
PID_FILE  = "/tmp/spool_daemon.pid"
WORKERS   = 16
NAS_HOST  = "192.168.88.82"
# Nombre d'octets lus depuis la fin du log pour la section logs
LOG_TAIL_BYTES = 32000

def _fmt_size(b):
    if b >= 1<<30: return f"{b/(1<<30):.1f} GB"
    if b >= 1<<20: return f"{b/(1<<20):.1f} MB"
    if b >= 1<<10: return f"{b/(1<<10):.0f} KB"
    return f"{b} B"

def _fmt_eta(remaining_b, speed_mbps):
    if speed_mbps <= 0 or remaining_b <= 0: return "--:--"
    secs = int(remaining_b / (speed_mbps * (1<<20)))
    h, r = divmod(secs, 3600); m, s = divmod(r, 60)
    if h:  return f"{h}h{m:02d}m"
    if m:  return f"{m}m{s:02d}s"
    return f"{s}s"

def _bar(pct, width=20, filled="█", empty="░"):
    n = max(0, min(int(pct / 100 * width), width))
    return filled * n + empty * (width - n)

def is_running():
    try:
        pid = int(open(PID_FILE).read().strip())
        os.kill(pid, 0)
        return pid
    except Exception:
        return None

def db_stats():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=2)
        rows = conn.execute(
            "SELECT status, COUNT(*), SUM(size_bytes) FROM jobs GROUP BY status"
        ).fetchall()
        conn.close()
        counts, sizes = {}, {}
        for st, n, b in rows:
            counts[st] = n
            sizes[st] = b or 0
        queued     = counts.get("queued", 0)
        processing = counts.get("processing", 0)
        done       = counts.get("done", 0)
        failed     = counts.get("failed", 0)
        remaining_b = sizes.get("queued", 0) + sizes.get("processing", 0)
        total_b     = sizes.get("queued", 0) + sizes.get("processing", 0) + sizes.get("done", 0)
        sent_b      = sizes.get("done", 0)
        return dict(queued=queued, processing=processing, done=done, failed=failed,
                    total_b=total_b, sent_b=sent_b, remaining_b=remaining_b)
    except Exception:
        return dict(queued=0, processing=0, done=0, failed=0,
                    total_b=0, sent_b=0, remaining_b=0)

def read_log_tail_raw(n_bytes=None, n_lines=None):
    """Retourne les dernières lignes non-vides du log."""
    try:
        size = os.path.getsize(LOG_FILE)
        read_bytes = n_bytes or LOG_TAIL_BYTES
        with open(LOG_FILE, "rb") as f:
            f.seek(max(0, size - read_bytes))
            raw = f.read().decode("utf-8", errors="replace")
        lines = [l for l in raw.splitlines() if l.strip()]
        if n_lines:
            return lines[-n_lines:]
        return lines
    except Exception as e:
        return [f"[erreur lecture log: {e}]"]

def parse_worker_states():
    """
    Parse le log pour extraire l'état de chaque worker actif.
    Retourne dict: session_id -> {files_done, file_count, speed_mbps, current_file, last_seen}
    Format log: [JOB xxxxxxxx] N/M 'path' @ X.X MB/s
    """
    lines = read_log_tail_raw(n_bytes=64000)
    workers = {}
    import re
    pat = re.compile(r'\[JOB ([0-9a-f]+)\]\s+(\d+)/(\d+)\s+\'([^\']+)\'\s+@\s+([\d.]+)\s+MB/s')
    for line in lines:
        m = pat.search(line)
        if m:
            jid, done, total, fname, spd = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4), float(m.group(5))
            # Cherche session_id associé au job dans les lignes précédentes
            workers[jid] = {"files_done": done, "file_count": total,
                            "current_file": fname, "speed_mbps": spd}
    # Associe session_id depuis les lignes "Session '...' — N fichiers"
    pat2 = re.compile(r"\[JOB ([0-9a-f]+)\] Session '([^']+)'")
    for line in lines:
        m = pat2.search(line)
        if m:
            jid, sid = m.group(1), m.group(2)
            if jid in workers:
                workers[jid]["session_id"] = sid
    # Construit liste indexée par session_id
    result = {}
    for jid, w in workers.items():
        sid = w.get("session_id", jid[:8])
        result[sid] = w
    return result

def read_log_speed(lines=None):
    """Vitesse totale = somme des dernières vitesses par worker actif."""
    if lines is None:
        lines = read_log_tail_raw(n_bytes=16000)
    import re
    pat = re.compile(r'\[JOB ([0-9a-f]+)\].*@\s+([\d.]+)\s+MB/s')
    last_speed = {}
    for line in reversed(lines):
        m = pat.search(line)
        if m:
            jid, spd = m.group(1), float(m.group(2))
            if jid not in last_speed:
                last_speed[jid] = spd
        if len(last_speed) >= WORKERS:
            break
    return sum(last_speed.values()) if last_speed else 0.0

def read_log_tail(n):
    """Retourne les n dernières lignes non-vides du log (pour section logs)."""
    # Filtre les lignes DEBUG trop verbeuses pour le TUI
    lines = read_log_tail_raw(n_bytes=LOG_TAIL_BYTES)
    filtered = [l for l in lines if " DEBUG " not in l]
    return filtered[-n:]

def main(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_CYAN,    -1)
    curses.init_pair(2, curses.COLOR_GREEN,   -1)
    curses.init_pair(3, curses.COLOR_YELLOW,  -1)
    curses.init_pair(4, curses.COLOR_RED,     -1)
    curses.init_pair(5, curses.COLOR_MAGENTA, -1)
    curses.init_pair(6, curses.COLOR_WHITE,   -1)
    curses.init_pair(7, curses.COLOR_BLACK,   curses.COLOR_CYAN)

    C_TITLE = curses.color_pair(1) | curses.A_BOLD
    C_OK    = curses.color_pair(2)
    C_WARN  = curses.color_pair(3)
    C_ERR   = curses.color_pair(4)
    C_STAT  = curses.color_pair(5) | curses.A_BOLD
    C_NORM  = curses.color_pair(6)
    C_HDR   = curses.color_pair(7)

    # Uptime depuis le démarrage du daemon (lu depuis le log)
    daemon_start = None
    try:
        size = os.path.getsize(LOG_FILE)
        with open(LOG_FILE, "rb") as f:
            header = f.read(200).decode("utf-8", errors="replace")
        first_line = [l for l in header.splitlines() if l.strip()]
        if first_line:
            ts_str = first_line[0][:19]
            daemon_start = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        daemon_start = time.time()

    def put(r, c, text, attr=C_NORM):
        rows, cols = stdscr.getmaxyx()
        if r >= rows - 1 or c >= cols: return
        try: stdscr.addstr(r, c, text[:cols - c], attr)
        except curses.error: pass

    def hline(r, char="─"):
        rows, cols = stdscr.getmaxyx()
        put(r, 0, char * cols)

    while True:
        key = stdscr.getch()
        if key == ord('q'): break

        stdscr.erase()
        rows, cols = stdscr.getmaxyx()

        pid        = is_running()
        st         = db_stats()
        speed_mbps = read_log_speed()

        elapsed = time.time() - (daemon_start or time.time())
        eta_str = _fmt_eta(st["remaining_b"], speed_mbps)
        total_b = st["total_b"]
        sent_b  = st["sent_b"]
        global_pct = sent_b / max(total_b, 1) * 100

        row = 0

        # ── HEADER ───────────────────────────────────────────────────────────
        status_tag = f"PID {pid}" if pid else "ARRÊTÉ"
        hdr_attr   = C_HDR if pid else C_ERR | curses.A_BOLD
        uptime     = f"up {int(elapsed//3600):02d}h{int((elapsed%3600)//60):02d}m"
        hdr = f" ⚡ SPOOL  ·  {WORKERS} workers  ·  NAS {NAS_HOST}  ·  {status_tag}  ·  {uptime} "
        put(row, 0, hdr.ljust(cols)[:cols], hdr_attr); row += 1

        # ── COMPTEURS ────────────────────────────────────────────────────────
        hline(row); row += 1
        put(row,  2, f"✓ {st['done']:>6} envoyées",    C_OK)
        put(row, 24, f"⧖ {st['queued']:>5} en attente", C_WARN)
        put(row, 44, f"⚙ {st['processing']:>2} actifs",  C_STAT)
        put(row, 58, f"✗ {st['failed']:>5} échecs",     C_ERR if st['failed'] else C_NORM)
        row += 1

        # ── BARRE GLOBALE ─────────────────────────────────────────────────────
        hline(row); row += 1
        bar_w = max(10, cols - 34)
        put(row, 2, "Global  ", C_TITLE)
        put(row, 10, _bar(global_pct, bar_w), C_OK if global_pct > 50 else C_WARN)
        put(row, 10 + bar_w + 1, f"{global_pct:5.1f}%", C_STAT)
        put(row, 10 + bar_w + 8, f"{_fmt_size(sent_b)} / {_fmt_size(total_b)}", C_NORM)
        row += 1

        # ── VITESSE + ETA ─────────────────────────────────────────────────────
        put(row, 2, "Vitesse  ", C_TITLE)
        put(row, 11, f"{speed_mbps:6.1f} MB/s", C_STAT)
        put(row, 28, f"restant {_fmt_size(st['remaining_b'])}", C_NORM)
        put(row, 48, f"ETA {eta_str}", C_WARN if eta_str != "--:--" else C_NORM)
        row += 1

        # ── WORKERS DÉTAIL ───────────────────────────────────────────────────
        hline(row); row += 1
        put(row, 2, f"Transferts actifs  {st['processing']}/{WORKERS}  —  {st['queued']} en attente", C_TITLE); row += 1
        hline(row, "╌"); row += 1

        worker_states = parse_worker_states()
        bar_w2 = max(10, cols - 52)

        # Sessions processing depuis la DB
        try:
            conn2 = sqlite3.connect(DB_PATH, timeout=1)
            proc_rows = conn2.execute(
                "SELECT session_id, size_bytes, file_count FROM jobs WHERE status='processing' ORDER BY updated_at DESC LIMIT 16"
            ).fetchall()
            conn2.close()
        except Exception:
            proc_rows = []

        displayed = 0
        for sid, size_b, fc in proc_rows:
            if row >= rows - 6: break
            w = worker_states.get(sid, {})
            fd    = w.get("files_done", 0)
            ft    = w.get("file_count", fc or 1)
            spd   = w.get("speed_mbps", 0.0)
            fname = w.get("current_file", "…")[-28:]
            pct   = fd / max(ft, 1) * 100
            label = sid[-22:].ljust(22)
            bar   = _bar(pct, bar_w2)
            put(row, 2,  label, C_NORM)
            put(row, 25, bar,   C_OK if pct > 50 else C_WARN)
            put(row, 25 + bar_w2 + 1, f"{pct:5.1f}%", C_STAT)
            put(row, 25 + bar_w2 + 8, f"{fd:>3}/{ft}f  {spd:.1f}MB/s", C_NORM)
            row += 1
            put(row, 4, f"↳ {fname}", C_NORM); row += 1
            displayed += 1

        if displayed == 0:
            put(row, 4, "(aucun transfert actif détecté dans le log)", C_NORM); row += 1

        # ── LOGS ─────────────────────────────────────────────────────────────
        if row < rows - 4:
            hline(row); row += 1
            put(row, 2, "Logs récents  (q = quitter)", C_TITLE); row += 1
            hline(row, "╌"); row += 1

            avail = max(0, rows - row - 1)
            logs  = read_log_tail(avail) if avail > 0 else []
            for line in logs:
                lo = line.lower()
                attr = (C_ERR  if "error" in lo
                   else C_WARN if "warning" in lo or "warn" in lo
                   else C_OK   if "info" in lo
                   else C_NORM)
                put(row, 1, line[:cols - 2], attr)
                row += 1
                if row >= rows - 1: break

        stdscr.refresh()
        time.sleep(1.0)

curses.wrapper(main)
PYEOF
}

# ── ENTRY POINT ───────────────────────────────────────────────────────────────

CMD="${1:-run}"

case "$CMD" in
    stop)
        banner
        stop_daemon
        ;;
    restart)
        banner
        stop_daemon
        sleep 1
        start_daemon
        attach_tui
        ;;
    status)
        banner
        show_status
        ;;
    logs)
        tail -f "$LOG_FILE"
        ;;
    watch)
        # Affiche uniquement les snapshots [Spool] en temps réel (1/s)
        banner
        info "Snapshots [Spool] en direct (Ctrl-C pour quitter)..."
        echo ""
        tail -f "$LOG_FILE" | grep --line-buffered "\[Spool\]"
        ;;
    run|"")
        banner
        start_daemon
        attach_tui
        ;;
    *)
        echo "Usage: $0 [run|stop|restart|status|logs|watch]"
        exit 1
        ;;
esac
