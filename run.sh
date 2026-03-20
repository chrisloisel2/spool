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
import sys, os, time, curses, sqlite3, datetime as dt, re

LOG_FILE       = "/srv/exoria/logs/spool.log"
DB_PATH        = "/srv/exoria/queue.db"
INBOX_DIR      = "/srv/exoria/inbox"
PID_FILE       = "/tmp/spool_daemon.pid"
WORKERS        = 16
NAS_HOST       = "192.168.88.82"
SESSION_PAT    = re.compile(r'^session_\d{8}_\d{6}$')
LOG_TAIL_BYTES = 48000

# ── helpers ──────────────────────────────────────────────────────────────────

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

def _fmt_uptime(elapsed):
    h = int(elapsed // 3600)
    m = int((elapsed % 3600) // 60)
    s = int(elapsed % 60)
    if h: return f"{h}h{m:02d}m"
    return f"{m}m{s:02d}s"

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

# ── données DB ───────────────────────────────────────────────────────────────

def db_stats():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=2)
        rows = conn.execute(
            "SELECT status, COUNT(*), SUM(size_bytes) FROM jobs GROUP BY status"
        ).fetchall()
        proc_rows = conn.execute(
            "SELECT session_id, size_bytes, file_count FROM jobs "
            "WHERE status='processing' ORDER BY updated_at DESC LIMIT 16"
        ).fetchall()
        conn.close()
        counts, sizes = {}, {}
        for st, n, b in rows:
            counts[st] = n
            sizes[st] = b or 0
        queued      = counts.get("queued", 0)
        processing  = counts.get("processing", 0)
        done        = counts.get("done", 0)
        failed      = counts.get("failed", 0)
        remaining_b = sizes.get("queued", 0) + sizes.get("processing", 0)
        total_b     = sizes.get("queued", 0) + sizes.get("processing", 0) + sizes.get("done", 0)
        sent_b      = sizes.get("done", 0)
        return dict(queued=queued, processing=processing, done=done, failed=failed,
                    total_b=total_b, sent_b=sent_b, remaining_b=remaining_b,
                    proc_rows=proc_rows)
    except Exception:
        return dict(queued=0, processing=0, done=0, failed=0,
                    total_b=0, sent_b=0, remaining_b=0, proc_rows=[])

def inbox_count():
    try:
        return sum(1 for e in os.listdir(INBOX_DIR) if SESSION_PAT.match(e))
    except Exception:
        return -1

# ── lecture log ──────────────────────────────────────────────────────────────

def read_log_raw(n_bytes=LOG_TAIL_BYTES):
    try:
        size = os.path.getsize(LOG_FILE)
        with open(LOG_FILE, "rb") as f:
            f.seek(max(0, size - n_bytes))
            return f.read().decode("utf-8", errors="replace").splitlines()
    except Exception as e:
        return [f"[erreur log: {e}]"]

def parse_worker_states(lines):
    """Extrait l'état des workers NAS depuis le log."""
    workers = {}
    pat  = re.compile(r'\[JOB ([0-9a-f]+)\]\s+(\d+)/(\d+)\s+\'([^\']+)\'\s+@\s+([\d.]+)\s+MB/s')
    pat2 = re.compile(r"\[JOB ([0-9a-f]+)\] Session '([^']+)'")
    for line in lines:
        m = pat.search(line)
        if m:
            jid, done, total, fname, spd = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4), float(m.group(5))
            workers[jid] = {"files_done": done, "file_count": total, "current_file": fname, "speed_mbps": spd}
    for line in lines:
        m = pat2.search(line)
        if m:
            jid, sid = m.group(1), m.group(2)
            if jid in workers:
                workers[jid]["session_id"] = sid
    result = {}
    for jid, w in workers.items():
        sid = w.get("session_id", jid[:8])
        result[sid] = w
    return result

def read_log_speed(lines):
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

def parse_scanner_activity(lines):
    """Derniers QC pass/fail/inbox du scanner."""
    result = []
    pat = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \S+ \S+ (.*)')
    for line in reversed(lines):
        lo = line.lower()
        if any(k in lo for k in ["qc ", "scanner", "inbox", "session mise en file", "quality"]):
            m = pat.match(line)
            if m:
                ts, msg = m.group(1)[11:], m.group(2)  # HH:MM:SS only
                result.append((ts, msg))
            if len(result) >= 8:
                break
    return list(reversed(result))

def read_log_tail_filtered(n):
    lines = read_log_raw()
    filtered = [l for l in lines if l.strip() and " DEBUG " not in l and "spool-reporter" not in l]
    return filtered[-n:]

# ── TUI ──────────────────────────────────────────────────────────────────────

def main(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_CYAN,    -1)  # title
    curses.init_pair(2, curses.COLOR_GREEN,   -1)  # ok
    curses.init_pair(3, curses.COLOR_YELLOW,  -1)  # warn
    curses.init_pair(4, curses.COLOR_RED,     -1)  # err
    curses.init_pair(5, curses.COLOR_MAGENTA, -1)  # stat
    curses.init_pair(6, curses.COLOR_WHITE,   -1)  # norm
    curses.init_pair(7, curses.COLOR_BLACK,   curses.COLOR_CYAN)  # hdr bg

    C_TITLE = curses.color_pair(1) | curses.A_BOLD
    C_OK    = curses.color_pair(2)
    C_WARN  = curses.color_pair(3)
    C_ERR   = curses.color_pair(4)
    C_STAT  = curses.color_pair(5) | curses.A_BOLD
    C_NORM  = curses.color_pair(6)
    C_HDR   = curses.color_pair(7)
    C_DIM   = curses.color_pair(6) | curses.A_DIM

    daemon_start = None
    try:
        with open(LOG_FILE, "rb") as f:
            header = f.read(300).decode("utf-8", errors="replace")
        first_lines = [l for l in header.splitlines() if l.strip()]
        if first_lines:
            daemon_start = dt.datetime.strptime(first_lines[0][:19], "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        daemon_start = time.time()

    def put(r, c, text, attr=C_NORM):
        rows, cols = stdscr.getmaxyx()
        if r >= rows - 1 or c < 0 or c >= cols: return
        try: stdscr.addstr(r, c, str(text)[:max(0, cols - c)], attr)
        except curses.error: pass

    def hline(r, char="─", attr=C_NORM):
        rows, cols = stdscr.getmaxyx()
        if r >= rows - 1: return
        try: stdscr.addstr(r, 0, char * cols, attr)
        except curses.error: pass

    tick = 0
    while True:
        key = stdscr.getch()
        if key == ord('q'): break

        stdscr.erase()
        rows, cols = stdscr.getmaxyx()

        pid        = is_running()
        st         = db_stats()
        lines      = read_log_raw()
        speed_mbps = read_log_speed(lines)
        inbox_n    = inbox_count()

        elapsed    = time.time() - (daemon_start or time.time())
        uptime_str = _fmt_uptime(elapsed)
        eta_str    = _fmt_eta(st["remaining_b"], speed_mbps)
        total_b    = st["total_b"]
        sent_b     = st["sent_b"]
        global_pct = sent_b / max(total_b, 1) * 100

        row = 0

        # ── HEADER ───────────────────────────────────────────────────────────
        status_str = f"PID {pid}" if pid else "ARRÊTÉ"
        hdr_attr   = C_HDR if pid else C_ERR | curses.A_BOLD
        hdr = f" ⚡ SPOOL  ·  {WORKERS} workers  ·  NAS {NAS_HOST}  ·  {status_str}  ·  up {uptime_str} "
        put(row, 0, hdr.ljust(cols)[:cols], hdr_attr); row += 1

        # ── COMPTEURS ────────────────────────────────────────────────────────
        hline(row); row += 1
        c0, c1, c2, c3, c4 = 2, 20, 38, 54, 70
        put(row, c0, f"✓ done",    C_DIM);  put(row, c0+8,  f"{st['done']:>7}",    C_OK)
        put(row, c1, f"⧖ queue",   C_DIM);  put(row, c1+8,  f"{st['queued']:>6}",   C_WARN)
        put(row, c2, f"⚙ actifs",  C_DIM);  put(row, c2+8,  f"{st['processing']:>4}", C_STAT)
        put(row, c3, f"✗ échecs",  C_DIM);  put(row, c3+8,  f"{st['failed']:>6}",   C_ERR if st['failed'] else C_NORM)
        if inbox_n >= 0:
            put(row, c4, f"📥 inbox",  C_DIM); put(row, c4+8, f"{inbox_n:>6}", C_WARN if inbox_n > 0 else C_NORM)
        row += 1

        # ── BARRE PROGRESSION ─────────────────────────────────────────────────
        hline(row); row += 1
        bar_w = max(10, cols - 32)
        put(row, 2, "Envoi   ", C_TITLE)
        put(row, 10, _bar(global_pct, bar_w), C_OK if global_pct > 50 else C_WARN)
        put(row, 10 + bar_w + 1, f"{global_pct:5.1f}%", C_STAT)
        put(row, 10 + bar_w + 8, f"{_fmt_size(sent_b)} / {_fmt_size(total_b)}", C_DIM)
        row += 1

        # ── VITESSE + ETA ─────────────────────────────────────────────────────
        put(row, 2, "Vitesse ", C_TITLE)
        put(row, 10, f"{speed_mbps:6.1f} MB/s", C_STAT if speed_mbps > 0 else C_DIM)
        put(row, 26, f"restant {_fmt_size(st['remaining_b'])}", C_NORM)
        put(row, 48, f"ETA {eta_str}", C_WARN if eta_str != "--:--" else C_DIM)
        row += 1

        # ── SCANNER ───────────────────────────────────────────────────────────
        hline(row); row += 1
        put(row, 2, f"Scanner  —  inbox: {inbox_n if inbox_n >= 0 else '?'} sessions en attente", C_TITLE); row += 1
        hline(row, "╌"); row += 1

        scanner_lines = parse_scanner_activity(lines)
        if scanner_lines:
            for ts, msg in scanner_lines:
                if row >= rows - 8: break
                lo = msg.lower()
                attr = (C_ERR  if "fail" in lo or "error" in lo or "erreur" in lo
                   else C_WARN if "warn" in lo or "qc fail" in lo
                   else C_OK   if "pass" in lo or "mise en file" in lo
                   else C_DIM)
                put(row, 2, f"{ts}", C_DIM)
                put(row, 12, msg[:cols-14], attr)
                row += 1
        else:
            put(row, 4, "(aucune activité scanner dans le log récent)", C_DIM); row += 1

        # ── WORKERS NAS ───────────────────────────────────────────────────────
        hline(row); row += 1
        put(row, 2, f"Transferts NAS  {st['processing']}/{WORKERS} actifs", C_TITLE); row += 1
        hline(row, "╌"); row += 1

        worker_states = parse_worker_states(lines)
        bar_w2 = max(10, cols - 54)

        displayed = 0
        for sid, size_b, fc in st["proc_rows"]:
            if row >= rows - 5: break
            w     = worker_states.get(sid, {})
            fd    = w.get("files_done", 0)
            ft    = w.get("file_count", fc or 1)
            spd   = w.get("speed_mbps", 0.0)
            fname = w.get("current_file", "…")[-30:]
            pct   = fd / max(ft, 1) * 100
            label = sid[-20:].ljust(20)
            bar   = _bar(pct, bar_w2)
            put(row, 2,  label, C_NORM)
            put(row, 23, bar,   C_OK if pct > 50 else C_WARN)
            put(row, 23 + bar_w2 + 1, f"{pct:5.1f}%", C_STAT)
            put(row, 23 + bar_w2 + 8, f"{fd}/{ft}f  {spd:.1f}MB/s", C_DIM)
            row += 1
            put(row, 4, f"↳ {fname}", C_DIM); row += 1
            displayed += 1

        if displayed == 0:
            put(row, 4, "(aucun transfert actif)", C_DIM); row += 1

        # ── LOGS ─────────────────────────────────────────────────────────────
        if row < rows - 3:
            hline(row); row += 1
            put(row, 2, "Logs  (q = quitter)", C_TITLE); row += 1

            avail = max(0, rows - row - 1)
            if avail > 0:
                logs = read_log_tail_filtered(avail)
                for line in logs:
                    lo = line.lower()
                    attr = (C_ERR  if "error" in lo or "erreur" in lo
                       else C_WARN if "warning" in lo or "warn" in lo
                       else C_OK   if "info" in lo
                       else C_NORM)
                    # Trim timestamp prefix to save space: keep HH:MM:SS + rest
                    display = line
                    if len(line) > 23 and line[10] == ' ':
                        display = line[11:]  # remove date, keep time + rest
                    put(row, 1, display[:cols - 2], attr)
                    row += 1
                    if row >= rows - 1: break

        tick += 1
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
