#!/usr/bin/env bash
# run.sh — Lance spool comme service permanent, puis affiche le TUI live.
# Usage : ./run.sh [stop|restart|logs|status]
set -euo pipefail
set -x

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
    kill_all_instances

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

kill_all_instances() {
    # Tuer uniquement les processus spool.py (pas bash run.sh — trop risqué de se tuer soi-même)
    local spool_pids
    spool_pids=$(pgrep -f "python.*spool\.py" 2>/dev/null || true)

    # Aussi tuer le daemon enregistré dans le PID file s'il existe
    if [[ -f "$PID_FILE" ]]; then
        local saved_pid
        saved_pid=$(cat "$PID_FILE" 2>/dev/null || true)
        [[ -n "$saved_pid" ]] && spool_pids="$spool_pids $saved_pid"
    fi

    local to_kill
    to_kill=$(echo "$spool_pids" | tr ' ' '\n' | grep -v '^$' | sort -un | tr '\n' ' ')

    if [[ -n "$to_kill" ]]; then
        warn "Instances existantes détectées (pids: $to_kill) — arrêt..."
        kill $to_kill 2>/dev/null || true
        sleep 2
        for pid in $to_kill; do
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
        done
        ok "Processus arrêté"
    fi
    rm -f "$PID_FILE" 2>/dev/null || sudo rm -f "$PID_FILE" 2>/dev/null || true
}

stop_daemon() {
    kill_all_instances
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

def parse_log(lines):
    """
    Parse le log et retourne:
    - worker_state[worker_idx] : état courant de chaque worker 1..16
    - scanner_state            : état courant du scanner
    - recent_events            : derniers événements notables
    """
    # worker-N dans le thread name
    pat_thread = re.compile(r'\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2}),\d+ \S+ (\S+) (.*)')
    pat_job_prog = re.compile(r'\[JOB ([0-9a-f]+)\] (\d+)/(\d+) \'([^\']+)\' @ ([\d.]+) MB/s')
    pat_job_start = re.compile(r"\[JOB ([0-9a-f]+)\] Session '([^']+)' — (\d+) fichiers")
    pat_job_done  = re.compile(r"\[JOB ([0-9a-f]+)\] Session '([^']+)' envoyée en ([\d.]+)s @ ([\d.]+) MB/s")
    pat_job_fail  = re.compile(r"\[JOB ([0-9a-f]+)\] Echec tentative (\d+)/(\d+)")
    pat_qc        = re.compile(r"\[Scanner\] Quality check en cours : (\S+)")
    pat_qc_pass   = re.compile(r"\[Quality\] PASSED — session=(\S+) score=([\d.]+)")
    pat_qc_fail   = re.compile(r"\[Quality\] BLOCKED (\S+) — session=(\S+)")
    pat_qnas      = re.compile(r"\[Quarantine\] '([^']+)' → NAS OK")
    pat_qnas_fail = re.compile(r"\[Quarantine\] NAS échouée '([^']+)'")

    # jid → session_id mapping
    jid_to_sid = {}
    # worker name → idx
    w_name_idx = {f"worker-{i}": i for i in range(1, 17)}

    workers = {i: {"status": "idle", "session": "", "files_done": 0, "file_count": 0,
                   "speed": 0.0, "pct": 0.0, "file": "", "ts": ""} for i in range(1, 17)}
    scanner = {"status": "idle", "session": "", "qc_done": 0, "qc_fail": 0, "ts": ""}
    events  = []

    for line in lines:
        m = pat_thread.match(line)
        if not m:
            continue
        ts, tname, msg = m.group(1), m.group(2), m.group(3)

        # ── worker NAS ───────────────────────────────────────────────────────
        widx = w_name_idx.get(tname)
        if widx:
            w = workers[widx]
            m2 = pat_job_start.search(msg)
            if m2:
                jid_to_sid[m2.group(1)] = m2.group(2)
                w.update(status="upload", session=m2.group(2),
                         file_count=int(m2.group(3)), files_done=0,
                         speed=0.0, pct=0.0, file="", ts=ts)
            m2 = pat_job_prog.search(msg)
            if m2:
                jid = m2.group(1)
                sid = jid_to_sid.get(jid, w["session"])
                fd, ft, fname, spd = int(m2.group(2)), int(m2.group(3)), m2.group(4), float(m2.group(5))
                pct = fd / max(ft, 1) * 100
                w.update(status="upload", session=sid, files_done=fd,
                         file_count=ft, speed=spd, pct=pct,
                         file=os.path.basename(fname), ts=ts)
            m2 = pat_job_done.search(msg)
            if m2:
                sid, spd = m2.group(2), float(m2.group(4))
                w.update(status="idle", session="", files_done=0,
                         file_count=0, speed=0.0, pct=0.0, file="", ts=ts)
                events.append((ts, "done", f"✓ {sid[-22:]}  {spd:.1f} MB/s"))
            m2 = pat_job_fail.search(msg)
            if m2:
                att, mx = m2.group(2), m2.group(3)
                w.update(status="retry", ts=ts)
                if att == mx:
                    events.append((ts, "err", f"✗ fail {w['session'][-22:]}"))

        # ── scanner ──────────────────────────────────────────────────────────
        if "ThreadPoolExecutor" in tname or tname == "scanner" or tname.startswith("qc_"):
            m2 = pat_qc.search(msg)
            if m2:
                scanner.update(status="qc", session=m2.group(1)[-22:], ts=ts)
            m2 = pat_qc_pass.search(msg)
            if m2:
                scanner["qc_done"] += 1
                scanner.update(status="qc_pass", session=m2.group(1)[-22:], ts=ts)
                events.append((ts, "ok", f"QC✓ {m2.group(1)[-20:]}  score={float(m2.group(2)):.0f}"))
            m2 = pat_qc_fail.search(msg)
            if m2:
                scanner["qc_fail"] += 1
                scanner.update(status="qc_fail", session=m2.group(2)[-22:], ts=ts)
                events.append((ts, "warn", f"QC✗ {m2.group(2)[-20:]}  [{m2.group(1)}]"))

        # ── quarantaine ──────────────────────────────────────────────────────
        if "quarantine" in tname:
            m2 = pat_qnas.search(msg)
            if m2:
                events.append((ts, "warn", f"QNAS {m2.group(1)[-20:]}"))
            m2 = pat_qnas_fail.search(msg)
            if m2:
                events.append((ts, "err", f"QNAS✗ {m2.group(1)[-18:]} fallback"))

    return workers, scanner, events[-30:]

def total_speed(workers):
    return sum(w["speed"] for w in workers.values() if w["status"] == "upload")

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
        pid = int(open(PID_FILE).read().strip())
        # /proc/<pid>/stat field 22 = starttime en jiffies depuis boot
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
        jiffies = int(fields[21])
        hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        with open("/proc/uptime") as f:
            boot_uptime = float(f.read().split()[0])
        proc_uptime = boot_uptime - jiffies / hz
        daemon_start = time.time() - proc_uptime
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

    while True:
        key = stdscr.getch()
        if key == ord('q'): break

        stdscr.erase()
        rows, cols = stdscr.getmaxyx()

        pid     = is_running()
        st      = db_stats()
        inbox_n = inbox_count()
        lines   = read_log_raw(96000)
        workers, scanner, events = parse_log(lines)
        spd_total = total_speed(workers)

        elapsed    = time.time() - (daemon_start or time.time())
        uptime_str = _fmt_uptime(elapsed)
        sent_b     = st["sent_b"]
        total_b    = st["total_b"]
        global_pct = sent_b / max(total_b, 1) * 100
        eta_str    = _fmt_eta(st["remaining_b"], spd_total)

        row = 0

        # ══ HEADER ════════════════════════════════════════════════════════════
        status_str = f"PID {pid}" if pid else "ARRÊTÉ"
        hdr_attr   = C_HDR if pid else (C_ERR | curses.A_BOLD)
        hdr = (f" ⚡ SPOOL  ·  NAS {NAS_HOST}  ·  {status_str}  ·  up {uptime_str}"
               f"  ·  {spd_total:.1f} MB/s  ·  ETA {eta_str} ")
        put(row, 0, hdr.ljust(cols)[:cols], hdr_attr); row += 1

        # ══ STATS GLOBALES (1 ligne) ══════════════════════════════════════════
        bar_w = max(8, cols - 70)
        put(row, 1,  f"done",     C_DIM)
        put(row, 6,  f"{st['done']:>6}", C_OK)
        put(row, 14, f"queue",    C_DIM)
        put(row, 20, f"{st['queued']:>6}", C_WARN)
        put(row, 28, f"actifs",   C_DIM)
        put(row, 35, f"{st['processing']:>2}/{WORKERS}", C_STAT)
        put(row, 42, f"échecs",   C_DIM)
        put(row, 49, f"{st['failed']:>5}", C_ERR if st['failed'] else C_DIM)
        put(row, 56, f"inbox",    C_DIM)
        put(row, 62, f"{inbox_n if inbox_n >= 0 else '?':>6}", C_WARN if inbox_n > 0 else C_DIM)
        # mini barre globale à droite
        if cols > 72:
            put(row, 70, _bar(global_pct, bar_w), C_OK if global_pct > 50 else C_WARN)
            put(row, 70 + bar_w + 1, f"{global_pct:.0f}%", C_STAT)
        row += 1

        hline(row); row += 1

        # ══ GRILLE WORKERS ════════════════════════════════════════════════════
        # Titre
        put(row, 1, "W", C_DIM)
        put(row, 4, "Session", C_DIM)
        put(row, 28, "Progress", C_DIM)
        put(row, 28 + max(8, cols - 66), "Files", C_DIM)
        put(row, 28 + max(8, cols - 66) + 10, "Speed", C_DIM)
        put(row, 28 + max(8, cols - 66) + 20, "File", C_DIM)
        row += 1
        hline(row, "╌"); row += 1

        bar_w2 = max(8, cols - 66)
        workers_start_row = row

        for idx in range(1, WORKERS + 1):
            if row >= rows - 6: break
            w = workers[idx]
            st_w = w["status"]

            # couleur selon état
            if st_w == "upload":
                s_attr = C_OK
                status_ch = "▶"
            elif st_w == "retry":
                s_attr = C_WARN
                status_ch = "↺"
            elif st_w == "qc_pass":
                s_attr = C_OK
                status_ch = "✓"
            elif st_w == "qc_fail":
                s_attr = C_ERR
                status_ch = "✗"
            else:
                s_attr = C_DIM
                status_ch = "·"

            put(row, 1, f"{idx:>2}", C_DIM)
            put(row, 3, status_ch, s_attr)

            if st_w == "upload":
                sid_short = w["session"][-22:].ljust(22)
                bar       = _bar(w["pct"], bar_w2)
                files_str = f"{w['files_done']}/{w['file_count']}f"
                spd_str   = f"{w['speed']:5.1f}MB/s"
                file_str  = w["file"][:max(0, cols - 66 - bar_w2 - 4)]

                put(row, 5,  sid_short,                         C_NORM)
                put(row, 28, bar,                               C_OK if w["pct"] > 50 else C_WARN)
                put(row, 28 + bar_w2 + 1, f"{w['pct']:4.0f}%", C_STAT)
                put(row, 28 + bar_w2 + 6, files_str.ljust(10), C_DIM)
                put(row, 28 + bar_w2 + 16, spd_str,            C_STAT)
                put(row, 28 + bar_w2 + 24, file_str,           C_DIM)
            elif st_w == "idle":
                put(row, 5, "idle", C_DIM)
            elif st_w == "retry":
                put(row, 5, w["session"][-22:].ljust(22), C_WARN)
                put(row, 28, "en attente retry...", C_WARN)
            else:
                put(row, 5, w["session"][-22:] if w["session"] else "—", s_attr)

            row += 1

        # ══ SCANNER (1 ligne sous les workers) ════════════════════════════════
        hline(row, "╌"); row += 1
        sc = scanner
        sc_status = {"qc": C_WARN, "qc_pass": C_OK, "qc_fail": C_ERR, "idle": C_DIM}.get(sc["status"], C_DIM)
        sc_icon   = {"qc": "⟳", "qc_pass": "✓", "qc_fail": "✗", "idle": "·"}.get(sc["status"], "·")
        put(row, 1, "SC", C_DIM)
        put(row, 3, sc_icon, sc_status)
        put(row, 5, f"Scanner  QC:{sc['qc_done']} pass  {sc['qc_fail']} fail", C_DIM)
        if sc["session"]:
            put(row, 40, sc["session"][-30:], sc_status)
        if sc["ts"]:
            put(row, cols - 10, sc["ts"], C_DIM)
        row += 1

        # ══ EVENTS RÉCENTS ════════════════════════════════════════════════════
        hline(row); row += 1
        put(row, 1, "Événements récents  (q = quitter)", C_TITLE); row += 1

        avail = max(0, rows - row - 1)
        ev_attr = {"done": C_OK, "ok": C_OK, "warn": C_WARN, "err": C_ERR}
        for ts, kind, msg in reversed(events[-avail:]):
            if row >= rows - 1: break
            attr = ev_attr.get(kind, C_DIM)
            put(row, 1, ts, C_DIM)
            put(row, 10, msg[:cols - 12], attr)
            row += 1

        stdscr.refresh()
        time.sleep(1.0)

try:
    curses.wrapper(main)
except Exception as e:
    import traceback
    print(f"\n[TUI ERROR] {e}", flush=True)
    traceback.print_exc()
    input("Appuie sur Entrée pour quitter...")
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
    reset)
        banner
        warn "RESET COMPLET : arrêt du daemon + purge DB + remise en inbox des sessions spool/"
        echo ""

        # 1. Arrêt
        stop_daemon
        sleep 1

        # 2. Remet les sessions de spool/ dans inbox/ (sessions en cours interrompues)
        SPOOL_DIR="/srv/exoria/spool"
        INBOX_DIR="/srv/exoria/inbox"
        n_moved=0
        if [[ -d "$SPOOL_DIR" ]]; then
            for d in "$SPOOL_DIR"/session_*/; do
                [[ -d "$d" ]] || continue
                name=$(basename "$d")
                dst="$INBOX_DIR/$name"
                if [[ -d "$dst" ]]; then
                    warn "Conflit $name déjà dans inbox — suppression de la copie spool"
                    rm -rf "$d"
                else
                    mv "$d" "$dst"
                    (( n_moved++ )) || true
                fi
            done
        fi
        ok "$n_moved session(s) remise(s) dans inbox/"

        # 3. Purge de la DB
        DB="/srv/exoria/queue.db"
        if [[ -f "$DB" ]]; then
            rm -f "$DB" "${DB}-wal" "${DB}-shm"
            ok "Base de données supprimée"
        else
            info "Pas de DB à supprimer"
        fi

        # 4. Redémarrage propre
        echo ""
        info "Redémarrage..."
        sleep 1
        start_daemon
        attach_tui
        ;;
    run|"")
        banner
        start_daemon
        attach_tui
        ;;
    *)
        echo "Usage: $0 [run|stop|restart|status|logs|watch|reset]"
        exit 1
        ;;
esac
