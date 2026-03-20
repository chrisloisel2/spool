#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import uuid
import json
import shutil
import hashlib
import sqlite3
import logging
import argparse
import threading
import datetime as dt
import posixpath
import traceback
import random
import socket

import curses
import collections
import concurrent.futures

import paramiko

# =========================
# CONFIG HARDCODE
# =========================

INBOX_DIR = "/srv/exoria/inbox"
SPOOL_DIR = "/srv/exoria/spool"
QUARANTINE_DIR = "/srv/exoria/quarantine"
DB_PATH = "/srv/exoria/queue.db"

SCAN_INTERVAL = 1  # déjà à 1s — optimal
SCAN_QC_WORKERS = 8  # threads parallèles pour le quality check
SCAN_BATCH_SIZE = 50  # sessions traitées par batch (évite de bloquer des heures)

# Nombre de workers NAS en parallèle.
# 1 = tout dans le thread principal (pas de threads supplémentaires).
# >1 = scanner + N workers chacun dans leur propre thread.
WORKERS = 16  # TURBO: 16 connexions SFTP simultanées

MAX_RETRIES = 8
RETRY_BACKOFF = 1  # TURBO: backoff réduit à 1s

# NAS (destination finale)
NAS_HOST = "192.168.88.82"
NAS_PORT = 22
NAS_USER = "root"
NAS_PASS = "Exori@2026!"

SFTP_BASE_DIR = "/data/INBOX/bronze"
LANDING_ZONE = ""
QUARANTINE_ZONE = "/data/INBOX/quarantine"

DELETE_LOCAL_AFTER_SUCCESS = True
COPY_TO_NAS_QUARANTINE = True
STABLE_FILE_SECONDS = 0   # fichiers déjà complets à l'arrivée — pas d'attente
WRITE_MANIFEST = False     # désactivé pour vitesse max — une écriture NAS de moins par fichier

# =========================
# DURCISSEMENT SSH/SFTP (banner reset)
# =========================

# Nombre max de connexions NAS simultanées — TURBO: aligné sur WORKERS
NAS_MAX_SIMULT_CONNECT = 16
SSH_TIMEOUT = 20
BANNER_TIMEOUT = 90
AUTH_TIMEOUT = 30
KEEPALIVE_SEC = 30

# Tuning SFTP — TURBO: buffers max pour saturer le réseau
SFTP_WINDOW_SIZE  = 134217728   # 128 MB — fenêtre SSH max
SFTP_MAX_PACKET   = 65536        # 64 KB — paquet SFTP max
SFTP_READ_BUFFER  = 8388608      # TURBO: 8 MB — buffer lecture locale (x2)

# =========================
# CONFIG QUALITY CHECKS
# =========================

# Chemin vers le dossier quality_processus/ (relatif au script ou absolu)
QUALITY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quality_processus")

# Timeouts (secondes) par étape — les vidéos lourdes peuvent prendre du temps
QUALITY_TIMEOUT_FIX     = 60
QUALITY_TIMEOUT_FILES   = 30
QUALITY_TIMEOUT_SANITY  = 120   # ffprobe sur 3 vidéos
QUALITY_TIMEOUT_QUALITY = 300   # ffmpeg blurdetect — le plus long
QUALITY_TIMEOUT_NAMING  = 120   # OpenCV verify_naming

# Seuil de score qualité minimum pour accepter la session (0–100)
QUALITY_SCORE_MIN = 40

# Seuil de certitude nommage (0.0–1.0)
QUALITY_NAMING_MIN = 0.50

# Active/désactive les étapes optionnelles (fixes si False = skip silencieux)
QUALITY_RUN_FIX     = True   # tente de réparer les petits problèmes avant checks
QUALITY_RUN_QUALITY = True   # score vidéo ffmpeg (lent)
QUALITY_RUN_NAMING  = True   # verify_naming OpenCV (lent)

# =========================
# CONFIG KAFKA
# =========================

KAFKA_BROKER = "192.168.88.4"
KAFKA_BROKER_PORT = 9092
KAFKA_TOPIC = "monitoring"

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    KafkaProducer = None
    HAS_KAFKA = False

# =========================
# LOG (maximum)
# =========================

LOG_DIR  = "/srv/exoria/logs"
LOG_FILE = os.path.join(LOG_DIR, "spool.log")

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("spool")

# =========================
# DB
# =========================

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY,
 session_dir TEXT,          -- chemin absolu du dossier session dans spool/
 session_id TEXT,           -- ex: session_20260308_161838
 size_bytes INTEGER,
 file_count INTEGER,
 status TEXT,
 attempts INTEGER,
 last_error TEXT,
 created_at TEXT,
 updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);
"""

SESSION_RE = re.compile(r"^session_\d{8}_\d{6}$")

def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.executescript(SCHEMA)
    return conn

# =========================
# KAFKA PRODUCER
# =========================

class KafkaEmitter:
    def __init__(self):
        self._producer = None
        self._lock = threading.Lock()

    def _ensure(self):
        if not HAS_KAFKA:
            return
        if self._producer:
            return
        with self._lock:
            if self._producer:
                return
            self._producer = KafkaProducer(
                bootstrap_servers=[f"{KAFKA_BROKER}:{KAFKA_BROKER_PORT}"],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                api_version=(2, 0, 0),
                retries=10,
                acks="all",
                linger_ms=10,
                request_timeout_ms=15000,
            )

    def emit(self, step: str, status: str, **fields):
        # évite conflit avec les champs réservés
        if "step" in fields:
            fields["op"] = fields.pop("step")
        if "status" in fields:
            fields["op_status"] = fields.pop("status")
        if "ts" in fields:
            fields["op_ts"] = fields.pop("ts")

        ev = {
            "ts": time.time(),
            "step": step,
            "status": status,
            **fields,
        }

        log.debug("kafka_emit step=%s status=%s fields=%s", step, status, fields)

        if not HAS_KAFKA:
            return
        try:
            self._ensure()
            if not self._producer:
                return
            self._producer.send(KAFKA_TOPIC, ev)
            self._producer.flush(timeout=5)
        except Exception as e:
            log.warning("kafka emit failed: %s", e)

KAFKA = KafkaEmitter()

# =========================
# SPOOL REPORTER — snapshot périodique vers monitoring
# =========================

REPORTER_INTERVAL = 1  # secondes entre chaque snapshot

class SpoolReporter(threading.Thread):
    """
    Publie deux types de messages sur le topic monitoring toutes les REPORTER_INTERVAL secondes :

    1. source="pc"          — un message par pc_id actif (compatibilité SalleReporter back)
    2. source="spool_status" — snapshot complet du spool pour le front
    """

    def __init__(self):
        super().__init__(daemon=True, name="spool-reporter")
        self._lock = threading.Lock()
        self._inbound_queue = []
        self._processed_today = 0
        self._forwarded_to_nas = 0
        self._failed = 0
        self._current_transfer = None
        self._conn = None           # connexion DB injectée par main()
        self._start_ts = time.time()

    def set_db(self, conn):
        self._conn = conn

    def set_inbound_queue(self, entries: list):
        with self._lock:
            self._inbound_queue = list(entries)

    def set_current_transfer(self, info: dict | None):
        with self._lock:
            self._current_transfer = info

    def inc_processed(self):
        with self._lock:
            self._processed_today += 1

    def inc_forwarded(self):
        with self._lock:
            self._forwarded_to_nas += 1

    def inc_failed(self):
        with self._lock:
            self._failed += 1

    def run(self):
        log.info("[Reporter] Démarrage — intervalle=%ds topic=%s", REPORTER_INTERVAL, KAFKA_TOPIC)
        while True:
            try:
                self._emit()
            except Exception as e:
                log.warning("[Reporter] Erreur lors de l'émission : %s", e)
            time.sleep(REPORTER_INTERVAL)

    def _db_stats(self) -> dict:
        """Requête SQLite pour les compteurs globaux et les jobs récents."""
        if not self._conn:
            return {}
        try:
            # Compteurs par statut
            rows = self._conn.execute(
                "SELECT status, COUNT(*) as n, SUM(size_bytes) as total_bytes "
                "FROM jobs GROUP BY status"
            ).fetchall()
            counts = {}
            sizes  = {}
            for status, n, total in rows:
                counts[status] = n
                sizes[status]  = total or 0

            total_jobs = sum(counts.values())
            n_done      = counts.get("done", 0)
            n_failed    = counts.get("failed", 0)
            n_queued    = counts.get("queued", 0)
            n_processing = counts.get("processing", 0)

            fail_pct = round(n_failed / total_jobs * 100, 1) if total_jobs else 0.0

            # Taille totale des fichiers en attente (queued)
            queued_bytes = sizes.get("queued", 0)

            # 5 derniers jobs failed avec leur erreur
            failed_jobs = self._conn.execute(
                "SELECT id, session_id, size_bytes, attempts, last_error, updated_at "
                "FROM jobs WHERE status='failed' ORDER BY updated_at DESC LIMIT 5"
            ).fetchall()
            recent_failed = [
                {
                    "job_id":    r[0][:8],
                    "session":   r[1],
                    "size_mb":   round((r[2] or 0) / (1024*1024), 2),
                    "attempts":  r[3],
                    "error":     (r[4] or "")[:120],
                    "failed_at": r[5],
                }
                for r in failed_jobs
            ]

            # 5 derniers jobs done
            done_jobs = self._conn.execute(
                "SELECT id, session_id, size_bytes, updated_at "
                "FROM jobs WHERE status='done' ORDER BY updated_at DESC LIMIT 5"
            ).fetchall()
            recent_done = [
                {
                    "job_id":       r[0][:8],
                    "session":      r[1],
                    "size_mb":      round((r[2] or 0) / (1024*1024), 2),
                    "completed_at": r[3],
                }
                for r in done_jobs
            ]

            return {
                "total_jobs":      total_jobs,
                "queued":          n_queued,
                "processing":      n_processing,
                "done":            n_done,
                "failed":          n_failed,
                "fail_pct":        fail_pct,
                "queued_bytes":    queued_bytes,
                "queued_mb":       round(queued_bytes / (1024*1024), 2),
                "recent_failed":   recent_failed,
                "recent_done":     recent_done,
            }
        except Exception as e:
            log.debug("[Reporter] db_stats error: %s", e)
            return {}

    def _disk_usage(self) -> dict:
        """Utilisation disque des dossiers spool/inbox/quarantine."""
        result = {}
        for label, path in [("inbox", INBOX_DIR), ("spool", SPOOL_DIR), ("quarantine", QUARANTINE_DIR)]:
            try:
                total, used, free = shutil.disk_usage(path)
                result[label] = {
                    "used_mb":  round(used  / (1024*1024), 1),
                    "free_mb":  round(free  / (1024*1024), 1),
                    "total_mb": round(total / (1024*1024), 1),
                    "used_pct": round(used / total * 100, 1) if total else 0.0,
                }
            except Exception:
                result[label] = None
        return result

    def _emit(self):
        with self._lock:
            queue    = list(self._inbound_queue)
            transfer = self._current_transfer
            failed   = self._failed
            processed = self._processed_today
            forwarded = self._forwarded_to_nas

        db    = self._db_stats()
        disk  = self._disk_usage()
        uptime_s = int(time.time() - self._start_ts)

        # ── Log périodique (1/s) — lisible dans spool.log / run.sh logs ─────────
        n_queued     = db.get("queued", len(queue))
        n_processing = db.get("processing", 0)
        n_done       = db.get("done", 0)
        n_failed_db  = db.get("failed", 0)
        queued_mb    = db.get("queued_mb", 0.0)
        xfer = transfer or {}
        xfer_str = (
            f"  xfer={xfer.get('session_id','?')} {xfer.get('progress_pct',0):.0f}%"
            f" @ {xfer.get('speed_mbps',0):.1f} MB/s"
            if xfer else "  xfer=idle"
        )
        up_h, up_r  = divmod(uptime_s, 3600)
        up_m, up_s_ = divmod(up_r, 60)
        log.info(
            "[Spool] up=%02dh%02dm%02ds  queued=%d (%.1f MB)  processing=%d"
            "  done=%d  failed=%d%s",
            up_h, up_m, up_s_,
            n_queued, queued_mb, n_processing,
            n_done, n_failed_db, xfer_str,
        )

        if not HAS_KAFKA:
            return

        try:
            KAFKA._ensure()
            if not KAFKA._producer:
                return

            # ── 1. Messages source="pc" (un par pc_id actif) ─────────────────
            pc_ids_seen  = set()
            entries_by_pc = {}
            for entry in queue:
                pc_id = int(entry.get("pc_id", 0))
                if pc_id:
                    entries_by_pc.setdefault(pc_id, []).append(entry)
                    pc_ids_seen.add(pc_id)
            if transfer and transfer.get("from_pc"):
                pc_ids_seen.add(int(transfer["from_pc"]))

            for pc_id in pc_ids_seen:
                pc_queue = entries_by_pc.get(pc_id, [])
                is_xfer  = bool(transfer and int(transfer.get("from_pc", 0)) == pc_id)
                KAFKA._producer.send(KAFKA_TOPIC, {
                    "source":            "pc",
                    "pc_id":             pc_id,
                    "hostname":          f"PC-{pc_id:05d}",
                    "operator_username": None,
                    "is_recording":      False,
                    "has_alert":         failed > 0,
                    "sqlite_queue":      len(pc_queue),
                    "last_send":         pc_queue[0].get("received_at") if pc_queue else None,
                    "disconnected":      False,
                    "current_transfer":  transfer if is_xfer else None,
                })

            # ── 2. Snapshot spool complet ─────────────────────────────────────
            KAFKA._producer.send(KAFKA_TOPIC, {
                "source":    "spool_status",
                "ts":        time.time(),
                "uptime_s":  uptime_s,

                # File d'attente
                "queue": {
                    "count":      db.get("queued", len(queue)),
                    "total_mb":   db.get("queued_mb", 0.0),
                    "entries":    queue,          # liste détaillée {pc_id, session_id, received_at, size_mb}
                },

                # Transfert en cours
                "current_transfer": transfer,     # None ou {from_pc, session_id, progress_pct, speed_mbps}

                # Compteurs de session (session courante depuis démarrage)
                "stats": {
                    "processed_today":  processed,
                    "forwarded_to_nas": forwarded,
                    "failed_session":   failed,
                    # Depuis la DB (all-time)
                    "total_jobs":       db.get("total_jobs", 0),
                    "done":             db.get("done", 0),
                    "failed_total":     db.get("failed", 0),
                    "processing":       db.get("processing", 0),
                    "fail_pct":         db.get("fail_pct", 0.0),   # % échec sur total
                },

                # Disque
                "disk": disk,

                # Historique jobs récents
                "recent_failed": db.get("recent_failed", []),
                "recent_done":   db.get("recent_done", []),

                # Config courante
                "config": {
                    "workers":      WORKERS,
                    "max_retries":  MAX_RETRIES,
                    "scan_interval_s": SCAN_INTERVAL,
                    "nas_host":     NAS_HOST,
                    "nas_port":     NAS_PORT,
                },
            })

            KAFKA._producer.flush(timeout=5)
        except Exception as e:
            log.warning("[Reporter] Kafka send failed : %s", e)


REPORTER = SpoolReporter()

# =========================
# TUI — Terminal UI (curses)
# =========================

# Shared state updated by workers & scanner, read by TUI thread
_TUI_LOCK = threading.Lock()
_tui_active_transfers: dict = {}   # worker_idx -> {session_id, files_done, file_count, bytes_sent, total_bytes, speed_mbps, t_start}
_tui_done_count    = 0
_tui_failed_count  = 0
_tui_queued_count  = 0
_tui_total_bytes_session = 0       # bytes total de tous les jobs au démarrage
_tui_bytes_sent_total    = 0       # bytes effectivement envoyés depuis démarrage
_tui_speed_history: collections.deque = collections.deque(maxlen=10)  # moyennes glissantes MB/s
_tui_log_lines: collections.deque = collections.deque(maxlen=200)     # derniers messages log
_tui_start_ts  = time.monotonic()
_tui_enabled   = True

def tui_update_transfer(worker_idx: int, info: dict | None):
    global _tui_active_transfers
    with _TUI_LOCK:
        if info is None:
            _tui_active_transfers.pop(worker_idx, None)
        else:
            _tui_active_transfers[worker_idx] = info

def tui_inc_done(bytes_sent: int):
    global _tui_done_count, _tui_bytes_sent_total
    with _TUI_LOCK:
        _tui_done_count += 1
        _tui_bytes_sent_total += bytes_sent

def tui_inc_failed():
    global _tui_failed_count
    with _TUI_LOCK:
        _tui_failed_count += 1

def tui_set_queue(n: int, total_bytes: int):
    global _tui_queued_count, _tui_total_bytes_session
    with _TUI_LOCK:
        _tui_queued_count = n
        _tui_total_bytes_session = total_bytes

def tui_push_speed(mbps: float):
    with _TUI_LOCK:
        _tui_speed_history.append(mbps)

def tui_log(msg: str):
    with _TUI_LOCK:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        _tui_log_lines.append(f"{ts}  {msg}")


def _fmt_size(b: int) -> str:
    if b >= 1 << 30:
        return f"{b / (1<<30):.1f} GB"
    if b >= 1 << 20:
        return f"{b / (1<<20):.1f} MB"
    if b >= 1 << 10:
        return f"{b / (1<<10):.0f} KB"
    return f"{b} B"

def _fmt_eta(remaining_bytes: int, speed_bps: float) -> str:
    if speed_bps <= 0 or remaining_bytes <= 0:
        return "--:--"
    secs = int(remaining_bytes / speed_bps)
    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"

def _bar(pct: float, width: int = 20, filled="█", empty="░") -> str:
    n = int(pct / 100 * width)
    n = max(0, min(n, width))
    return filled * n + empty * (width - n)


class SpoolTUI(threading.Thread):
    REFRESH_HZ = 4   # redraws per second

    def __init__(self):
        super().__init__(daemon=True, name="tui")

    def run(self):
        try:
            curses.wrapper(self._loop)
        except Exception:
            pass  # si le terminal ne supporte pas curses, on ignore silencieusement

    def _loop(self, stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        curses.start_color()
        curses.use_default_colors()

        # Paires de couleurs
        curses.init_pair(1, curses.COLOR_CYAN,    -1)   # titre / header
        curses.init_pair(2, curses.COLOR_GREEN,   -1)   # succès / done
        curses.init_pair(3, curses.COLOR_YELLOW,  -1)   # en cours / warning
        curses.init_pair(4, curses.COLOR_RED,     -1)   # erreur / failed
        curses.init_pair(5, curses.COLOR_MAGENTA, -1)   # vitesse / stats
        curses.init_pair(6, curses.COLOR_WHITE,   -1)   # texte normal
        curses.init_pair(7, curses.COLOR_BLACK,   curses.COLOR_CYAN)  # header bar

        C_TITLE  = curses.color_pair(1) | curses.A_BOLD
        C_OK     = curses.color_pair(2)
        C_WARN   = curses.color_pair(3)
        C_ERR    = curses.color_pair(4)
        C_STAT   = curses.color_pair(5) | curses.A_BOLD
        C_NORM   = curses.color_pair(6)
        C_HDR    = curses.color_pair(7)

        interval = 1.0 / self.REFRESH_HZ

        while True:
            key = stdscr.getch()
            if key == ord('q'):
                break

            stdscr.erase()
            rows, cols = stdscr.getmaxyx()

            # Snapshot thread-safe
            with _TUI_LOCK:
                active   = dict(_tui_active_transfers)
                done     = _tui_done_count
                failed   = _tui_failed_count
                queued   = _tui_queued_count
                total_b  = _tui_total_bytes_session
                sent_b   = _tui_bytes_sent_total
                speeds   = list(_tui_speed_history)
                logs     = list(_tui_log_lines)

            elapsed   = time.monotonic() - _tui_start_ts
            avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
            # vitesse instantanée = somme des workers actifs
            live_speed = sum(t.get("speed_mbps", 0) for t in active.values())
            speed_bps  = live_speed * (1 << 20)

            # bytes en cours (workers actifs)
            active_bytes_sent  = sum(t.get("bytes_sent", 0) for t in active.values())
            active_bytes_total = sum(t.get("total_bytes", 1) for t in active.values())

            # ETA globale : bytes restants = (total session - déjà envoyé - en cours)
            global_remaining = max(0, total_b - sent_b - active_bytes_sent)
            eta_str = _fmt_eta(global_remaining, speed_bps)

            # Progression globale
            global_pct = (sent_b + active_bytes_sent) / max(total_b, 1) * 100

            row = 0

            def put(r, c, text, attr=C_NORM):
                if r >= rows - 1 or c >= cols:
                    return
                try:
                    stdscr.addstr(r, c, text[:cols - c], attr)
                except curses.error:
                    pass

            def hline(r, char="─", attr=C_NORM):
                put(r, 0, char * cols, attr)

            # ── HEADER ───────────────────────────────────────────────────────
            header = f" ⚡ SPOOL TURBO  ·  {WORKERS} workers  ·  NAS {NAS_HOST} "
            header = header.ljust(cols)
            put(row, 0, header[:cols], C_HDR)
            row += 1

            # ── STATS GLOBALES ───────────────────────────────────────────────
            hline(row, "─"); row += 1

            done_s    = f"✓ {done} envoyées"
            queued_s  = f"⧖ {queued} en attente"
            failed_s  = f"✗ {failed} échecs"
            active_s  = f"⇢ {len(active)} actifs"
            uptime_s  = f"up {int(elapsed//3600):02d}h{int((elapsed%3600)//60):02d}m"

            put(row, 2,  done_s,   C_OK);   put(row, 22, queued_s, C_WARN)
            put(row, 42, failed_s, C_ERR);  put(row, 60, active_s, C_STAT)
            put(row, 76, uptime_s, C_NORM)
            row += 1

            # ── BARRE PROGRESSION GLOBALE ────────────────────────────────────
            hline(row, "─"); row += 1
            bar_w = max(10, cols - 30)
            gbar = _bar(global_pct, bar_w)
            put(row, 2, f"Global  ", C_TITLE)
            put(row, 10, gbar, C_OK if global_pct > 50 else C_WARN)
            put(row, 10 + bar_w + 1, f"{global_pct:5.1f}%", C_STAT)
            put(row, 10 + bar_w + 8, f"  {_fmt_size(sent_b + active_bytes_sent)} / {_fmt_size(total_b)}", C_NORM)
            row += 1

            # Vitesse + ETA
            put(row, 2, f"Vitesse  ", C_TITLE)
            put(row, 11, f"{live_speed:6.1f} MB/s", C_STAT)
            put(row, 25, f"  moy {avg_speed:.1f} MB/s", C_NORM)
            put(row, 44, f"  ETA  {eta_str}", C_WARN if eta_str != "--:--" else C_NORM)
            row += 1

            # ── TRANSFERTS ACTIFS ────────────────────────────────────────────
            hline(row, "─"); row += 1
            put(row, 2, f"Transferts actifs ({len(active)})", C_TITLE); row += 1
            hline(row, "╌"); row += 1

            bar_w2 = max(10, cols - 55)
            for widx in sorted(active.keys()):
                if row >= rows - 6:
                    put(row, 4, f"  … {len(active) - (widx - min(active.keys()))} de plus …", C_NORM)
                    row += 1
                    break
                t      = active[widx]
                sid    = t.get("session_id", "?")[-22:]
                fd     = t.get("files_done", 0)
                fc     = t.get("file_count", 1)
                bs     = t.get("bytes_sent", 0)
                bt     = t.get("total_bytes", 1)
                spd    = t.get("speed_mbps", 0.0)
                pct    = bs / max(bt, 1) * 100
                tbar   = _bar(pct, bar_w2)
                label  = f"W{widx:02d} {sid}"
                label  = label[:24].ljust(24)
                put(row, 2, label, C_NORM)
                put(row, 27, tbar, C_OK if pct > 50 else C_WARN)
                put(row, 27 + bar_w2 + 1, f"{pct:5.1f}%", C_STAT)
                put(row, 27 + bar_w2 + 8, f" {fd}/{fc}f  {spd:.1f}MB/s", C_NORM)
                row += 1

            if not active:
                put(row, 4, "  (aucun transfert en cours)", C_NORM); row += 1

            # ── LOG ──────────────────────────────────────────────────────────
            hline(row, "─"); row += 1
            put(row, 2, "Logs récents  (q = quitter)", C_TITLE); row += 1
            hline(row, "╌"); row += 1

            log_lines_avail = rows - row - 1
            visible = logs[-log_lines_avail:] if log_lines_avail > 0 else []
            for line in visible:
                attr = C_ERR if "ERR" in line or "échec" in line.lower() or "failed" in line.lower() \
                      else C_WARN if "WARN" in line or "retry" in line.lower() \
                      else C_OK if "done" in line.lower() or "envoyée" in line.lower() \
                      else C_NORM
                put(row, 2, line[:cols - 3], attr)
                row += 1
                if row >= rows - 1:
                    break

            stdscr.refresh()
            time.sleep(interval)


TUI = SpoolTUI()

# Hook dans le logger pour alimenter le panneau logs TUI
class _TuiLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            # ligne courte pour le TUI
            short = msg.split("%(")[-1] if "%(" in msg else msg
            tui_log(self.format(record))
        except Exception:
            pass

_tui_handler = _TuiLogHandler()
_tui_handler.setLevel(logging.INFO)
_tui_handler.setFormatter(logging.Formatter("%(levelname)s %(threadName)s %(message)s"))
log.addHandler(_tui_handler)


# =========================
# UTILS
# =========================

SENDER_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")

def safe_sender(v):
    if SENDER_RE.match(v):
        return v
    return "unknown"

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb", buffering=SFTP_READ_BUFFER) as f:
        for chunk in iter(lambda: f.read(SFTP_READ_BUFFER), b""):
            h.update(chunk)
    return h.hexdigest()

def now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def stable(path):
    if STABLE_FILE_SECONDS == 0:
        return os.path.exists(path)
    try:
        s1 = os.stat(path)
        time.sleep(STABLE_FILE_SECONDS)
        s2 = os.stat(path)
        ok = (s1.st_size == s2.st_size) and (s1.st_mtime == s2.st_mtime)
        log.debug("stable_check path=%s size1=%d size2=%d ok=%s", path, s1.st_size, s2.st_size, ok)
        return ok
    except Exception as e:
        log.debug("stable_check error path=%s err=%s", path, e)
        return False

def tcp_check(host: str, port: int, timeout_s: float = 3.0) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_s)
    rc = s.connect_ex((host, port))
    s.close()
    if rc != 0:
        raise ConnectionError(f"TCP {host}:{port} unreachable rc={rc}")

# =========================
# NAS SFTP — connexion persistante + limite de concurrence
# =========================

NAS_CONNECT_SEM = threading.Semaphore(NAS_MAX_SIMULT_CONNECT)

class NASClient:
    def __init__(self):
        self.ssh = None
        self.sftp = None
        self._lock = threading.Lock()

    def is_alive(self) -> bool:
        try:
            if not self.ssh or not self.sftp:
                return False
            t = self.ssh.get_transport()
            return bool(t and t.is_active())
        except Exception:
            return False

    def connect(self) -> None:
        with self._lock:
            if self.is_alive():
                return

            log.info("[NAS] Connexion au NAS %s:%s (utilisateur: %s)...", NAS_HOST, NAS_PORT, NAS_USER)

            NAS_CONNECT_SEM.acquire()
            try:
                tcp_check(NAS_HOST, NAS_PORT, 3.0)

                try:
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(
                        hostname=NAS_HOST,
                        port=NAS_PORT,
                        username=NAS_USER,
                        password=NAS_PASS,
                        look_for_keys=False,
                        allow_agent=False,
                        timeout=SSH_TIMEOUT,
                        banner_timeout=BANNER_TIMEOUT,
                        auth_timeout=AUTH_TIMEOUT,
                    )

                    try:
                        tr = ssh.get_transport()
                        if tr:
                            tr.set_keepalive(KEEPALIVE_SEC)
                    except Exception:
                        pass

                    self.ssh = ssh
                    self.sftp = ssh.open_sftp()
                    self.sftp.get_channel().in_window_size = SFTP_WINDOW_SIZE
                    self.sftp.get_channel().out_window_size = SFTP_WINDOW_SIZE

                    log.info("[NAS] Connexion établie avec succès.")

                except (paramiko.SSHException, ConnectionResetError, EOFError, OSError) as e:
                    try:
                        if self.ssh:
                            self.ssh.close()
                    except Exception:
                        pass
                    self.ssh = None
                    self.sftp = None
                    raise RuntimeError(f"NAS connect failed: {e}")

            except Exception as e:
                log.error("[NAS] Impossible de se connecter au NAS après 5 tentatives : %s\n%s", e, traceback.format_exc())
                NAS_CONNECT_SEM.release()
                raise

    def close(self) -> None:
        with self._lock:
            try:
                if self.sftp:
                    try:
                        self.sftp.close()
                    except Exception:
                        pass
            finally:
                if self.ssh:
                    try:
                        self.ssh.close()
                    except Exception:
                        pass
            self.ssh = None
            self.sftp = None
            try:
                NAS_CONNECT_SEM.release()
            except Exception:
                pass
            log.debug("nas_closed")

    def ensure(self) -> None:
        if not self.is_alive():
            self.connect()

    def exists(self, path: str) -> bool:
        try:
            self.sftp.stat(path)
            return True
        except Exception:
            return False

    def mkdir_p(self, remote: str) -> None:
        remote = remote.replace("\\", "/")
        parts = remote.split("/")
        cur = ""
        created = 0
        for p in parts:
            if not p:
                continue
            cur += "/" + p
            if not self.exists(cur):
                self.sftp.mkdir(cur)
                created += 1
                log.debug("nas_mkdir %s", cur)
        log.debug("nas_mkdir_p remote=%s created=%d", remote, created)

    def put_atomic(self, local: str, remote: str) -> str:
        remote = remote.replace("\\", "/")
        directory = posixpath.dirname(remote)
        self.mkdir_p(directory)

        size = os.path.getsize(local)
        tmp = remote + ".part"
        log.info("[NAS] Envoi '%s' (%d octets)...", os.path.basename(local), size)

        # putfo avec buffer 4 MB — bien plus rapide que sftp.put (évite les micro-lectures)
        with open(local, "rb", buffering=SFTP_READ_BUFFER) as fh:
            self.sftp.putfo(fh, tmp, file_size=size)

        log.info("[NAS] Transfert OK '%s'", os.path.basename(local))

        final = remote
        if self.exists(final):
            base = final
            i = 1
            while self.exists(f"{base}.dup{i}"):
                i += 1
            final = f"{base}.dup{i}"
            log.warning("[NAS] Doublon '%s' -> '%s'", os.path.basename(base), os.path.basename(final))

        self.sftp.rename(tmp, final)
        log.info("[NAS] Disponible : %s", final)
        return final

# =========================
# QUALITY CHECKER
# =========================

class QualityResult:
    """Résultat d'un quality check sur une session."""
    def __init__(self, passed: bool, label: str, score: float, errors: list, details: dict):
        self.passed  = passed
        self.label   = label    # "ok" | "blocked_files" | "blocked_sanity" | "low_quality" | "bad_naming"
        self.score   = score    # 0–100 (score vidéo moyen)
        self.errors  = errors   # liste de strings d'erreur
        self.details = details  # dict JSON-serialisable

    def __repr__(self):
        return f"QualityResult(passed={self.passed}, label={self.label!r}, score={self.score:.1f})"


class QualityChecker:
    """
    Encapsule les 4 étapes du pipeline qualité :
      0. fix_shit.sh  — corrections auto (JSONL tronqué, metadata manquant, …)
      1. files.sh     — présence de tous les fichiers requis
      2. sanity.sh    — cohérence metadata.json + streams vidéo + CSV
      3. quality.sh   — score visuel /100 (ffmpeg blurdetect + signalstats)
      4. verify_naming.sh (py) — vérification nommage caméras via OpenCV

    Chaque étape est optionnelle via QUALITY_RUN_*.
    Un échec de files ou sanity est bloquant (session → quarantaine).
    Un score quality < QUALITY_SCORE_MIN ou un naming < QUALITY_NAMING_MIN
    est également bloquant.
    """

    def __init__(self):
        self._fix_script     = os.path.join(QUALITY_DIR, "fix_shit.sh")
        self._files_script   = os.path.join(QUALITY_DIR, "files.sh")
        self._sanity_script  = os.path.join(QUALITY_DIR, "sanity.sh")
        self._quality_script = os.path.join(QUALITY_DIR, "quality.sh")
        self._naming_script  = os.path.join(QUALITY_DIR, "verify_naming.sh")

    # ── helpers ───────────────────────────────────────────────────────────────

    def _run(self, cmd: list, timeout: int) -> tuple[int, str, str]:
        """Lance un sous-processus, retourne (returncode, stdout, stderr)."""
        try:
            proc = __import__("subprocess").run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return proc.returncode, proc.stdout, proc.stderr
        except __import__("subprocess").TimeoutExpired:
            return -1, "", f"timeout after {timeout}s"
        except Exception as e:
            return -1, "", str(e)

    def _run_sh(self, script: str, session_dir: str, timeout: int) -> tuple[bool, list, list]:
        """
        Lance un script bash sur session_dir.
        Retourne (ok, errors[], oks[]).
        """
        rc, stdout, stderr = self._run(["bash", script, session_dir], timeout)
        combined = stdout + "\n" + stderr
        errors = [l[len("[ERROR] "):].strip() for l in combined.splitlines() if "[ERROR]" in l]
        oks    = [l[len("[OK] "):].strip()    for l in combined.splitlines() if "[OK]" in l]
        passed = (rc == 0) and not errors
        return passed, errors, oks

    # ── étapes ────────────────────────────────────────────────────────────────

    def _step_fix(self, session_dir: str) -> list:
        """fix_shit.sh — corrections auto, ne bloque jamais."""
        if not QUALITY_RUN_FIX or not os.path.isfile(self._fix_script):
            return []
        _, stdout, stderr = self._run(["bash", self._fix_script, session_dir], QUALITY_TIMEOUT_FIX)
        combined = stdout + "\n" + stderr
        fixes = [l[len("[FIX]"):].strip() for l in combined.splitlines() if "[FIX]" in l]
        if fixes:
            log.info("[Quality] fixes appliquées : %s", fixes)
        return fixes

    def _step_files(self, session_dir: str) -> tuple[bool, list]:
        """files.sh — bloquant."""
        if not os.path.isfile(self._files_script):
            log.warning("[Quality] files.sh introuvable, skip")
            return True, []
        passed, errors, _ = self._run_sh(self._files_script, session_dir, QUALITY_TIMEOUT_FILES)
        return passed, errors

    def _step_sanity(self, session_dir: str) -> tuple[bool, list]:
        """sanity.sh — bloquant."""
        if not os.path.isfile(self._sanity_script):
            log.warning("[Quality] sanity.sh introuvable, skip")
            return True, []
        rc, stdout, stderr = self._run(["bash", self._sanity_script, session_dir], QUALITY_TIMEOUT_SANITY)
        combined = stdout + "\n" + stderr
        errors = [l[len("[ERROR] "):].strip() for l in combined.splitlines() if "[ERROR]" in l]
        passed = (rc == 0) and "[SUCCESS]" in combined and not errors
        return passed, errors

    def _step_quality(self, session_dir: str) -> tuple[float, dict]:
        """quality.sh — score /100 par vidéo. Retourne (score_avg, details)."""
        if not QUALITY_RUN_QUALITY or not os.path.isfile(self._quality_script):
            return -1.0, {}
        rc, stdout, stderr = self._run(["bash", self._quality_script, session_dir], QUALITY_TIMEOUT_QUALITY)
        combined = stdout + "\n" + stderr
        # Parser les notes finales : "NOTE FINALE: XX.X / 100"
        import re as _re
        scores = {}
        current_cam = None
        for line in combined.splitlines():
            m = _re.match(r"^VIDEO:\s*(\w+)", line)
            if m:
                current_cam = m.group(1)
            m2 = _re.match(r"^NOTE FINALE:\s*([\d.]+)", line)
            if m2 and current_cam:
                scores[current_cam] = float(m2.group(1))
        score_avg = sum(scores.values()) / len(scores) if scores else -1.0
        return score_avg, {"cameras": scores, "score_avg": score_avg}

    def _step_naming(self, session_dir: str) -> tuple[bool, dict]:
        """verify_naming.sh (Python) — nommage caméras OpenCV."""
        if not QUALITY_RUN_NAMING or not os.path.isfile(self._naming_script):
            return True, {}
        rc, stdout, stderr = self._run(
            ["python3", self._naming_script, session_dir],
            QUALITY_TIMEOUT_NAMING,
        )
        if rc != 0:
            # Si cv2/OpenCV n'est pas installé, skip silencieux plutôt que bloquer toutes les sessions
            if "No module named 'cv2'" in stderr or "ModuleNotFoundError" in stderr:
                log.warning("[Quality] verify_naming skipped — cv2 non installé (pip3 install opencv-python-headless)")
                return True, {"skipped": "cv2_missing"}
            log.warning("[Quality] verify_naming error (rc=%d): %s", rc, stderr[:200])
            return False, {"error": stderr[:200]}
        try:
            report = json.loads(stdout)
            result = report.get("result", {})
            ok = all(v >= QUALITY_NAMING_MIN for v in result.values())
            return ok, result
        except Exception as e:
            log.warning("[Quality] verify_naming JSON parse error: %s", e)
            return False, {"error": str(e)}

    # ── point d'entrée ────────────────────────────────────────────────────────

    def check(self, session_dir: str, session_id: str) -> QualityResult:
        """
        Lance le pipeline complet sur session_dir.
        Retourne un QualityResult avec passed=True si la session peut partir sur le NAS.
        """
        log.info("[Quality] Début quality check — session=%s", session_id)
        all_errors = []
        details: dict = {}

        # 0. Fix auto
        fixes = self._step_fix(session_dir)
        if fixes:
            details["fixes"] = fixes

        # 1. Files — bloquant
        files_ok, files_errors = self._step_files(session_dir)
        details["files"] = {"passed": files_ok, "errors": files_errors}
        if not files_ok:
            all_errors += files_errors
            log.warning("[Quality] BLOCKED files — session=%s errors=%s", session_id, files_errors)
            KAFKA.emit("quality", "blocked_files", session_id=session_id, errors=files_errors)
            return QualityResult(False, "blocked_files", 0.0, all_errors, details)

        # 2. Sanity — bloquant
        sanity_ok, sanity_errors = self._step_sanity(session_dir)
        details["sanity"] = {"passed": sanity_ok, "errors": sanity_errors}
        if not sanity_ok:
            all_errors += sanity_errors
            log.warning("[Quality] BLOCKED sanity — session=%s errors=%s", session_id, sanity_errors)
            KAFKA.emit("quality", "blocked_sanity", session_id=session_id, errors=sanity_errors)
            return QualityResult(False, "blocked_sanity", 0.0, all_errors, details)

        # 3. Quality score (optionnel mais bloquant si score trop bas)
        score_avg, quality_details = self._step_quality(session_dir)
        details["quality"] = quality_details
        if score_avg >= 0 and score_avg < QUALITY_SCORE_MIN:
            msg = f"score qualité trop bas: {score_avg:.1f} < {QUALITY_SCORE_MIN}"
            all_errors.append(msg)
            log.warning("[Quality] BLOCKED low_quality — session=%s %s", session_id, msg)
            KAFKA.emit("quality", "blocked_low_quality", session_id=session_id, score=score_avg)
            return QualityResult(False, "low_quality", score_avg, all_errors, details)

        # 4. Naming (optionnel mais bloquant si confusion caméras)
        naming_ok, naming_details = self._step_naming(session_dir)
        details["naming"] = naming_details
        if not naming_ok:
            msg = "nommage caméras incorrect (verify_naming)"
            all_errors.append(msg)
            log.warning("[Quality] BLOCKED bad_naming — session=%s scores=%s", session_id, naming_details)
            KAFKA.emit("quality", "blocked_bad_naming", session_id=session_id, naming=naming_details)
            return QualityResult(False, "bad_naming", score_avg, all_errors, details)

        log.info("[Quality] PASSED — session=%s score=%.1f", session_id, score_avg)
        KAFKA.emit("quality", "passed", session_id=session_id, score=score_avg)
        return QualityResult(True, "ok", score_avg, [], details)


QUALITY_CHECKER = QualityChecker()

# =========================
# SCANNER
# =========================

class Scanner(threading.Thread):
    def __init__(self, conn):
        super().__init__(daemon=True, name="scanner")
        self.conn = conn

    def run(self):
        log.info("[Scanner] Démarrage — surveillance du dossier : %s", INBOX_DIR)
        while True:
            try:
                self.scan()
            except Exception as e:
                log.error("[Scanner] Erreur inattendue : %s\n%s", e, traceback.format_exc())
            time.sleep(SCAN_INTERVAL)

    def _dir_size_and_count(self, path: str):
        total_bytes = 0
        count = 0
        for root, _dirs, files in os.walk(path):
            for f in files:
                try:
                    total_bytes += os.path.getsize(os.path.join(root, f))
                    count += 1
                except Exception:
                    pass
        return total_bytes, count

    def _quarantine_session(self, session_dir: str, session_id: str, reason: str):
        """Envoie une session failed sur le NAS (quarantine) puis supprime localement."""
        if not COPY_TO_NAS_QUARANTINE:
            try:
                os.makedirs(QUARANTINE_DIR, exist_ok=True)
                dst = os.path.join(QUARANTINE_DIR, session_id)
                if os.path.exists(dst):
                    dst = dst + f"_dup_{int(time.time())}"
                shutil.move(session_dir, dst)
                log.warning("[Scanner] Session '%s' mise en quarantaine locale : %s", session_id, reason)
                tui_log(f"QUARANTINE {session_id} — {reason}")
            except Exception as e:
                log.error("[Scanner] Impossible de mettre en quarantaine '%s' : %s", session_id, e)
            return

        remote_base = posixpath.join(QUARANTINE_ZONE.rstrip("/"), session_id)
        nas = NASClient()
        success = False
        for attempt in range(1, 4):
            try:
                nas.connect()
                nas.mkdir_p(remote_base)
                for root, _, files in os.walk(session_dir):
                    for fname in files:
                        local_abs = os.path.join(root, fname)
                        rel = os.path.relpath(local_abs, session_dir).replace(os.sep, "/")
                        nas.put_atomic(local_abs, posixpath.join(remote_base, rel))
                success = True
                break
            except Exception as e:
                log.warning("[Scanner] Quarantine NAS tentative %d/3 échouée '%s' : %s", attempt, session_id, e)
                nas.close()
        try:
            nas.close()
        except Exception:
            pass

        if success:
            log.warning("[Scanner] Session '%s' envoyée en quarantaine NAS — %s", session_id, reason)
            tui_log(f"QUARANTINE→NAS {session_id} [{reason}]")
            try:
                shutil.rmtree(session_dir)
            except Exception as e:
                log.warning("[Scanner] Suppression locale quarantaine '%s' échouée : %s", session_id, e)
        else:
            log.error("[Scanner] Quarantine NAS échouée '%s' — fallback local", session_id)
            try:
                os.makedirs(QUARANTINE_DIR, exist_ok=True)
                dst = os.path.join(QUARANTINE_DIR, session_id)
                if os.path.exists(dst):
                    dst = dst + f"_dup_{int(time.time())}"
                shutil.move(session_dir, dst)
            except Exception as e2:
                log.error("[Scanner] Fallback quarantaine locale échoué '%s' : %s", session_id, e2)

    def _qc_one(self, name):
        """Lance le quality check pour une session. Retourne (name, qr) ou (name, None) si erreur."""
        session_dir = os.path.join(INBOX_DIR, name)
        log.info("[Scanner] Quality check en cours : %s", name)
        tui_log(f"QC {name} …")
        try:
            qr = QUALITY_CHECKER.check(session_dir, name)
            return name, qr
        except Exception as e:
            log.error("[Scanner] Erreur inattendue quality check '%s' : %s\n%s",
                      name, e, traceback.format_exc())
            return name, None

    def scan(self):
        # Cherche les sous-dossiers directs de inbox/ qui matchent session_YYYYMMDD_HHMMSS
        try:
            entries = os.listdir(INBOX_DIR)
        except Exception as e:
            log.error("[Scanner] Impossible de lire inbox : %s", e)
            return

        # Récupère tous les session_id déjà connus en une seule requête
        try:
            known = set(
                r[0] for r in self.conn.execute("SELECT session_id FROM jobs").fetchall()
            )
        except Exception as e:
            log.error("[Scanner] Impossible de lire les jobs connus : %s", e)
            return

        # Filtre les sessions candidates (pas encore en DB)
        candidates = []
        for name in sorted(entries):
            if not SESSION_RE.match(name):
                continue
            if name in known:
                continue
            session_dir = os.path.join(INBOX_DIR, name)
            if not os.path.isdir(session_dir):
                continue
            candidates.append(name)

        inbox_pending = len(candidates)
        if inbox_pending:
            log.info("[Scanner] %d sessions en attente dans inbox (batch=%d)", inbox_pending, SCAN_BATCH_SIZE)
            tui_log(f"INBOX {inbox_pending} sessions à traiter")

        if not candidates:
            return

        # Traite par batch pour insérer en DB progressivement et laisser les workers démarrer
        batch = candidates[:SCAN_BATCH_SIZE]

        # ── Quality check en parallèle ───────────────────────────────────────
        with concurrent.futures.ThreadPoolExecutor(max_workers=SCAN_QC_WORKERS) as pool:
            futures = {pool.submit(self._qc_one, name): name for name in batch}
            for future in concurrent.futures.as_completed(futures):
                try:
                    name, qr = future.result()
                except Exception as e:
                    log.error("[Scanner] Future exception : %s", e)
                    continue

                if qr is None:
                    # Erreur interne du checker — session laissée en inbox pour retry
                    continue

                session_dir = os.path.join(INBOX_DIR, name)

                if not qr.passed:
                    log.warning("[Scanner] Quality FAIL %s — label=%s errors=%s",
                                name, qr.label, qr.errors)
                    tui_log(f"QC FAIL {name} [{qr.label}]")
                    self._quarantine_session(session_dir, name, qr.label)
                    jid = uuid.uuid4().hex
                    self.conn.execute(
                        "INSERT OR IGNORE INTO jobs(id,session_dir,session_id,size_bytes,file_count,"
                        "status,attempts,last_error,created_at,updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (jid, os.path.join(QUARANTINE_DIR, name), name,
                         0, 0, "failed", 1,
                         f"quality:{qr.label}:{'; '.join(qr.errors[:3])}",
                         now_iso(), now_iso()),
                    )
                    self.conn.commit()
                    REPORTER.inc_failed()
                    tui_inc_failed()
                    continue

                tui_log(f"QC PASS {name} score={qr.score:.0f}")

                # ── Quality OK → déplace dans spool/ et met en queue ────────
                try:
                    jid = uuid.uuid4().hex
                    dst = os.path.join(SPOOL_DIR, name)

                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                        log.warning("[Scanner] Résidu supprimé dans spool/ : %s", dst)

                    # session_dir peut avoir bougé si un autre thread a déjà traité
                    # une session du même nom (race) — on vérifie avant rename
                    if not os.path.isdir(session_dir):
                        log.warning("[Scanner] Session '%s' disparue avant enregistrement, ignorée.", name)
                        continue

                    os.rename(session_dir, dst)
                    size_bytes, file_count = self._dir_size_and_count(dst)

                    self.conn.execute(
                        "INSERT INTO jobs(id,session_dir,session_id,size_bytes,file_count,status,attempts,last_error,created_at,updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (jid, dst, name, size_bytes, file_count, "queued", 0, "", now_iso(), now_iso()),
                    )
                    self.conn.commit()

                    log.info("[Scanner] Session mise en file : %s (%d fichiers, %.1f MB) score=%.0f [job=%s]",
                             name, file_count, size_bytes / (1024*1024), qr.score, jid[:8])

                except Exception as e:
                    log.error("[Scanner] Impossible d'enregistrer la session '%s' : %s\n%s", name, e, traceback.format_exc())

        # Mise à jour reporter + TUI
        try:
            rows = self.conn.execute(
                "SELECT id, session_id, size_bytes, file_count, created_at FROM jobs WHERE status='queued' ORDER BY created_at ASC"
            ).fetchall()
            queue = [
                {
                    "session_id": r[1],
                    "size_mb": round((r[2] or 0) / (1024*1024), 2),
                    "file_count": r[3],
                    "received_at": r[4],
                }
                for r in rows
            ]
            REPORTER.set_inbound_queue(queue)
            total_q_bytes = sum((r[2] or 0) for r in rows)
            tui_set_queue(len(rows), total_q_bytes)
        except Exception as e:
            log.debug("[Scanner] Reporter queue update failed: %s", e)

# =========================
# WORKER — NAS persistent + reconnect/backoff
# =========================

class Worker(threading.Thread):
    def __init__(self, idx, conn):
        super().__init__(daemon=True, name=f"worker-{idx}")
        self.conn = conn
        self.idx = idx
        self.nas = NASClient()

    def get_job(self):
        # BEGIN EXCLUSIVE garantit qu'un seul worker à la fois peut sélectionner
        # et marquer un job, évitant que plusieurs workers prennent le même job.
        try:
            self.conn.execute("BEGIN EXCLUSIVE")
        except Exception:
            return None

        try:
            row = self.conn.execute(
                "SELECT id,session_dir,session_id,size_bytes,file_count,attempts "
                "FROM jobs WHERE status='queued' ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if not row:
                self.conn.execute("ROLLBACK")
                return None

            jid = row[0]
            self.conn.execute(
                "UPDATE jobs SET status='processing', updated_at=? WHERE id=?",
                (now_iso(), jid),
            )
            self.conn.execute("COMMIT")
        except Exception:
            try:
                self.conn.execute("ROLLBACK")
            except Exception:
                pass
            return None

        log.debug("[Worker-%d] Job %s pris en charge.", self.idx, jid[:8])
        return row  # (id, session_dir, session_id, size_bytes, file_count, attempts)

    def run(self):
        log.info("[Worker-%d] Démarrage.", self.idx)
        while True:
            job = self.get_job()
            if not job:
                time.sleep(0.2)  # TURBO: poll plus fréquent (200ms au lieu de 1s)
                continue
            self.process(job)

    def _nas_call(self, op: str, fn, *args, **kwargs):
        for attempt in range(1, 6):
            try:
                self.nas.ensure()
                return fn(*args, **kwargs)
            except (paramiko.SSHException, ConnectionResetError, EOFError, OSError) as e:
                log.warning("[NAS] '%s' échoué tentative %d/5 : %s", op, attempt, e)
                try:
                    self.nas.close()
                except Exception:
                    pass
                time.sleep(min(30, attempt * 3.0) + random.random())
        raise RuntimeError(f"NAS operation failed op={op}")

    def _collect_files(self, session_dir: str):
        """Retourne la liste de (local_abs, rel_posix) pour tous les fichiers de la session."""
        files = []
        for root, _, fnames in os.walk(session_dir):
            for fname in fnames:
                local_abs = os.path.join(root, fname)
                rel = os.path.relpath(local_abs, session_dir)
                rel_posix = rel.replace(os.sep, "/")
                files.append((local_abs, rel_posix))
        return sorted(files, key=lambda x: x[1])

    def process(self, job):
        jid, session_dir, session_id, size_bytes, file_count, attempts_prev = job
        attempts = attempts_prev + 1

        log.info("[JOB %s] Session '%s' — %d fichiers, %.1f MB, tentative %d",
                 jid[:8], session_id, file_count, (size_bytes or 0) / (1024*1024), attempts)

        tui_update_transfer(self.idx, {
            "session_id": session_id,
            "files_done": 0,
            "file_count": file_count,
            "bytes_sent": 0,
            "total_bytes": size_bytes or 1,
            "speed_mbps": 0.0,
        })
        REPORTER.set_current_transfer({
            "session_id": session_id,
            "progress_pct": 0,
            "speed_mbps": 0.0,
            "file_count": file_count,
        })

        try:
            if not os.path.isdir(session_dir):
                raise Exception(f"dossier session introuvable : {session_dir}")

            files = self._collect_files(session_dir)
            if not files:
                raise Exception("session vide — aucun fichier trouvé")

            # Dossier NAS cible : /data/INBOX/<session_id>/
            remote_base = posixpath.join(SFTP_BASE_DIR.rstrip("/"), session_id)
            self._nas_call("MKDIR_SESSION", self.nas.mkdir_p, remote_base)

            t_start = time.monotonic()
            bytes_sent = 0
            total_bytes = sum(os.path.getsize(f) for f, _ in files) or 1

            for i, (local_abs, rel_posix) in enumerate(files):
                remote_path = posixpath.join(remote_base, rel_posix)
                fsize = os.path.getsize(local_abs)

                self._nas_call(f"UPLOAD:{rel_posix}", self.nas.put_atomic, local_abs, remote_path)

                bytes_sent += fsize
                pct = int(bytes_sent / total_bytes * 90)
                elapsed = max(time.monotonic() - t_start, 0.001)
                speed = round(bytes_sent / (1024*1024) / elapsed, 2)

                tui_update_transfer(self.idx, {
                    "session_id": session_id,
                    "files_done": i + 1,
                    "file_count": len(files),
                    "bytes_sent": bytes_sent,
                    "total_bytes": total_bytes,
                    "speed_mbps": speed,
                })
                tui_push_speed(speed)
                REPORTER.set_current_transfer({
                    "session_id": session_id,
                    "progress_pct": pct,
                    "speed_mbps": speed,
                    "files_done": i + 1,
                    "file_count": len(files),
                })

                log.debug("[JOB %s] %d/%d '%s' @ %.1f MB/s", jid[:8], i+1, len(files), rel_posix, speed)

            elapsed = max(time.monotonic() - t_start, 0.001)
            speed = round((size_bytes or 0) / (1024*1024) / elapsed, 2)
            log.info("[JOB %s] Session '%s' envoyée en %.1fs @ %.1f MB/s", jid[:8], session_id, elapsed, speed)

            self.conn.execute(
                "UPDATE jobs SET status='done', updated_at=? WHERE id=?",
                (now_iso(), jid),
            )
            self.conn.commit()

            REPORTER.inc_processed()
            REPORTER.inc_forwarded()
            REPORTER.set_current_transfer(None)
            tui_update_transfer(self.idx, None)
            tui_inc_done(size_bytes or 0)

            if DELETE_LOCAL_AFTER_SUCCESS:
                try:
                    shutil.rmtree(session_dir)
                    log.info("[JOB %s] Dossier local supprimé : %s", jid[:8], session_dir)
                    tui_log(f"DELETED {session_id}")
                except Exception as ce:
                    log.warning("[JOB %s] Suppression locale échouée : %s — %s", jid[:8], session_dir, ce)

        except Exception as e:
            err = str(e)
            log.warning("[JOB %s] Echec tentative %d/%d — %s\n%s", jid[:8], attempts, MAX_RETRIES, err, traceback.format_exc())

            if attempts < MAX_RETRIES:
                self.conn.execute(
                    "UPDATE jobs SET status='queued', attempts=?, last_error=?, updated_at=? WHERE id=?",
                    (attempts, err[:2000], now_iso(), jid),
                )
                self.conn.commit()

                log.info("[JOB %s] Nouvelle tentative dans %ds (%d/%d).", jid[:8], RETRY_BACKOFF, attempts, MAX_RETRIES)
                time.sleep(RETRY_BACKOFF)
                return

            try:
                os.makedirs(QUARANTINE_DIR, exist_ok=True)
                q = os.path.join(QUARANTINE_DIR, session_id)

                if os.path.isdir(session_dir):
                    shutil.move(session_dir, q)
                    log.warning("[JOB %s] Session '%s' mise en quarantaine locale : %s", jid[:8], session_id, q)

                if COPY_TO_NAS_QUARANTINE and os.path.isdir(q):
                    remote_q_base = posixpath.join(SFTP_BASE_DIR.rstrip("/"), QUARANTINE_ZONE, session_id)
                    log.warning("[JOB %s] Envoi quarantaine NAS : %s", jid[:8], remote_q_base)
                    for local_abs, rel_posix in self._collect_files(q):
                        remote_path = posixpath.join(remote_q_base, rel_posix)
                        try:
                            self._nas_call(f"QUARANTINE:{rel_posix}", self.nas.put_atomic, local_abs, remote_path)
                        except Exception:
                            pass

            except Exception as e2:
                log.error("[JOB %s] Echec mise en quarantaine : %s\n%s", jid[:8], e2, traceback.format_exc())

            self.conn.execute(
                "UPDATE jobs SET status='failed', attempts=?, last_error=?, updated_at=? WHERE id=?",
                (attempts, err[:2000], now_iso(), jid),
            )
            self.conn.commit()

            REPORTER.inc_processed()
            REPORTER.inc_failed()
            REPORTER.set_current_transfer(None)
            tui_update_transfer(self.idx, None)
            tui_inc_failed()

            log.error("[JOB %s] Job définitivement échoué après %d tentatives.", jid[:8], attempts)

# =========================
# MAIN
# =========================

def reset_db():
    if not os.path.exists(DB_PATH):
        print(f"Rien à supprimer ({DB_PATH} n'existe pas).")
        return
    answer = input(f"Supprimer la base de données {DB_PATH} ? [oui/non] : ").strip().lower()
    if answer != "oui":
        print("Annulé.")
        sys.exit(0)
    os.remove(DB_PATH)
    print(f"Base supprimée : {DB_PATH}")
    print("Relance le programme sans --reset pour repartir à zéro.")
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="Spool — transfert fichiers vers NAS")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Supprime la base de données et repart à zéro (demande confirmation).",
    )
    args = parser.parse_args()

    if args.reset:
        reset_db()

    os.makedirs(INBOX_DIR, exist_ok=True)
    os.makedirs(SPOOL_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    log.info("[App] Démarrage — inbox=%s  spool=%s  quarantine=%s  db=%s", INBOX_DIR, SPOOL_DIR, QUARANTINE_DIR, DB_PATH)
    log.info("[App] Mode : %d worker(s)", WORKERS)

    conn = db()

    # Reset des jobs figés en 'processing' (crash précédent) → retour en 'queued'
    stuck = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE status='processing'"
    ).fetchone()[0]
    if stuck:
        conn.execute(
            "UPDATE jobs SET status='queued', updated_at=? WHERE status='processing'",
            (now_iso(),),
        )
        conn.commit()
        log.warning("[App] %d job(s) figé(s) en 'processing' remis en file.", stuck)

    # Purge des jobs queued/processing dont le dossier n'existe plus sur disque
    orphans = conn.execute(
        "SELECT id, session_dir, session_id FROM jobs WHERE status IN ('queued','processing')"
    ).fetchall()
    purged = 0
    for jid, session_dir, session_id in orphans:
        if not session_dir or not os.path.isdir(session_dir):
            conn.execute(
                "UPDATE jobs SET status='failed', last_error='dossier introuvable au démarrage', updated_at=? WHERE id=?",
                (now_iso(), jid),
            )
            purged += 1
    if purged:
        conn.commit()
        log.warning("[App] %d job(s) sans dossier sur disque marqués failed.", purged)

    REPORTER.set_db(conn)
    REPORTER.start()

    # Initialise les compteurs TUI depuis la DB (sessions déjà connues)
    try:
        r = conn.execute(
            "SELECT COUNT(*), SUM(size_bytes) FROM jobs WHERE status IN ('queued','processing')"
        ).fetchone()
        tui_set_queue(r[0] or 0, r[1] or 0)

        r2 = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='done'").fetchone()
        r3 = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='failed'").fetchone()
        global _tui_done_count, _tui_failed_count
        with _TUI_LOCK:
            _tui_done_count  = r2[0] or 0
            _tui_failed_count = r3[0] or 0
    except Exception:
        pass

    TUI.start()

    if WORKERS == 1:
        log.info("[App] Démarrage en mode mono-thread.")
        scanner = Scanner(conn)
        worker = Worker(1, conn)
        while True:
            try:
                scanner.scan()
            except Exception as e:
                log.error("[Scanner] Erreur inattendue : %s\n%s", e, traceback.format_exc())
            job = worker.get_job()
            if job:
                worker.process(job)
            else:
                time.sleep(SCAN_INTERVAL)
    else:
        log.info("[App] Démarrage en mode multi-thread (%d workers).", WORKERS)
        Scanner(conn).start()
        for i in range(WORKERS):
            Worker(i + 1, db()).start()
        while True:
            time.sleep(60)

if __name__ == "__main__":
    main()
