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

import paramiko

# =========================
# CONFIG HARDCODE
# =========================

INBOX_DIR = "/srv/exoria/inbox"
SPOOL_DIR = "/srv/exoria/spool"
QUARANTINE_DIR = "/srv/exoria/quarantine"
DB_PATH = "/srv/exoria/queue.db"

SCAN_INTERVAL = 1

# Nombre de workers NAS en parallèle.
# 1 = tout dans le thread principal (pas de threads supplémentaires).
# >1 = scanner + N workers chacun dans leur propre thread.
WORKERS = 4

MAX_RETRIES = 8
RETRY_BACKOFF = 2

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

# Nombre max de connexions NAS simultanées (= WORKERS en pratique, laisser à 1 sauf besoin).
NAS_MAX_SIMULT_CONNECT = 4
SSH_TIMEOUT = 20
BANNER_TIMEOUT = 90
AUTH_TIMEOUT = 30
KEEPALIVE_SEC = 30

# Tuning SFTP — buffers larges pour maximiser le débit
SFTP_WINDOW_SIZE  = 134217728   # 128 MB — fenêtre SSH max
SFTP_MAX_PACKET   = 65536        # 64 KB — paquet SFTP max
SFTP_READ_BUFFER  = 4194304      # 4 MB  — buffer lecture locale

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

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
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
                "SELECT id, sender, original_name, size_bytes, attempts, last_error, updated_at "
                "FROM jobs WHERE status='failed' ORDER BY updated_at DESC LIMIT 5"
            ).fetchall()
            recent_failed = [
                {
                    "job_id":    r[0][:8],
                    "sender":    r[1],
                    "file":      r[2],
                    "size_mb":   round((r[3] or 0) / (1024*1024), 2),
                    "attempts":  r[4],
                    "error":     (r[5] or "")[:120],
                    "failed_at": r[6],
                }
                for r in failed_jobs
            ]

            # 5 derniers jobs done
            done_jobs = self._conn.execute(
                "SELECT id, sender, original_name, size_bytes, updated_at "
                "FROM jobs WHERE status='done' ORDER BY updated_at DESC LIMIT 5"
            ).fetchall()
            recent_done = [
                {
                    "job_id":       r[0][:8],
                    "sender":       r[1],
                    "file":         r[2],
                    "size_mb":      round((r[3] or 0) / (1024*1024), 2),
                    "completed_at": r[4],
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

        log.debug("[Reporter] Snapshot : queue=%d transfer=%s failed=%d",
                  len(queue), transfer, failed)

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

    def _session_ready(self, session_dir: str) -> bool:
        return True

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

    def scan(self):
        # Cherche les sous-dossiers directs de inbox/ qui matchent session_YYYYMMDD_HHMMSS
        try:
            entries = os.listdir(INBOX_DIR)
        except Exception as e:
            log.error("[Scanner] Impossible de lire inbox : %s", e)
            return

        for name in entries:
            if not SESSION_RE.match(name):
                continue

            session_dir = os.path.join(INBOX_DIR, name)
            if not os.path.isdir(session_dir):
                continue

            # Déjà en base ?
            existing = self.conn.execute(
                "SELECT id FROM jobs WHERE session_id=?", (name,)
            ).fetchone()
            if existing:
                continue

            # Session complète ?
            if not self._session_ready(session_dir):
                log.debug("[Scanner] Session pas encore prête : %s", name)
                continue

            # Déplace le dossier entier dans spool/
            try:
                jid = uuid.uuid4().hex
                dst = os.path.join(SPOOL_DIR, name)

                shutil.move(session_dir, dst)
                size_bytes, file_count = self._dir_size_and_count(dst)

                self.conn.execute(
                    "INSERT INTO jobs(id,session_dir,session_id,size_bytes,file_count,status,attempts,last_error,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (jid, dst, name, size_bytes, file_count, "queued", 0, "", now_iso(), now_iso()),
                )
                self.conn.commit()

                log.info("[Scanner] Session mise en file : %s (%d fichiers, %.1f MB) [job=%s]",
                         name, file_count, size_bytes / (1024*1024), jid[:8])

            except Exception as e:
                log.error("[Scanner] Impossible d'enregistrer la session '%s' : %s\n%s", name, e, traceback.format_exc())

        # Mise à jour reporter
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
        row = self.conn.execute(
            "SELECT id,session_dir,session_id,size_bytes,file_count,attempts "
            "FROM jobs WHERE status='queued' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if not row:
            return None

        jid = row[0]
        self.conn.execute(
            "UPDATE jobs SET status='processing', updated_at=? WHERE id=? AND status='queued'",
            (now_iso(), jid),
        )
        self.conn.commit()

        # Vérifie qu'on a bien pris le lock
        if not self.conn.execute(
            "SELECT 1 FROM jobs WHERE id=? AND status='processing'", (jid,)
        ).fetchone():
            return None

        log.debug("[Worker-%d] Job %s pris en charge.", self.idx, jid[:8])
        return row  # (id, session_dir, session_id, size_bytes, file_count, attempts)

    def run(self):
        log.info("[Worker-%d] Démarrage.", self.idx)
        while True:
            job = self.get_job()
            if not job:
                time.sleep(1)
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
            total_bytes = size_bytes or 1

            for i, (local_abs, rel_posix) in enumerate(files):
                remote_path = posixpath.join(remote_base, rel_posix)
                fsize = os.path.getsize(local_abs)

                self._nas_call(f"UPLOAD:{rel_posix}", self.nas.put_atomic, local_abs, remote_path)

                bytes_sent += fsize
                pct = int(bytes_sent / total_bytes * 90)
                elapsed = max(time.monotonic() - t_start, 0.001)
                speed = round(bytes_sent / (1024*1024) / elapsed, 2)

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

            if DELETE_LOCAL_AFTER_SUCCESS:
                try:
                    shutil.rmtree(session_dir)
                    log.debug("[JOB %s] Dossier local supprimé.", jid[:8])
                except Exception as ce:
                    log.warning("[JOB %s] Suppression locale échouée : %s", jid[:8], ce)

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

    REPORTER.set_db(conn)
    REPORTER.start()

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
