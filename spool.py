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

SCAN_INTERVAL = 2

# Nombre de workers NAS en parallèle.
# 1 = tout dans le thread principal (pas de threads supplémentaires).
# >1 = scanner + N workers chacun dans leur propre thread.
WORKERS = 1

MAX_RETRIES = 8
RETRY_BACKOFF = 5

# NAS (destination finale)
NAS_HOST = "192.168.88.248"
NAS_PORT = 22
NAS_USER = "EXORIA"
NAS_PASS = "NasExori@2026!!#"  # mets ton mot de passe ici

SFTP_BASE_DIR = "/DB-EXORIA/lakehouse"
LANDING_ZONE = "bronze/landing"
QUARANTINE_ZONE = "bronze/quarantine"

DELETE_LOCAL_AFTER_SUCCESS = True
COPY_TO_NAS_QUARANTINE = True
STABLE_FILE_SECONDS = 2

# =========================
# DURCISSEMENT SSH/SFTP (banner reset)
# =========================

# Nombre max de connexions NAS simultanées (= WORKERS en pratique, laisser à 1 sauf besoin).
NAS_MAX_SIMULT_CONNECT = 1
SSH_TIMEOUT = 20
BANNER_TIMEOUT = 90
AUTH_TIMEOUT = 30
KEEPALIVE_SEC = 30

# =========================
# CONFIG KAFKA
# =========================

KAFKA_BROKER = "192.168.88.4"
KAFKA_BROKER_PORT = 9092
KAFKA_TOPIC = "topic2"

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
 local_path TEXT,
 sender TEXT,
 original_name TEXT,
 size_bytes INTEGER,
 sha256 TEXT,
 status TEXT,
 attempts INTEGER,
 last_error TEXT,
 created_at TEXT,
 updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);
"""

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
# SPOOL REPORTER — snapshot périodique vers topic2
# =========================

REPORTER_INTERVAL = 5  # secondes entre chaque snapshot

class SpoolReporter(threading.Thread):
    """
    Maintient l'état courant du spool et publie un snapshot complet
    sur topic2 toutes les REPORTER_INTERVAL secondes.
    Format : { source, timestamp, inbound_queue, processed_today,
               forwarded_to_nas, failed, current_transfer }
    """

    def __init__(self):
        super().__init__(daemon=True, name="spool-reporter")
        self._lock = threading.Lock()
        self._inbound_queue = []   # list of dicts {pc_id, session_id, received_at, size_mb}
        self._processed_today = 0
        self._forwarded_to_nas = 0
        self._failed = 0
        self._current_transfer = None  # dict or None

    # --- méthodes appelées par Scanner / Worker ---

    def set_inbound_queue(self, entries: list):
        """Remplace la file d'attente entière (appelé par Scanner après chaque scan)."""
        with self._lock:
            self._inbound_queue = list(entries)

    def set_current_transfer(self, info: dict | None):
        """Met à jour le transfert en cours (dict ou None)."""
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

    # --- boucle principale ---

    def run(self):
        log.info("[Reporter] Démarrage — intervalle=%ds topic=%s", REPORTER_INTERVAL, KAFKA_TOPIC)
        while True:
            try:
                self._emit()
            except Exception as e:
                log.warning("[Reporter] Erreur lors de l'émission : %s", e)
            time.sleep(REPORTER_INTERVAL)

    def _emit(self):
        with self._lock:
            msg = {
                "source": "spool",
                "timestamp": now_iso(),
                "inbound_queue": list(self._inbound_queue),
                "processed_today": self._processed_today,
                "forwarded_to_nas": self._forwarded_to_nas,
                "failed": self._failed,
                "current_transfer": self._current_transfer,
            }

        log.debug("[Reporter] Snapshot : queue=%d processed=%d forwarded=%d failed=%d transfer=%s",
                  len(msg["inbound_queue"]), msg["processed_today"],
                  msg["forwarded_to_nas"], msg["failed"],
                  msg["current_transfer"])

        if not HAS_KAFKA:
            return

        try:
            KAFKA._ensure()
            if not KAFKA._producer:
                return
            KAFKA._producer.send(KAFKA_TOPIC, msg)
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
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def stable(path):
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
        log.info("[NAS] Envoi du fichier '%s' (%d octets) vers NAS...", os.path.basename(local), size)
        log.debug("[NAS] Chemin local : %s", local)
        log.debug("[NAS] Chemin distant temporaire : %s", tmp)
        self.sftp.put(local, tmp)
        log.info("[NAS] Transfert terminé pour '%s'", os.path.basename(local))

        final = remote
        if self.exists(final):
            base = final
            i = 1
            while self.exists(f"{base}.dup{i}"):
                i += 1
            final = f"{base}.dup{i}"
            log.warning("[NAS] Fichier '%s' existe déjà, renommé en '%s'", os.path.basename(base), os.path.basename(final))

        log.debug("[NAS] Renommage : %s -> %s", tmp, final)
        self.sftp.rename(tmp, final)
        log.info("[NAS] Fichier disponible sur le NAS : %s", final)
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

    def scan(self):
        for root, _dirs, files in os.walk(INBOX_DIR):
            rel = os.path.relpath(root, INBOX_DIR)
            sender = "unknown" if rel == "." else safe_sender(rel.split(os.sep)[0])

            for f in files:
                src = os.path.join(root, f)
                if f.endswith(".part") or f.endswith(".tmp"):
                    log.debug("[Scanner] Fichier temporaire ignoré : %s", src)
                    continue

                log.debug("[Scanner] Fichier détecté — expéditeur=%s fichier=%s", sender, f)

                if not stable(src):
                    log.debug("[Scanner] Fichier pas encore stable (écriture en cours) : %s", src)
                    continue

                try:
                    jid = uuid.uuid4().hex
                    dst = os.path.join(SPOOL_DIR, jid + "__" + f)

                    os.replace(src, dst)
                    size = os.path.getsize(dst)
                    log.debug("[Scanner] Calcul SHA256 pour '%s'...", f)
                    h = sha256(dst)

                    self.conn.execute(
                        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (jid, dst, sender, f, size, h, "queued", 0, "", now_iso(), now_iso()),
                    )
                    self.conn.commit()

                    log.info("[Scanner] Nouveau fichier mis en file : '%s' (%d octets) de '%s' [job=%s]", f, size, sender, jid[:8])

                except Exception as e:
                    log.error("[Scanner] Impossible d'enregistrer le fichier '%s' : %s\n%s", src, e, traceback.format_exc())

        # Mise à jour de la file d'attente dans le reporter
        try:
            rows = self.conn.execute(
                "SELECT id, sender, size_bytes, created_at FROM jobs WHERE status='queued' ORDER BY created_at ASC"
            ).fetchall()
            queue = []
            for row in rows:
                # pc_id : on essaie d'extraire un entier depuis sender, sinon 0
                try:
                    pc_id = int(row[1])
                except (ValueError, TypeError):
                    pc_id = 0
                queue.append({
                    "pc_id": pc_id,
                    "session_id": row[0][:8],
                    "received_at": row[3],
                    "size_mb": round(row[2] / (1024 * 1024), 2) if row[2] else 0.0,
                })
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
            "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()

        if not row:
            return None

        jid = row[0]
        updated = now_iso()
        self.conn.execute(
            "UPDATE jobs SET status='processing', updated_at=? WHERE id=? AND status='queued'",
            (updated, jid),
        )
        self.conn.commit()

        row2 = self.conn.execute(
            "SELECT * FROM jobs WHERE id=? AND status='processing'",
            (jid,),
        ).fetchone()

        if not row2:
            return None

        log.debug("[Worker-%d] Job %s pris en charge.", self.idx, jid[:8])
        return row2

    def run(self):
        log.info("[Worker-%d] Démarrage.", self.idx)

        while True:
            job = self.get_job()
            if not job:
                time.sleep(1)
                continue
            self.process(job)

    def build_remote(self, sender, name):
        today = dt.datetime.utcnow()
        y = f"{today.year:04}"
        m = f"{today.month:02}"
        d = f"{today.day:02}"
        base = SFTP_BASE_DIR.rstrip("/")
        directory = posixpath.join(base, LANDING_ZONE, y, m, d, sender)
        file = posixpath.join(directory, name)
        manifest = file + ".manifest.json"
        return file, manifest, directory

    def _nas_call(self, op: str, fn, *args, **kwargs):
        for attempt in range(1, 6):
            try:
                self.nas.ensure()
                return fn(*args, **kwargs)
            except (paramiko.SSHException, ConnectionResetError, EOFError, OSError) as e:
                log.warning("[NAS] Opération '%s' échouée (tentative %d/5) : %s", op, attempt, e)
                try:
                    self.nas.close()
                except Exception:
                    pass
                time.sleep(min(30, attempt * 3.0) + random.random())
        raise RuntimeError(f"NAS operation failed op={op}")

    def process(self, job):
        jid = job[0]
        path = job[1]
        sender = job[2]
        name = job[3]
        size = job[4]
        sha = job[5]
        attempts_prev = job[7]
        attempts = attempts_prev + 1

        log.info("[JOB %s] Traitement démarré — fichier='%s' expéditeur='%s' tentative=%d", jid[:8], name, sender, attempts)

        # pc_id : extrait depuis sender si numérique
        try:
            pc_id = int(sender)
        except (ValueError, TypeError):
            pc_id = 0

        REPORTER.set_current_transfer({
            "from_pc": pc_id,
            "session_id": jid[:8],
            "progress_pct": 0,
            "speed_mbps": 0.0,
        })

        try:
            if not os.path.exists(path):
                raise Exception("missing file")

            log.debug("[JOB %s] Vérification intégrité SHA256...", jid[:8])
            h2 = sha256(path)
            if h2 != sha:
                raise Exception("hash mismatch")
            log.debug("[JOB %s] Intégrité OK.", jid[:8])

            remote, manifest_remote, remote_dir = self.build_remote(sender, name)
            log.debug("[JOB %s] Dossier NAS cible : %s", jid[:8], remote_dir)

            manifest_local = path + ".manifest.json"
            data = {
                "job": jid,
                "sender": sender,
                "file": name,
                "sha256": sha,
                "size_bytes": size,
                "time": now_iso(),
            }
            with open(manifest_local, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log.debug("[JOB %s] Manifeste créé localement.", jid[:8])

            self._nas_call("MKDIR_REMOTE", self.nas.mkdir_p, remote_dir)

            REPORTER.set_current_transfer({
                "from_pc": pc_id,
                "session_id": jid[:8],
                "progress_pct": 10,
                "speed_mbps": 0.0,
            })

            log.info("[JOB %s] Envoi du fichier '%s' vers le NAS...", jid[:8], name)
            t_start = time.monotonic()
            final_remote = self._nas_call("UPLOAD_FILE", self.nas.put_atomic, path, remote)
            elapsed = max(time.monotonic() - t_start, 0.001)
            speed = round((size / (1024 * 1024)) / elapsed, 2)
            log.info("[JOB %s] Fichier '%s' envoyé avec succès -> %s", jid[:8], name, final_remote)

            REPORTER.set_current_transfer({
                "from_pc": pc_id,
                "session_id": jid[:8],
                "progress_pct": 90,
                "speed_mbps": speed,
            })

            log.debug("[JOB %s] Envoi du manifeste vers le NAS...", jid[:8])
            final_manifest = self._nas_call("UPLOAD_MANIFEST", self.nas.put_atomic, manifest_local, manifest_remote)
            log.debug("[JOB %s] Manifeste envoyé -> %s", jid[:8], final_manifest)

            self.conn.execute(
                "UPDATE jobs SET status='done', updated_at=? WHERE id=?",
                (now_iso(), jid),
            )
            self.conn.commit()

            REPORTER.inc_processed()
            REPORTER.inc_forwarded()
            REPORTER.set_current_transfer(None)

            log.info("[JOB %s] Traitement terminé avec succès — fichier disponible sur le NAS : %s", jid[:8], final_remote)

            try:
                os.remove(manifest_local)
                log.debug("[JOB %s] Manifeste local supprimé.", jid[:8])
            except Exception as ce:
                log.warning("[JOB %s] Impossible de supprimer le manifeste local : %s", jid[:8], ce)

            if DELETE_LOCAL_AFTER_SUCCESS:
                try:
                    os.remove(path)
                    log.debug("[JOB %s] Fichier local supprimé après envoi réussi.", jid[:8])
                except Exception as ce:
                    log.warning("[JOB %s] Impossible de supprimer le fichier local : %s", jid[:8], ce)

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
                q = os.path.join(QUARANTINE_DIR, os.path.basename(path))

                if os.path.exists(path):
                    shutil.move(path, q)
                    log.warning("[JOB %s] Fichier '%s' mis en quarantaine locale : %s", jid[:8], name, q)

                if COPY_TO_NAS_QUARANTINE and os.path.exists(q):
                    base = SFTP_BASE_DIR.rstrip("/")
                    remote_q = posixpath.join(base, QUARANTINE_ZONE, sender, os.path.basename(q))
                    log.warning("[JOB %s] Envoi en quarantaine NAS : %s", jid[:8], remote_q)
                    final_q = self._nas_call("UPLOAD_QUARANTINE", self.nas.put_atomic, q, remote_q)
                    log.warning("[JOB %s] Fichier en quarantaine NAS : %s", jid[:8], final_q)

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

    REPORTER.start()
    conn = db()

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
