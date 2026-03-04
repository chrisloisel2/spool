#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import uuid
import json
import shutil
import hashlib
import sqlite3
import logging
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
WORKERS = 6
MAX_RETRIES = 8
RETRY_BACKOFF = 5

# NAS (destination finale)
NAS_HOST = "192.168.88.248"
NAS_PORT = 22
NAS_USER = "EXORIA"
NAS_PASS = "REPLACE_ME"  # mets ton mot de passe ici

SFTP_BASE_DIR = "/DB-EXORIA/lakehouse"
LANDING_ZONE = "bronze/landing"
QUARANTINE_ZONE = "bronze/quarantine"

DELETE_LOCAL_AFTER_SUCCESS = False
COPY_TO_NAS_QUARANTINE = True
STABLE_FILE_SECONDS = 2

# =========================
# DURCISSEMENT SSH/SFTP (banner reset)
# =========================

NAS_MAX_SIMULT_CONNECT = 1      # réduit la rafale de connexions NAS
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
                retries=10,
                acks="all",
                linger_ms=10,
                request_timeout_ms=15000,
                api_version_auto_timeout_ms=15000,
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

            log.debug("nas_connect_start host=%s port=%s user=%s", NAS_HOST, NAS_PORT, NAS_USER)
            KAFKA.emit("SFTP_CONNECT_START", "ok", host=NAS_HOST, port=NAS_PORT, user=NAS_USER)

            NAS_CONNECT_SEM.acquire()
            try:
                tcp_check(NAS_HOST, NAS_PORT, 3.0)

                last_err = None
                for attempt in range(1, 6):
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

                        log.debug("nas_connect_ok")
                        KAFKA.emit("SFTP_CONNECTED", "ok", host=NAS_HOST, port=NAS_PORT, user=NAS_USER)
                        return

                    except (paramiko.SSHException, ConnectionResetError, EOFError, OSError) as e:
                        last_err = e
                        log.warning("nas_connect_retry attempt=%d err=%s", attempt, e)
                        KAFKA.emit("SFTP_CONNECT_RETRY", "warn", attempt=attempt, error=str(e))

                        try:
                            if self.ssh:
                                self.ssh.close()
                        except Exception:
                            pass
                        self.ssh = None
                        self.sftp = None

                        time.sleep(min(30, attempt * 3.0) + random.random())

                raise RuntimeError(f"NAS connect failed: {last_err}")

            except Exception as e:
                log.error("nas_connect_fail %s\n%s", e, traceback.format_exc())
                KAFKA.emit("SFTP_CONNECT_FAIL", "error", host=NAS_HOST, port=NAS_PORT, user=NAS_USER, error=str(e))
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

        tmp = remote + ".part"
        log.debug("nas_put_upload_start local=%s tmp=%s", local, tmp)
        self.sftp.put(local, tmp)
        log.debug("nas_put_upload_done local=%s tmp=%s", local, tmp)

        final = remote
        if self.exists(final):
            base = final
            i = 1
            while self.exists(f"{base}.dup{i}"):
                i += 1
            final = f"{base}.dup{i}"
            log.debug("nas_put_target_exists base=%s chosen=%s", base, final)

        log.debug("nas_put_rename tmp=%s final=%s", tmp, final)
        self.sftp.rename(tmp, final)
        log.debug("nas_put_complete final=%s", final)
        return final

# =========================
# SCANNER
# =========================

class Scanner(threading.Thread):
    def __init__(self, conn):
        super().__init__(daemon=True, name="scanner")
        self.conn = conn

    def run(self):
        log.info("scanner_start inbox=%s", INBOX_DIR)
        KAFKA.emit("SCANNER_START", "ok", inbox=INBOX_DIR)
        while True:
            try:
                self.scan()
            except Exception as e:
                log.error("scanner_error %s\n%s", e, traceback.format_exc())
                KAFKA.emit("SCANNER_ERROR", "error", error=str(e))
            time.sleep(SCAN_INTERVAL)

    def scan(self):
        for root, _dirs, files in os.walk(INBOX_DIR):
            rel = os.path.relpath(root, INBOX_DIR)
            sender = "unknown" if rel == "." else safe_sender(rel.split(os.sep)[0])

            for f in files:
                src = os.path.join(root, f)
                if f.endswith(".part") or f.endswith(".tmp"):
                    log.debug("scanner_skip_tmp %s", src)
                    continue

                log.debug("scanner_detected sender=%s path=%s", sender, src)
                KAFKA.emit("FILE_DETECTED", "ok", sender=sender, path=src, name=f)

                if not stable(src):
                    log.debug("scanner_not_stable %s", src)
                    KAFKA.emit("FILE_NOT_STABLE", "warn", sender=sender, path=src, name=f)
                    continue

                KAFKA.emit("FILE_STABLE", "ok", sender=sender, path=src, name=f)

                try:
                    jid = uuid.uuid4().hex
                    dst = os.path.join(SPOOL_DIR, jid + "__" + f)

                    log.debug("scanner_move_start src=%s dst=%s", src, dst)
                    os.replace(src, dst)
                    log.debug("scanner_move_done dst=%s", dst)
                    KAFKA.emit("MOVED_TO_SPOOL", "ok", job_id=jid, sender=sender, src=src, dst=dst, name=f)

                    size = os.path.getsize(dst)
                    log.debug("scanner_size job=%s size=%d", jid, size)

                    log.debug("scanner_hash_start job=%s", jid)
                    h = sha256(dst)
                    log.debug("scanner_hash_done job=%s sha256=%s", jid, h)
                    KAFKA.emit("HASHED", "ok", job_id=jid, sender=sender, name=f, size=size, sha256=h)

                    self.conn.execute(
                        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (jid, dst, sender, f, size, h, "queued", 0, "", now_iso(), now_iso()),
                    )
                    self.conn.commit()

                    log.info("queued job=%s sender=%s file=%s size=%d sha=%s", jid, sender, f, size, h[:12])
                    KAFKA.emit("QUEUED_DB", "ok", job_id=jid, sender=sender, name=f, size=size, sha256=h)

                except Exception as e:
                    log.error("scanner_enqueue_failed src=%s err=%s\n%s", src, e, traceback.format_exc())
                    KAFKA.emit("SCAN_ENQUEUE_FAILED", "error", sender=sender, path=src, name=f, error=str(e))

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

        log.debug("job_claimed job=%s", jid)
        KAFKA.emit("JOB_CLAIMED", "ok", job_id=jid, worker=self.idx)
        return row2

    def run(self):
        log.info("worker_start idx=%s", self.idx)
        KAFKA.emit("WORKER_START", "ok", worker=self.idx)

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
                log.warning("nas_call_fail op=%s attempt=%d err=%s", op, attempt, e)
                KAFKA.emit("NAS_CALL_FAIL", "warn", op=op, attempt=attempt, error=str(e))
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

        log.info("process_start job=%s attempt=%d sender=%s file=%s", jid, attempts, sender, name)
        KAFKA.emit("PROCESS_START", "ok", job_id=jid, attempt=attempts, sender=sender, name=name, size=size)

        try:
            if not os.path.exists(path):
                raise Exception("missing file")

            log.debug("hash_verify_start job=%s path=%s", jid, path)
            h2 = sha256(path)
            if h2 != sha:
                raise Exception("hash mismatch")
            log.debug("hash_verify_ok job=%s", jid)
            KAFKA.emit("HASH_VERIFIED", "ok", job_id=jid, sender=sender, name=name, sha256=sha)

            remote, manifest_remote, remote_dir = self.build_remote(sender, name)
            log.debug("remote_paths job=%s remote=%s manifest=%s", jid, remote, manifest_remote)
            KAFKA.emit("REMOTE_PATHS_READY", "ok", job_id=jid, sender=sender, remote=remote, manifest=manifest_remote)

            manifest_local = path + ".manifest.json"
            data = {
                "job": jid,
                "sender": sender,
                "file": name,
                "sha256": sha,
                "size_bytes": size,
                "time": now_iso(),
            }
            log.debug("manifest_write_start job=%s local=%s", jid, manifest_local)
            with open(manifest_local, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log.debug("manifest_write_done job=%s", jid)
            KAFKA.emit("MANIFEST_WRITTEN", "ok", job_id=jid, sender=sender, manifest_local=manifest_local)

            KAFKA.emit("REMOTE_DIR_READY", "ok", job_id=jid, remote_dir=remote_dir)
            self._nas_call("MKDIR_REMOTE", self.nas.mkdir_p, remote_dir)

            KAFKA.emit("UPLOAD_FILE_START", "ok", job_id=jid, local=path, remote=remote)
            final_remote = self._nas_call("UPLOAD_FILE", self.nas.put_atomic, path, remote)
            KAFKA.emit("UPLOAD_FILE_DONE", "ok", job_id=jid, remote=final_remote)

            KAFKA.emit("UPLOAD_MANIFEST_START", "ok", job_id=jid, local=manifest_local, remote=manifest_remote)
            final_manifest = self._nas_call("UPLOAD_MANIFEST", self.nas.put_atomic, manifest_local, manifest_remote)
            KAFKA.emit("UPLOAD_MANIFEST_DONE", "ok", job_id=jid, remote=final_manifest)

            self.conn.execute(
                "UPDATE jobs SET status='done', updated_at=? WHERE id=?",
                (now_iso(), jid),
            )
            self.conn.commit()

            log.info("process_done job=%s remote=%s", jid, final_remote)
            KAFKA.emit("JOB_DONE", "ok", job_id=jid, sender=sender, name=name, remote=final_remote)

            try:
                os.remove(manifest_local)
                log.debug("cleanup_manifest_removed job=%s", jid)
                KAFKA.emit("CLEANUP_MANIFEST", "ok", job_id=jid)
            except Exception as ce:
                log.warning("cleanup_manifest_failed job=%s err=%s", jid, ce)
                KAFKA.emit("CLEANUP_MANIFEST", "warn", job_id=jid, error=str(ce))

            if DELETE_LOCAL_AFTER_SUCCESS:
                try:
                    os.remove(path)
                    log.debug("cleanup_file_removed job=%s", jid)
                    KAFKA.emit("CLEANUP_FILE", "ok", job_id=jid)
                except Exception as ce:
                    log.warning("cleanup_file_failed job=%s err=%s", jid, ce)
                    KAFKA.emit("CLEANUP_FILE", "warn", job_id=jid, error=str(ce))

        except Exception as e:
            err = str(e)
            log.warning("process_failed job=%s attempt=%d err=%s\n%s", jid, attempts, err, traceback.format_exc())
            KAFKA.emit("PROCESS_FAILED", "error", job_id=jid, attempt=attempts, sender=sender, name=name, error=err)

            if attempts < MAX_RETRIES:
                self.conn.execute(
                    "UPDATE jobs SET status='queued', attempts=?, last_error=?, updated_at=? WHERE id=?",
                    (attempts, err[:2000], now_iso(), jid),
                )
                self.conn.commit()

                log.info("retry_scheduled job=%s attempt=%d backoff=%ds", jid, attempts, RETRY_BACKOFF)
                KAFKA.emit("RETRY_SCHEDULED", "warn", job_id=jid, attempt=attempts, backoff_s=RETRY_BACKOFF, error=err)
                time.sleep(RETRY_BACKOFF)
                return

            try:
                os.makedirs(QUARANTINE_DIR, exist_ok=True)
                q = os.path.join(QUARANTINE_DIR, os.path.basename(path))

                if os.path.exists(path):
                    shutil.move(path, q)
                    log.info("quarantine_local job=%s path=%s", jid, q)
                    KAFKA.emit("QUARANTINED_LOCAL", "warn", job_id=jid, local=q)

                if COPY_TO_NAS_QUARANTINE and os.path.exists(q):
                    base = SFTP_BASE_DIR.rstrip("/")
                    remote_q = posixpath.join(base, QUARANTINE_ZONE, sender, os.path.basename(q))
                    KAFKA.emit("UPLOAD_QUARANTINE_START", "warn", job_id=jid, local=q, remote=remote_q)
                    final_q = self._nas_call("UPLOAD_QUARANTINE", self.nas.put_atomic, q, remote_q)
                    KAFKA.emit("UPLOAD_QUARANTINE_DONE", "warn", job_id=jid, remote=final_q)
                    log.info("quarantine_nas job=%s remote=%s", jid, final_q)

            except Exception as e2:
                log.error("quarantine_failed job=%s err=%s\n%s", jid, e2, traceback.format_exc())
                KAFKA.emit("QUARANTINE_FAILED", "error", job_id=jid, error=str(e2))

            self.conn.execute(
                "UPDATE jobs SET status='failed', attempts=?, last_error=?, updated_at=? WHERE id=?",
                (attempts, err[:2000], now_iso(), jid),
            )
            self.conn.commit()

            log.error("job_failed_final job=%s attempts=%d", jid, attempts)
            KAFKA.emit("JOB_FAILED_FINAL", "error", job_id=jid, attempts=attempts, error=err)

# =========================
# MAIN
# =========================

def main():
    os.makedirs(INBOX_DIR, exist_ok=True)
    os.makedirs(SPOOL_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    log.info("app_start inbox=%s spool=%s quarantine=%s db=%s", INBOX_DIR, SPOOL_DIR, QUARANTINE_DIR, DB_PATH)
    KAFKA.emit("APP_START", "ok", inbox=INBOX_DIR, spool=SPOOL_DIR, quarantine=QUARANTINE_DIR, db=DB_PATH)

    conn = db()
    Scanner(conn).start()

    for i in range(WORKERS):
        Worker(i + 1, db()).start()

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
