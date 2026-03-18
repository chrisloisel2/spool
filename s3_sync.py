#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
s3_sync.py — Surveille /srv/exoria/inbox/ et upload chaque session_* vers S3
Bucket  : s3://physical-data-storage/bronze/<session_id>/...
Stratégie : une session est uploadée dès qu'elle apparaît et reste stable
            (aucun fichier modifié depuis STABLE_SECONDS secondes).
            Après upload complet et vérifié → optionnellement supprime le local.

Usage :
    python3 s3_sync.py [--inbox DIR] [--bucket BUCKET] [--prefix PREFIX]
                       [--interval 5] [--stable 10] [--delete-after-upload]
                       [--workers 4] [--dry-run]
"""

import os
import sys
import time
import json
import socket
import logging
import argparse
import hashlib
import threading
import datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    KafkaProducer = None
    HAS_KAFKA = False

# ── Config ────────────────────────────────────────────────────────────────────
INBOX_DIR           = "/srv/exoria/inbox"
S3_BUCKET           = "physical-data-storage"
S3_PREFIX           = "bronze"
SCAN_INTERVAL       = 5       # secondes entre deux scans
STABLE_SECONDS      = 10      # secondes sans modification avant d'uploader
DELETE_AFTER_UPLOAD = False   # supprimer localement après upload réussi
UPLOAD_WORKERS      = 4       # threads parallèles pour l'upload des fichiers

KAFKA_BROKER        = "192.168.88.4:9092"
KAFKA_TOPIC         = "monitoring"
HEARTBEAT_INTERVAL  = 30      # secondes entre deux heartbeats serveur
SERVER_ID           = os.environ.get("SERVER_ID", "spool-server-01")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [s3_sync] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("s3_sync")

# ── Kafka ─────────────────────────────────────────────────────────────────────
_kafka_producer = None
_kafka_lock     = threading.Lock()
_kafka_broker   = KAFKA_BROKER  # modifiable avant démarrage via main()


def _get_kafka():
    global _kafka_producer
    if not HAS_KAFKA:
        return None
    with _kafka_lock:
        if _kafka_producer:
            return _kafka_producer
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=[_kafka_broker],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                api_version=(2, 0, 0),
                retries=5,
                acks="all",
                request_timeout_ms=10000,
            )
            return _kafka_producer
        except Exception as e:
            log.warning("Kafka indisponible : %s", e)
            return None


def kafka_emit(**fields):
    p = _get_kafka()
    if not p:
        return
    try:
        payload = {
            "source": "s3_sync",
            "ts":     round(time.time(), 3),
            "ts_iso": dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            **fields,
        }
        p.send(KAFKA_TOPIC, payload)
        p.flush(timeout=5)
    except Exception as e:
        log.warning("Kafka emit échoué : %s", e)
        global _kafka_producer
        with _kafka_lock:
            _kafka_producer = None


# ── Heartbeat serveur ─────────────────────────────────────────────────────────

def _disk_info(path: str) -> dict:
    try:
        st = os.statvfs(path)
        total = st.f_frsize * st.f_blocks
        free  = st.f_frsize * st.f_bavail
        used  = total - free
        return {
            "total_gb": round(total / 1e9, 1),
            "free_gb":  round(free  / 1e9, 1),
            "used_pct": round(used / total * 100, 1) if total > 0 else 0.0,
        }
    except Exception:
        return {"total_gb": 0.0, "free_gb": 0.0, "used_pct": 0.0}


def emit_heartbeat(watcher: "S3SyncWatcher"):
    """Émet un heartbeat server_heartbeat sur Kafka."""
    try:
        entries = [
            e for e in os.listdir(watcher.inbox)
            if e.startswith("session_") and
            os.path.isdir(os.path.join(watcher.inbox, e))
        ]
    except Exception:
        entries = []

    with watcher._lock:
        done_count     = len(watcher._done)
        fail_count     = len(watcher._fail)
        uploading_count = len(watcher._uploading)

    total    = len(entries)
    pending  = total - done_count - fail_count - uploading_count
    pending  = max(pending, 0)

    disk = _disk_info(watcher.inbox)

    kafka_emit(
        source           = "server_heartbeat",
        server_id        = SERVER_ID,
        hostname         = socket.gethostname(),

        inbox_dir             = watcher.inbox,
        inbox_sessions_total  = total,
        inbox_sessions_pending    = pending,
        inbox_sessions_uploading  = uploading_count,
        inbox_sessions_done       = done_count,
        inbox_sessions_failed     = fail_count,

        disk_inbox_free_gb  = disk["free_gb"],
        disk_inbox_total_gb = disk["total_gb"],
        disk_inbox_used_pct = disk["used_pct"],

        s3_sync_active = True,
        s3_bucket      = watcher.bucket,
        s3_prefix      = watcher.prefix,
        dry_run        = watcher.dry_run,

        kafka_ok  = HAS_KAFKA,
        uptime_s  = round(time.time() - watcher._start_time, 1),
    )


# ── S3 ────────────────────────────────────────────────────────────────────────

def _get_s3_client():
    """Crée un client S3. Les credentials viennent de ~/.aws/credentials ou
    des variables d'env AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY."""
    if not HAS_BOTO3:
        raise RuntimeError("boto3 n'est pas installé (pip3 install boto3)")
    return boto3.client("s3")


def _sha256_file(path: str, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _collect_files(session_dir: str) -> list[str]:
    """Retourne tous les fichiers (chemins relatifs) d'une session."""
    files = []
    for root, _, fnames in os.walk(session_dir):
        for fname in fnames:
            abs_path = os.path.join(root, fname)
            rel_path = os.path.relpath(abs_path, session_dir)
            files.append(rel_path)
    return sorted(files)


def _is_stable(session_dir: str, stable_seconds: int) -> bool:
    """Retourne True si aucun fichier n'a été modifié depuis stable_seconds."""
    cutoff = time.time() - stable_seconds
    for root, _, fnames in os.walk(session_dir):
        for fname in fnames:
            try:
                mtime = os.path.getmtime(os.path.join(root, fname))
                if mtime > cutoff:
                    return False
            except OSError:
                return False
    return True


def _upload_file(s3, session_dir: str, rel_path: str,
                 bucket: str, s3_key: str, dry_run: bool) -> dict:
    """Upload un fichier et retourne un dict avec résultat."""
    local_path = os.path.join(session_dir, rel_path)
    size_bytes  = os.path.getsize(local_path)
    t0 = time.time()

    if dry_run:
        log.info("  [DRY] s3://%s/%s", bucket, s3_key)
        return {"rel": rel_path, "ok": True, "size_bytes": size_bytes, "speed_mbps": 0.0, "dry": True}

    try:
        s3.upload_file(local_path, bucket, s3_key)
        elapsed = time.time() - t0
        speed   = (size_bytes / 1e6) / elapsed if elapsed > 0 else 0.0
        log.info("  ✔ %-45s  %.1f MB  %.1f MB/s", rel_path, size_bytes / 1e6, speed)
        return {"rel": rel_path, "ok": True, "size_bytes": size_bytes,
                "speed_mbps": round(speed, 2), "s3_key": s3_key}
    except Exception as e:
        log.error("  ✘ %s : %s", rel_path, e)
        return {"rel": rel_path, "ok": False, "error": str(e)}


def upload_session(session_dir: str, bucket: str, prefix: str,
                   workers: int, dry_run: bool, delete_after: bool) -> bool:
    """Upload une session complète vers S3. Retourne True si tout est OK."""
    session_id = os.path.basename(session_dir)
    files      = _collect_files(session_dir)
    total      = len(files)

    log.info("[%s] Début upload — %d fichiers → s3://%s/%s/%s/",
             session_id, total, bucket, prefix, session_id)

    kafka_emit(step="upload", status="started", session_id=session_id,
               file_count=total, bucket=bucket,
               s3_prefix=f"{prefix}/{session_id}/")

    s3 = _get_s3_client()
    results   = []
    t_start   = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for rel in files:
            s3_key = f"{prefix}/{session_id}/{rel}"
            f = pool.submit(_upload_file, s3, session_dir, rel, bucket, s3_key, dry_run)
            futures[f] = rel

        for f in as_completed(futures):
            results.append(f.result())

    elapsed     = time.time() - t_start
    ok_files    = [r for r in results if r.get("ok")]
    fail_files  = [r for r in results if not r.get("ok")]
    total_bytes = sum(r.get("size_bytes", 0) for r in ok_files)
    avg_speed   = (total_bytes / 1e6) / elapsed if elapsed > 0 else 0.0

    if fail_files:
        log.error("[%s] Upload partiel — %d/%d échoués : %s",
                  session_id, len(fail_files), total,
                  [r["rel"] for r in fail_files])
        kafka_emit(step="upload", status="partial_failure", session_id=session_id,
                   files_ok=len(ok_files), files_failed=len(fail_files),
                   failed_files=[r["rel"] for r in fail_files])
        return False

    log.info("[%s] Upload terminé — %d fichiers  %.1f MB  %.1f MB/s  %.1fs",
             session_id, total, total_bytes / 1e6, avg_speed, elapsed)

    kafka_emit(step="upload", status="completed", session_id=session_id,
               files_uploaded=total, files_total=total,
               total_bytes=total_bytes,
               total_mb=round(total_bytes / 1e6, 2),
               avg_speed_mbps=round(avg_speed, 2),
               elapsed_s=round(elapsed, 1),
               bucket=bucket,
               s3_prefix=f"{prefix}/{session_id}/",
               dry_run=dry_run)

    if delete_after and not dry_run:
        import shutil
        shutil.rmtree(session_dir)
        log.info("[%s] Dossier local supprimé après upload", session_id)

    return True


# ── Watcher ───────────────────────────────────────────────────────────────────

class S3SyncWatcher:
    def __init__(self, inbox: str, bucket: str, prefix: str,
                 interval: int, stable: int,
                 workers: int, dry_run: bool, delete_after: bool):
        self.inbox        = inbox
        self.bucket       = bucket
        self.prefix       = prefix
        self.interval     = interval
        self.stable       = stable
        self.workers      = workers
        self.dry_run      = dry_run
        self.delete_after = delete_after

        self._done:      set[str] = set()   # sessions uploadées avec succès
        self._fail:      set[str] = set()   # sessions en erreur
        self._uploading: set[str] = set()   # sessions en cours d'upload
        self._lock       = threading.Lock()
        self._pool       = ThreadPoolExecutor(max_workers=2)
        self._start_time = time.time()

    def _scan(self):
        try:
            entries = os.listdir(self.inbox)
        except Exception as e:
            log.error("Impossible de lire %s : %s", self.inbox, e)
            return

        for name in sorted(entries):
            if not name.startswith("session_"):
                continue
            session_dir = os.path.join(self.inbox, name)
            if not os.path.isdir(session_dir):
                continue

            with self._lock:
                if name in self._done or name in self._fail:
                    continue

            if not _is_stable(session_dir, self.stable):
                log.debug("[%s] Pas encore stable, attente…", name)
                continue

            with self._lock:
                self._uploading.add(name)

            log.info("[%s] Session stable → lancement upload", name)
            self._pool.submit(self._handle_session, name, session_dir)

    def _handle_session(self, name: str, session_dir: str):
        try:
            ok = upload_session(
                session_dir,
                self.bucket,
                self.prefix,
                self.workers,
                self.dry_run,
                self.delete_after,
            )
            with self._lock:
                self._uploading.discard(name)
                if ok:
                    self._done.add(name)
                else:
                    self._fail.add(name)
        except Exception as e:
            log.error("[%s] Exception : %s", name, e)
            with self._lock:
                self._uploading.discard(name)
                self._fail.add(name)

    def _heartbeat_loop(self):
        while True:
            try:
                emit_heartbeat(self)
            except Exception as e:
                log.warning("Heartbeat échoué : %s", e)
            time.sleep(HEARTBEAT_INTERVAL)

    def run(self):
        log.info("Démarrage — inbox=%s  bucket=s3://%s/%s  stable=%ds  interval=%ds",
                 self.inbox, self.bucket, self.prefix, self.stable, self.interval)
        if self.dry_run:
            log.info("[DRY-RUN] Aucun fichier ne sera uploadé ni supprimé")
        kafka_emit(step="daemon", status="started",
                   inbox=self.inbox, bucket=self.bucket, prefix=self.prefix)

        hb = threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat")
        hb.start()
        log.info("Heartbeat démarré (toutes les %ds)", HEARTBEAT_INTERVAL)

        while True:
            self._scan()
            time.sleep(self.interval)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync inbox → S3 bronze")
    parser.add_argument("--inbox",    default=INBOX_DIR)
    parser.add_argument("--bucket",   default=S3_BUCKET)
    parser.add_argument("--prefix",   default=S3_PREFIX)
    parser.add_argument("--interval", type=int,   default=SCAN_INTERVAL,
                        help="Secondes entre deux scans (défaut: 5)")
    parser.add_argument("--stable",   type=int,   default=STABLE_SECONDS,
                        help="Secondes de stabilité avant upload (défaut: 10)")
    parser.add_argument("--workers",  type=int,   default=UPLOAD_WORKERS,
                        help="Threads d'upload par session (défaut: 4)")
    parser.add_argument("--delete-after-upload", action="store_true",
                        help="Supprimer le dossier local après upload réussi")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Simuler sans uploader ni supprimer")
    parser.add_argument("--broker",   default=KAFKA_BROKER)
    args = parser.parse_args()

    global _kafka_broker
    _kafka_broker = args.broker

    if not HAS_BOTO3:
        log.error("boto3 non installé — pip3 install boto3")
        sys.exit(1)

    # Vérification credentials AWS au démarrage
    try:
        boto3.client("sts").get_caller_identity()
        log.info("Credentials AWS OK")
    except NoCredentialsError:
        log.error("Credentials AWS manquants — configurer ~/.aws/credentials ou AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
        sys.exit(1)
    except Exception as e:
        log.warning("Impossible de vérifier les credentials AWS : %s", e)

    watcher = S3SyncWatcher(
        inbox        = args.inbox,
        bucket       = args.bucket,
        prefix       = args.prefix,
        interval     = args.interval,
        stable       = args.stable,
        workers      = args.workers,
        dry_run      = args.dry_run,
        delete_after = args.delete_after_upload,
    )
    watcher.run()


if __name__ == "__main__":
    main()
