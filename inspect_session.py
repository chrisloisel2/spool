#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inspect_session.py
==================
Inspection complète d'une session robotique.

Pipeline :
  1. Lit la queue RabbitMQ  ingestion_queue
  2. Pour chaque message : localise le dossier de session
  3. Vérifie l'intégrité structurelle et la cohérence des données
  4. Uploade sur le NAS (SFTP atomique)
  5. Publie des events Kafka ultra-détaillés à chaque étape
  6. ACK RabbitMQ si tout OK, NACK sinon

Usage :
  python3 inspect_session.py
  python3 inspect_session.py --once            # traite UN message puis quitte
  python3 inspect_session.py --local-dir /path/to/session  # injection directe (sans RabbitMQ)
"""

import os
import re
import sys
import csv
import json
import time
import stat
import socket
import hashlib
import logging
import argparse
import posixpath
import threading
import traceback
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── deps optionnels ────────────────────────────────────────────────────────────

try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# ── RabbitMQ ──────────────────────────────────────────────────────────────────
RABBITMQ_HOST   = os.environ.get("RABBITMQ_HOST",  "192.168.88.246")
RABBITMQ_PORT   = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER   = os.environ.get("RABBITMQ_USER",  "admin")
RABBITMQ_PASS   = os.environ.get("RABBITMQ_PASS",  "Admin123456!")
RABBITMQ_VHOST  = os.environ.get("RABBITMQ_VHOST", "/")
RABBITMQ_QUEUE  = os.environ.get("RABBITMQ_QUEUE", "ingestion_queue")
SCENARIOS_QUEUE = os.environ.get("SCENARIOS_QUEUE", "scenarios_queue")

# ── NAS SFTP ──────────────────────────────────────────────────────────────────
NAS_HOST        = os.environ.get("NAS_HOST",   "192.168.88.248")
NAS_PORT        = int(os.environ.get("NAS_PORT",  "22"))
NAS_USER        = os.environ.get("NAS_USER",   "EXORIA")
NAS_PASS        = os.environ.get("NAS_PASS",   "NasExori@2026!!#")
NAS_BASE_DIR    = "/DB-EXORIA/lakehouse"
NAS_LANDING     = "bronze/landing"          # sessions OK
NAS_QUARANTINE  = "bronze/quarantine"       # sessions KO

SSH_TIMEOUT     = 20
BANNER_TIMEOUT  = 90
AUTH_TIMEOUT    = 30
KEEPALIVE_SEC   = 30

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.environ.get("KAFKA_BROKER", "192.168.88.4:9092")
KAFKA_TOPIC     = "topic2"

# ── Session locale ────────────────────────────────────────────────────────────
# Dossier racine où sont stockées les sessions sur cette machine.
# Les messages RabbitMQ doivent contenir  {"session_dir": "/chemin/absolu/..."}
# OU {"session_id": "20260308_161838"} → on cherche dans SESSIONS_ROOT.
SESSIONS_ROOT   = os.environ.get("SESSIONS_ROOT", "/srv/exoria/sessions")

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
)
log = logging.getLogger("inspect")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def now_ts() -> float:
    return time.time()

def sha256_file(path: str, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for blk in iter(lambda: f.read(chunk), b""):
            h.update(blk)
    return h.hexdigest()

def file_size(path: str) -> int:
    return os.path.getsize(path)

def human_mb(n: int) -> float:
    return round(n / (1024 * 1024), 3)

def tcp_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    rc = s.connect_ex((host, port))
    s.close()
    return rc == 0

# ══════════════════════════════════════════════════════════════════════════════
# KAFKA EMITTER
# ══════════════════════════════════════════════════════════════════════════════

class KafkaEmitter:
    """Thread-safe Kafka producer avec reconnexion automatique."""

    def __init__(self):
        self._p = None
        self._lock = threading.Lock()

    def _ensure(self):
        if not HAS_KAFKA:
            return
        if self._p:
            return
        with self._lock:
            if self._p:
                return
            self._p = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
                api_version=(2, 0, 0),
                retries=5,
                acks="all",
                linger_ms=5,
                request_timeout_ms=10000,
            )

    def emit(self, step: str, status: str, session_id: str = "", **fields) -> None:
        """Publie un event structuré sur Kafka et le logue en DEBUG."""
        ev = {
            "source":     "inspect_session",
            "ts":         now_ts(),
            "ts_iso":     now_iso(),
            "step":       step,
            "status":     status,
            "session_id": session_id,
            **fields,
        }
        log.debug("[KAFKA] step=%s status=%s sid=%s extra=%s", step, status, session_id, fields)
        if not HAS_KAFKA:
            return
        try:
            self._ensure()
            if not self._p:
                return
            self._p.send(KAFKA_TOPIC, ev)
            self._p.flush(timeout=5)
        except Exception as e:
            log.warning("[KAFKA] emit failed: %s", e)

KAFKA = KafkaEmitter()

# ══════════════════════════════════════════════════════════════════════════════
# NAS CLIENT
# ══════════════════════════════════════════════════════════════════════════════

class NASClient:
    def __init__(self):
        self.ssh  = None
        self.sftp = None

    def _alive(self) -> bool:
        try:
            if not self.ssh or not self.sftp:
                return False
            t = self.ssh.get_transport()
            return bool(t and t.is_active())
        except Exception:
            return False

    def connect(self) -> None:
        if not HAS_PARAMIKO:
            raise RuntimeError("paramiko non installé — pip install paramiko")
        if not tcp_ok(NAS_HOST, NAS_PORT):
            raise ConnectionError(f"NAS {NAS_HOST}:{NAS_PORT} injoignable")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname     = NAS_HOST,
            port         = NAS_PORT,
            username     = NAS_USER,
            password     = NAS_PASS,
            look_for_keys= False,
            allow_agent  = False,
            timeout      = SSH_TIMEOUT,
            banner_timeout   = BANNER_TIMEOUT,
            auth_timeout     = AUTH_TIMEOUT,
        )
        tr = ssh.get_transport()
        if tr:
            tr.set_keepalive(KEEPALIVE_SEC)
        self.ssh  = ssh
        self.sftp = ssh.open_sftp()
        log.info("[NAS] Connexion établie %s:%s", NAS_HOST, NAS_PORT)

    def ensure(self) -> None:
        if not self._alive():
            self.connect()

    def close(self) -> None:
        for attr in ("sftp", "ssh"):
            try:
                obj = getattr(self, attr, None)
                if obj:
                    obj.close()
            except Exception:
                pass
        self.ssh = self.sftp = None

    def exists(self, path: str) -> bool:
        try:
            self.sftp.stat(path)
            return True
        except Exception:
            return False

    def mkdir_p(self, remote: str) -> None:
        remote = remote.replace("\\", "/")
        cur = ""
        for part in remote.split("/"):
            if not part:
                continue
            cur += "/" + part
            if not self.exists(cur):
                self.sftp.mkdir(cur)

    def put_atomic(self, local: str, remote: str) -> Tuple[str, float]:
        """Upload local → remote.part puis rename. Retourne (remote_final, MBps)."""
        remote = remote.replace("\\", "/")
        self.mkdir_p(posixpath.dirname(remote))
        size  = file_size(local)
        tmp   = remote + ".part"
        t0    = time.monotonic()
        self.sftp.put(local, tmp)
        elapsed = max(time.monotonic() - t0, 0.001)
        speed = round(human_mb(size) / elapsed, 2)
        final = remote
        if self.exists(final):
            i = 1
            while self.exists(f"{remote}.dup{i}"):
                i += 1
            final = f"{remote}.dup{i}"
        self.sftp.rename(tmp, final)
        return final, speed

NAS = NASClient()

# ══════════════════════════════════════════════════════════════════════════════
# STRUCTURE ATTENDUE D'UNE SESSION
# ══════════════════════════════════════════════════════════════════════════════

REQUIRED_FILES = [
    "metadata.json",
    "tracker_positions.csv",
    "gripper_left_data.csv",
    "gripper_right_data.csv",
]

REQUIRED_VIDEOS = [
    "videos/head.mp4",
    "videos/left.mp4",
    "videos/right.mp4",
    "videos/head.jsonl",
    "videos/left.jsonl",
    "videos/right.jsonl",
]

# ══════════════════════════════════════════════════════════════════════════════
# INSPECTION
# ══════════════════════════════════════════════════════════════════════════════

class SessionInspector:
    """Inspecte un dossier de session et retourne un rapport complet."""

    def __init__(self, session_dir: str):
        self.dir = session_dir
        self.sid = os.path.basename(session_dir)  # ex: session_20260308_161838
        self.report: Dict[str, Any] = {
            "session_id": self.sid,
            "session_dir": session_dir,
            "started_at": now_iso(),
            "checks": {},
            "files": {},
            "ok": False,
            "errors": [],
        }

    # ── helpers internes ──────────────────────────────────────────────────────

    def _path(self, *parts) -> str:
        return os.path.join(self.dir, *parts)

    def _err(self, msg: str) -> None:
        log.error("[INSPECT %s] %s", self.sid, msg)
        self.report["errors"].append(msg)

    def _check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.report["checks"][name] = {"ok": ok, "detail": detail}
        lvl = logging.INFO if ok else logging.ERROR
        log.log(lvl, "[INSPECT %s] check=%-40s ok=%s  %s", self.sid, name, ok, detail)
        return ok

    # ── sous-vérifications ────────────────────────────────────────────────────

    def check_dir_exists(self) -> bool:
        ok = os.path.isdir(self.dir)
        return self._check("dir_exists", ok, self.dir)

    def check_required_files(self) -> bool:
        all_ok = True
        for rel in REQUIRED_FILES + REQUIRED_VIDEOS:
            p = self._path(*rel.split("/"))
            exists = os.path.isfile(p)
            sz = file_size(p) if exists else 0
            self._check(f"file_present:{rel}", exists, f"size={sz}B" if exists else "MISSING")
            if not exists:
                all_ok = False
                self._err(f"Fichier manquant : {rel}")
            else:
                self.report["files"][rel] = {"path": p, "size_bytes": sz, "size_mb": human_mb(sz)}
        return all_ok

    def check_metadata(self) -> bool:
        p = self._path("metadata.json")
        if not os.path.isfile(p):
            return self._check("metadata_valid", False, "fichier absent")
        try:
            with open(p, encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            return self._check("metadata_valid", False, f"JSON invalide: {e}")

        required_keys = ["session_id", "scenario", "start_time", "end_time",
                         "duration_seconds", "cameras", "trackers", "failed"]
        missing = [k for k in required_keys if k not in meta]
        if missing:
            return self._check("metadata_valid", False, f"clés manquantes: {missing}")

        failed = meta.get("failed", True)
        duration = meta.get("duration_seconds", 0)

        self.report["metadata"] = {
            "scenario":         meta.get("scenario"),
            "start_time":       meta.get("start_time"),
            "end_time":         meta.get("end_time"),
            "duration_seconds": duration,
            "failed":           failed,
            "cameras_count":    len(meta.get("cameras", {})),
            "trackers_count":   len(meta.get("trackers", {})),
            "video_config":     meta.get("video_config", {}),
        }

        if failed:
            self._err("metadata.json indique failed=true")
        if duration <= 0:
            self._err(f"Durée invalide: {duration}s")

        ok = (not failed) and (duration > 0) and (not missing)
        return self._check("metadata_valid", ok,
                           f"scenario={meta.get('scenario')} duration={duration:.1f}s failed={failed}")

    def check_csv(self, rel: str, min_rows: int = 2) -> bool:
        p = self._path(*rel.split("/"))
        if not os.path.isfile(p):
            return self._check(f"csv_valid:{rel}", False, "absent")
        try:
            with open(p, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if len(rows) < min_rows + 1:  # +1 pour header
                return self._check(f"csv_valid:{rel}", False,
                                   f"trop peu de lignes: {len(rows)} (min {min_rows+1})")
            header = rows[0]
            data_rows = len(rows) - 1
            self.report["files"].setdefault(rel, {})["csv_rows"] = data_rows
            self.report["files"].setdefault(rel, {})["csv_cols"] = len(header)
            return self._check(f"csv_valid:{rel}", True,
                               f"rows={data_rows} cols={len(header)} header={header[:4]}")
        except Exception as e:
            return self._check(f"csv_valid:{rel}", False, str(e))

    def check_jsonl(self, rel: str, min_entries: int = 10) -> bool:
        p = self._path(*rel.split("/"))
        if not os.path.isfile(p):
            return self._check(f"jsonl_valid:{rel}", False, "absent")
        try:
            entries = []
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError as je:
                        raise RuntimeError(f"Ligne JSONL invalide : {je} — {line[:80]!r}")
            count = len(entries)
            ok = count >= min_entries
            self.report["files"].setdefault(rel, {})["jsonl_entries"] = count
            return self._check(f"jsonl_valid:{rel}", ok,
                               f"entries={count} (min {min_entries})")
        except Exception as e:
            return self._check(f"jsonl_valid:{rel}", False, str(e))

    def check_video_size(self, rel: str, min_bytes: int = 1024 * 1024) -> bool:
        p = self._path(*rel.split("/"))
        if not os.path.isfile(p):
            return self._check(f"video_size:{rel}", False, "absent")
        sz = file_size(p)
        ok = sz >= min_bytes
        return self._check(f"video_size:{rel}", ok,
                           f"size={human_mb(sz):.2f}MB (min {human_mb(min_bytes):.2f}MB)")

    def check_frames_dir(self) -> bool:
        d = self._path("frames")
        if not os.path.isdir(d):
            # frames est optionnel — pas une erreur bloquante
            return self._check("frames_dir", True, "absent (optionnel)")
        items = os.listdir(d)
        return self._check("frames_dir", True, f"entries={len(items)}")

    def check_sha256(self) -> Dict[str, str]:
        """Calcule le SHA256 de chaque fichier listé dans report['files']."""
        hashes: Dict[str, str] = {}
        for rel, info in self.report["files"].items():
            p = info.get("path") or self._path(*rel.split("/"))
            if not os.path.isfile(p):
                continue
            KAFKA.emit("sha256_compute", "started",
                       session_id=self.sid, file=rel, size_mb=info.get("size_mb", 0))
            h = sha256_file(p)
            hashes[rel] = h
            self.report["files"][rel]["sha256"] = h
            KAFKA.emit("sha256_compute", "done",
                       session_id=self.sid, file=rel, sha256=h)
            log.info("[INSPECT %s] sha256 %s = %s", self.sid, rel, h)
        return hashes

    # ── point d'entrée ────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        sid = self.sid
        log.info("[INSPECT %s] ══ Démarrage inspection ══", sid)
        KAFKA.emit("inspection", "started",
                   session_id=sid, session_dir=self.dir)

        # 1. Existence du dossier
        if not self.check_dir_exists():
            self.report["ok"] = False
            KAFKA.emit("inspection", "aborted",
                       session_id=sid, reason="session_dir_not_found")
            return self.report

        # 2. Fichiers obligatoires
        KAFKA.emit("check", "required_files_start", session_id=sid)
        files_ok = self.check_required_files()
        KAFKA.emit("check", "required_files_done",
                   session_id=sid, ok=files_ok,
                   total_size_mb=sum(
                       v.get("size_mb", 0) for v in self.report["files"].values()))

        # 3. Métadonnées
        KAFKA.emit("check", "metadata_start", session_id=sid)
        meta_ok = self.check_metadata()
        KAFKA.emit("check", "metadata_done",
                   session_id=sid, ok=meta_ok,
                   metadata=self.report.get("metadata", {}))

        # 4. CSVs
        for csv_rel in ["tracker_positions.csv",
                        "gripper_left_data.csv",
                        "gripper_right_data.csv"]:
            KAFKA.emit("check", "csv_start", session_id=sid, file=csv_rel)
            csv_ok = self.check_csv(csv_rel, min_rows=5)
            KAFKA.emit("check", "csv_done",
                       session_id=sid, file=csv_rel, ok=csv_ok,
                       rows=self.report["files"].get(csv_rel, {}).get("csv_rows", 0))

        # 5. JSONLs vidéo
        for jsonl_rel in ["videos/head.jsonl", "videos/left.jsonl", "videos/right.jsonl"]:
            KAFKA.emit("check", "jsonl_start", session_id=sid, file=jsonl_rel)
            j_ok = self.check_jsonl(jsonl_rel, min_entries=10)
            KAFKA.emit("check", "jsonl_done",
                       session_id=sid, file=jsonl_rel, ok=j_ok,
                       entries=self.report["files"].get(jsonl_rel, {}).get("jsonl_entries", 0))

        # 6. Taille minimale des vidéos
        for mp4_rel in ["videos/head.mp4", "videos/left.mp4", "videos/right.mp4"]:
            self.check_video_size(mp4_rel, min_bytes=512 * 1024)

        # 7. Dossier frames
        self.check_frames_dir()

        # 8. SHA256 de tout
        KAFKA.emit("check", "sha256_all_start",
                   session_id=sid, file_count=len(self.report["files"]))
        hashes = self.check_sha256()
        KAFKA.emit("check", "sha256_all_done",
                   session_id=sid, file_count=len(hashes),
                   checksums=hashes)

        # Bilan
        failed_checks = [k for k, v in self.report["checks"].items() if not v["ok"]]
        ok = len(self.report["errors"]) == 0 and files_ok and meta_ok
        self.report["ok"] = ok
        self.report["ended_at"] = now_iso()
        self.report["failed_checks"] = failed_checks

        lvl = logging.INFO if ok else logging.ERROR
        log.log(lvl, "[INSPECT %s] ══ Résultat : %s (%d checks KO) ══",
                sid, "OK" if ok else "ÉCHEC", len(failed_checks))

        KAFKA.emit("inspection", "completed",
                   session_id=sid,
                   ok=ok,
                   failed_checks=failed_checks,
                   total_checks=len(self.report["checks"]),
                   errors=self.report["errors"],
                   metadata=self.report.get("metadata", {}))

        return self.report

# ══════════════════════════════════════════════════════════════════════════════
# UPLOAD NAS
# ══════════════════════════════════════════════════════════════════════════════

def build_nas_path(session_id: str, rel: str, zone: str = NAS_LANDING) -> str:
    """
    Construit le chemin NAS :
      /DB-EXORIA/lakehouse/bronze/landing/2026/03/08/<session_id>/<rel>
    """
    # extrait la date depuis session_id (session_YYYYMMDD_HHMMSS)
    m = re.search(r"(\d{4})(\d{2})(\d{2})", session_id)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
    else:
        now = dt.datetime.utcnow()
        y, mo, d = f"{now.year:04}", f"{now.month:02}", f"{now.day:02}"

    base = NAS_BASE_DIR.rstrip("/")
    return posixpath.join(base, zone, y, mo, d, session_id, rel)


def upload_session(report: Dict[str, Any], zone: str = NAS_LANDING) -> Dict[str, Any]:
    """
    Uploade tous les fichiers de la session sur le NAS.
    Retourne un dict {rel: {"remote": ..., "speed_mbps": ..., "sha256": ...}}
    """
    sid    = report["session_id"]
    sdir   = report["session_dir"]
    result: Dict[str, Any] = {}

    KAFKA.emit("upload", "started",
               session_id=sid, zone=zone,
               file_count=len(report["files"]))

    # Connexion NAS
    try:
        NAS.ensure()
    except Exception as e:
        KAFKA.emit("upload", "nas_connect_failed",
                   session_id=sid, error=str(e))
        raise

    KAFKA.emit("upload", "nas_connected",
               session_id=sid,
               nas_host=NAS_HOST, nas_port=NAS_PORT)

    # Upload fichier par fichier
    all_rels = list(report["files"].keys())
    total_bytes = sum(v.get("size_bytes", 0) for v in report["files"].values())

    log.info("[UPLOAD %s] %d fichiers à uploader (total %.2f MB)",
             sid, len(all_rels), human_mb(total_bytes))

    for idx, rel in enumerate(all_rels, 1):
        info = report["files"][rel]
        local = info.get("path") or os.path.join(sdir, *rel.split("/"))
        remote = build_nas_path(sid, rel, zone)
        sz = info.get("size_bytes", 0)
        sha = info.get("sha256", "")

        KAFKA.emit("upload", "file_start",
                   session_id=sid,
                   file_index=idx,
                   file_total=len(all_rels),
                   rel=rel,
                   local=local,
                   remote=remote,
                   size_bytes=sz,
                   size_mb=human_mb(sz),
                   sha256=sha)

        log.info("[UPLOAD %s] (%d/%d) %s → %s",
                 sid, idx, len(all_rels), rel, remote)

        try:
            t0 = time.monotonic()
            final_remote, speed = NAS.put_atomic(local, remote)
            elapsed = time.monotonic() - t0

            result[rel] = {
                "remote":     final_remote,
                "speed_mbps": speed,
                "size_bytes": sz,
                "sha256":     sha,
                "elapsed_s":  round(elapsed, 3),
                "ok":         True,
            }

            KAFKA.emit("upload", "file_done",
                       session_id=sid,
                       file_index=idx,
                       file_total=len(all_rels),
                       rel=rel,
                       remote=final_remote,
                       size_bytes=sz,
                       size_mb=human_mb(sz),
                       speed_mbps=speed,
                       elapsed_s=round(elapsed, 3),
                       sha256=sha)

            log.info("[UPLOAD %s] ✓ %s  %.2f MB/s  %.1fs",
                     sid, rel, speed, elapsed)

        except Exception as e:
            err = traceback.format_exc()
            result[rel] = {"ok": False, "error": str(e)}
            KAFKA.emit("upload", "file_error",
                       session_id=sid,
                       rel=rel,
                       error=str(e),
                       traceback=err)
            log.error("[UPLOAD %s] ✗ %s : %s", sid, rel, e)
            raise  # on propage — la session entière doit aller en quarantaine

    # Manifeste de session
    manifest = {
        "session_id":   sid,
        "uploaded_at":  now_iso(),
        "zone":         zone,
        "files":        result,
        "inspection":   report.get("metadata", {}),
        "checksums":    {rel: v.get("sha256", "") for rel, v in result.items()},
    }
    manifest_local = os.path.join("/tmp", f"{sid}_manifest.json")
    with open(manifest_local, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    manifest_remote = build_nas_path(sid, "_manifest.json", zone)
    try:
        NAS.put_atomic(manifest_local, manifest_remote)
        KAFKA.emit("upload", "manifest_uploaded",
                   session_id=sid, remote=manifest_remote)
        log.info("[UPLOAD %s] Manifeste NAS : %s", sid, manifest_remote)
    except Exception as e:
        log.warning("[UPLOAD %s] Manifeste non uploadé : %s", sid, e)
    finally:
        try:
            os.remove(manifest_local)
        except Exception:
            pass

    total_speed = round(
        sum(v.get("speed_mbps", 0) for v in result.values() if v.get("ok")) /
        max(sum(1 for v in result.values() if v.get("ok")), 1), 2
    )

    KAFKA.emit("upload", "completed",
               session_id=sid,
               zone=zone,
               files_uploaded=sum(1 for v in result.values() if v.get("ok")),
               files_total=len(result),
               total_bytes=total_bytes,
               total_mb=human_mb(total_bytes),
               avg_speed_mbps=total_speed,
               manifest_remote=manifest_remote)

    log.info("[UPLOAD %s] Upload terminé — %d fichiers, %.2f MB, moy %.2f MB/s",
             sid, len(result), human_mb(total_bytes), total_speed)

    return result

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def process_session(session_dir: str) -> bool:
    """
    Pipeline complet pour un dossier de session.
    Retourne True si tout s'est bien passé.
    """
    sid = os.path.basename(session_dir)

    KAFKA.emit("pipeline", "started",
               session_id=sid, session_dir=session_dir)
    log.info("[PIPELINE %s] ════ Début du pipeline ════", sid)

    # ── ÉTAPE 1 : Inspection ──────────────────────────────────────────────────
    log.info("[PIPELINE %s] Étape 1 : Inspection locale", sid)
    inspector = SessionInspector(session_dir)
    report = inspector.run()

    if not report["ok"]:
        log.error("[PIPELINE %s] Inspection KO — mise en quarantaine NAS", sid)
        KAFKA.emit("pipeline", "inspection_failed",
                   session_id=sid,
                   errors=report["errors"],
                   failed_checks=report.get("failed_checks", []))
        # Upload en quarantaine quand même (avec les fichiers présents)
        try:
            upload_session(report, zone=NAS_QUARANTINE)
        except Exception as e:
            KAFKA.emit("pipeline", "quarantine_upload_failed",
                       session_id=sid, error=str(e))
            log.error("[PIPELINE %s] Quarantaine NAS échouée : %s", sid, e)
        return False

    KAFKA.emit("pipeline", "inspection_passed",
               session_id=sid,
               checks_count=len(report["checks"]),
               metadata=report.get("metadata", {}))
    log.info("[PIPELINE %s] Inspection OK", sid)

    # ── ÉTAPE 2 : Upload NAS (landing) ────────────────────────────────────────
    log.info("[PIPELINE %s] Étape 2 : Upload NAS → %s", sid, NAS_LANDING)
    try:
        upload_result = upload_session(report, zone=NAS_LANDING)
    except Exception as e:
        log.error("[PIPELINE %s] Upload NAS échoué : %s", sid, e)
        KAFKA.emit("pipeline", "upload_failed",
                   session_id=sid, error=str(e))
        return False

    # ── ÉTAPE 3 : Vérification post-upload ────────────────────────────────────
    failed_uploads = [rel for rel, v in upload_result.items() if not v.get("ok")]
    if failed_uploads:
        KAFKA.emit("pipeline", "upload_partial_failure",
                   session_id=sid, failed_files=failed_uploads)
        log.error("[PIPELINE %s] Fichiers non uploadés : %s", sid, failed_uploads)
        return False

    # ── Succès ────────────────────────────────────────────────────────────────
    KAFKA.emit("pipeline", "completed",
               session_id=sid,
               zone=NAS_LANDING,
               files_count=len(upload_result),
               metadata=report.get("metadata", {}),
               upload_summary={
                   rel: {
                       "remote":     v.get("remote"),
                       "speed_mbps": v.get("speed_mbps"),
                       "sha256":     v.get("sha256"),
                   }
                   for rel, v in upload_result.items()
               })

    log.info("[PIPELINE %s] ════ Pipeline terminé avec succès ════", sid)
    return True

# ══════════════════════════════════════════════════════════════════════════════
# RABBITMQ CONSUMER
# ══════════════════════════════════════════════════════════════════════════════

def resolve_session_dir(body: Dict[str, Any]) -> Optional[str]:
    """
    Détermine le chemin local du dossier de session depuis le message RabbitMQ.

    Le message peut contenir :
      - "session_dir"  : chemin absolu direct
      - "session_id"   : on cherche dans SESSIONS_ROOT
                         ex: "20260308_161838" → /srv/exoria/sessions/session_20260308_161838
      - "path"         : alias de session_dir
    """
    if "session_dir" in body:
        return body["session_dir"]
    if "path" in body:
        return body["path"]
    if "session_id" in body:
        sid = body["session_id"]
        candidates = [
            os.path.join(SESSIONS_ROOT, f"session_{sid}"),
            os.path.join(SESSIONS_ROOT, sid),
        ]
        for c in candidates:
            if os.path.isdir(c):
                return c
        log.error("Session '%s' introuvable dans %s", sid, SESSIONS_ROOT)
    return None


RABBITMQ_RECONNECT_DELAY_MAX = 60   # secondes, plafond exponentiel

def start_rabbitmq_consumer(once: bool = False) -> None:
    if not HAS_PIKA:
        raise RuntimeError("pika non installé — pip install pika")

    creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=creds,
        heartbeat=60,
        blocked_connection_timeout=300,
    )

    def on_message(ch, method, properties, raw_body):
        delivery_tag = method.delivery_tag
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            log.error("[RABBITMQ] Message JSON invalide : %s — %r", e, raw_body[:200])
            ch.basic_nack(delivery_tag=delivery_tag, requeue=False)
            return

        sid = body.get("session_id") or body.get("session_dir", "?")
        log.info("[RABBITMQ] Message reçu delivery_tag=%d session=%s", delivery_tag, sid)

        KAFKA.emit("consumer", "message_received",
                   session_id=str(sid),
                   delivery_tag=delivery_tag,
                   body_keys=list(body.keys()))

        session_dir = resolve_session_dir(body)
        if session_dir is None:
            log.error("[RABBITMQ] Impossible de résoudre le dossier de session pour : %s", body)
            KAFKA.emit("consumer", "session_dir_unresolved",
                       session_id=str(sid), body=body)
            ch.basic_nack(delivery_tag=delivery_tag, requeue=False)
            return

        try:
            ok = process_session(session_dir)
        except Exception as e:
            log.error("[RABBITMQ] Exception non gérée pendant pipeline : %s\n%s",
                      e, traceback.format_exc())
            KAFKA.emit("consumer", "pipeline_exception",
                       session_id=str(sid), error=str(e))
            ok = False

        if ok:
            ch.basic_ack(delivery_tag=delivery_tag)
            KAFKA.emit("consumer", "message_acked",
                       session_id=str(sid), delivery_tag=delivery_tag)
            log.info("[RABBITMQ] ACK delivery_tag=%d", delivery_tag)
        else:
            ch.basic_nack(delivery_tag=delivery_tag, requeue=False)
            KAFKA.emit("consumer", "message_nacked",
                       session_id=str(sid), delivery_tag=delivery_tag)
            log.warning("[RABBITMQ] NACK delivery_tag=%d (requeue=False)", delivery_tag)

        if once:
            raise KeyboardInterrupt  # sortie propre après un seul message

    # ── Boucle de reconnexion avec backoff exponentiel ────────────────────────
    delay = 5
    attempt = 0
    while True:
        attempt += 1
        log.info("[RABBITMQ] Tentative de connexion #%d → %s:%s vhost=%s queue=%s",
                 attempt, RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_VHOST, RABBITMQ_QUEUE)
        KAFKA.emit("consumer", "connecting",
                   session_id="",
                   rabbitmq_host=RABBITMQ_HOST,
                   rabbitmq_queue=RABBITMQ_QUEUE,
                   attempt=attempt)
        conn = None
        try:
            conn = pika.BlockingConnection(params)
            ch   = conn.channel()

            # Création des queues si elles n'existent pas encore
            ch.queue_declare(queue=RABBITMQ_QUEUE,  durable=True)
            ch.queue_declare(queue=SCENARIOS_QUEUE, durable=True)
            log.info("[RABBITMQ] Queues déclarées : %s, %s", RABBITMQ_QUEUE, SCENARIOS_QUEUE)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message)

            delay = 5   # reset du backoff après connexion réussie
            log.info("[RABBITMQ] Connecté. En attente de messages sur '%s' ...", RABBITMQ_QUEUE)
            KAFKA.emit("consumer", "started",
                       session_id="",
                       rabbitmq_host=RABBITMQ_HOST,
                       rabbitmq_queue=RABBITMQ_QUEUE,
                       attempt=attempt)

            ch.start_consuming()

        except KeyboardInterrupt:
            log.info("[RABBITMQ] Arrêt demandé.")
            KAFKA.emit("consumer", "stopped", session_id="", reason="keyboard_interrupt")
            try:
                if conn and conn.is_open:
                    conn.close()
            except Exception:
                pass
            return  # sortie définitive

        except Exception as e:
            log.error("[RABBITMQ] Connexion perdue (tentative #%d) : %s — nouvelle tentative dans %ds",
                      attempt, e, delay)
            KAFKA.emit("consumer", "connection_lost",
                       session_id="",
                       attempt=attempt,
                       error=str(e),
                       retry_in_s=delay)
        finally:
            try:
                if conn and conn.is_open:
                    conn.close()
            except Exception:
                pass

        time.sleep(delay)
        delay = min(delay * 2, RABBITMQ_RECONNECT_DELAY_MAX)

# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inspection complète de sessions robotiques")
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--local-dir", metavar="DIR",
                     help="Traiter directement ce dossier de session (sans RabbitMQ)")
    grp.add_argument("--once", action="store_true",
                     help="Lire UN message RabbitMQ puis quitter")
    p.add_argument("--rabbitmq-host", default=RABBITMQ_HOST)
    p.add_argument("--rabbitmq-port", type=int, default=RABBITMQ_PORT)
    p.add_argument("--rabbitmq-user", default=RABBITMQ_USER)
    p.add_argument("--rabbitmq-pass", default=RABBITMQ_PASS)
    p.add_argument("--kafka-broker",  default=KAFKA_BROKER)
    p.add_argument("--nas-host",      default=NAS_HOST)
    p.add_argument("--nas-pass",      default=NAS_PASS)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Surcharge globals depuis CLI
    global RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
    global KAFKA_BROKER, NAS_HOST, NAS_PASS
    RABBITMQ_HOST  = args.rabbitmq_host
    RABBITMQ_PORT  = args.rabbitmq_port
    RABBITMQ_USER  = args.rabbitmq_user
    RABBITMQ_PASS  = args.rabbitmq_pass
    KAFKA_BROKER   = args.kafka_broker
    NAS_HOST       = args.nas_host
    NAS_PASS       = args.nas_pass

    KAFKA.emit("app", "start",
               session_id="",
               has_kafka=HAS_KAFKA,
               has_pika=HAS_PIKA,
               has_paramiko=HAS_PARAMIKO,
               kafka_broker=KAFKA_BROKER,
               nas_host=NAS_HOST,
               rabbitmq_host=RABBITMQ_HOST,
               rabbitmq_queue=RABBITMQ_QUEUE)

    if args.local_dir:
        # Mode direct : pas besoin de RabbitMQ
        ok = process_session(os.path.abspath(args.local_dir))
        sys.exit(0 if ok else 1)
    else:
        # Mode consumer RabbitMQ (continu ou --once)
        if not HAS_PIKA:
            log.error("pika requis pour le mode RabbitMQ : pip install pika")
            sys.exit(2)
        start_rabbitmq_consumer(once=args.once)


if __name__ == "__main__":
    main()
