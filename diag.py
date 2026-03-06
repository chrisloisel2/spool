#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
nas_diag.py
Diagnostic SFTP/NAS ultra-complet.

Objectif :
- vérifier exactement ce que le NAS accepte / refuse
- produire un rapport JSON détaillé
- rester strictement cantonné à un répertoire de test

Exemples :
  python3 nas_diag.py --host 192.168.88.248 --port 22 --user EXORIA --password '***' \
      --base-dir /DB-EXORIA/lakehouse --test-root bronze/_diag \
      --report-file nas_diag_report.json

  python3 nas_diag.py --host 192.168.88.248 --user EXORIA --password '***' \
      --base-dir /DB-EXORIA/lakehouse --parallel 4 --max-upload-mb 64

Dépendances :
  pip install paramiko
"""

import os
import sys
import io
import re
import json
import time
import stat
import socket
import hashlib
import random
import string
import shutil
import logging
import argparse
import tempfile
import traceback
import threading
import datetime as dt
import posixpath
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

try:
    import paramiko
except ImportError:
    print("ERREUR: paramiko n'est pas installé. Fais: pip install paramiko", file=sys.stderr)
    sys.exit(2)


# ============================================================
# LOG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
)
log = logging.getLogger("nas-diag")


# ============================================================
# OUTILS
# ============================================================

def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def random_ascii_name(n: int) -> str:
    alphabet = string.ascii_letters + string.digits + "-_."
    return "".join(random.choice(alphabet) for _ in range(n))

def random_bytes(size: int, seed: Optional[int] = None) -> bytes:
    rng = random.Random(seed)
    return bytes(rng.getrandbits(8) for _ in range(size))

def ensure_local_temp_file(size_bytes: int, seed: int, suffix: str = ".bin") -> str:
    fd, path = tempfile.mkstemp(prefix="nasdiag_", suffix=suffix)
    os.close(fd)
    with open(path, "wb") as f:
        remaining = size_bytes
        chunk_size = 1024 * 1024
        while remaining > 0:
            n = min(chunk_size, remaining)
            f.write(random_bytes(n, seed))
            remaining -= n
            seed += 1
    return path

def safe_exc(e: Exception) -> Dict[str, Any]:
    return {
        "type": type(e).__name__,
        "message": str(e),
        "traceback": traceback.format_exc(limit=8),
    }

def mode_to_str(mode: Optional[int]) -> Optional[str]:
    if mode is None:
        return None
    try:
        return oct(stat.S_IMODE(mode))
    except Exception:
        return None

def human_mb(nbytes: int) -> float:
    return round(nbytes / (1024 * 1024), 2)

def tcp_connect_ex(host: str, port: int, timeout_s: float) -> Tuple[bool, Optional[int], Optional[str]]:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_s)
    try:
        rc = s.connect_ex((host, port))
        if rc == 0:
            return True, rc, None
        return False, rc, os.strerror(rc) if rc else None
    except Exception as e:
        return False, None, str(e)
    finally:
        try:
            s.close()
        except Exception:
            pass


# ============================================================
# STRUCTURES
# ============================================================

@dataclass
class TestResult:
    name: str
    category: str
    ok: bool
    started_at: str
    ended_at: str
    duration_ms: int
    details: Dict[str, Any]
    error: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class ResultCollector:
    def __init__(self):
        self.results: List[TestResult] = []
        self._lock = threading.Lock()

    def add(self, res: TestResult):
        with self._lock:
            self.results.append(res)

    def summary(self) -> Dict[str, Any]:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.ok)
        failed = total - passed
        by_category: Dict[str, Dict[str, int]] = {}
        for r in self.results:
            cat = by_category.setdefault(r.category, {"total": 0, "passed": 0, "failed": 0})
            cat["total"] += 1
            if r.ok:
                cat["passed"] += 1
            else:
                cat["failed"] += 1
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "by_category": by_category,
        }


# ============================================================
# CLIENT NAS
# ============================================================

class NASProbeClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        ssh_timeout: float,
        banner_timeout: float,
        auth_timeout: float,
        keepalive_sec: int,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssh_timeout = ssh_timeout
        self.banner_timeout = banner_timeout
        self.auth_timeout = auth_timeout
        self.keepalive_sec = keepalive_sec
        self.ssh: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

    def connect(self):
        if self.ssh or self.sftp:
            self.close()

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            look_for_keys=False,
            allow_agent=False,
            timeout=self.ssh_timeout,
            banner_timeout=self.banner_timeout,
            auth_timeout=self.auth_timeout,
        )
        tr = ssh.get_transport()
        if tr:
            tr.set_keepalive(self.keepalive_sec)
        sftp = ssh.open_sftp()
        self.ssh = ssh
        self.sftp = sftp

    def close(self):
        try:
            if self.sftp:
                self.sftp.close()
        except Exception:
            pass
        try:
            if self.ssh:
                self.ssh.close()
        except Exception:
            pass
        self.ssh = None
        self.sftp = None

    def ensure(self):
        if self.ssh is None or self.sftp is None:
            self.connect()

    def stat(self, path: str):
        self.ensure()
        return self.sftp.stat(path)

    def lstat(self, path: str):
        self.ensure()
        return self.sftp.lstat(path)

    def listdir(self, path: str):
        self.ensure()
        return self.sftp.listdir(path)

    def listdir_attr(self, path: str):
        self.ensure()
        return self.sftp.listdir_attr(path)

    def mkdir(self, path: str, mode: int = 0o755):
        self.ensure()
        self.sftp.mkdir(path, mode=mode)

    def rmdir(self, path: str):
        self.ensure()
        self.sftp.rmdir(path)

    def remove(self, path: str):
        self.ensure()
        self.sftp.remove(path)

    def rename(self, oldpath: str, newpath: str):
        self.ensure()
        self.sftp.rename(oldpath, newpath)

    def put(self, local: str, remote: str):
        self.ensure()
        self.sftp.put(local, remote)

    def get(self, remote: str, local: str):
        self.ensure()
        self.sftp.get(remote, local)

    def chmod(self, path: str, mode: int):
        self.ensure()
        self.sftp.chmod(path, mode)

    def utime(self, path: str, times: Tuple[int, int]):
        self.ensure()
        self.sftp.utime(path, times)

    def open_write(self, path: str):
        self.ensure()
        return self.sftp.open(path, "wb")

    def open_read(self, path: str):
        self.ensure()
        return self.sftp.open(path, "rb")

    def exists(self, path: str) -> bool:
        try:
            self.stat(path)
            return True
        except Exception:
            return False

    def mkdir_p(self, remote: str, mode: int = 0o755):
        remote = remote.replace("\\", "/")
        if not remote.startswith("/"):
            raise ValueError(f"mkdir_p attend un chemin absolu, reçu: {remote}")
        parts = remote.split("/")
        cur = ""
        for part in parts:
            if not part:
                continue
            cur += "/" + part
            if not self.exists(cur):
                self.mkdir(cur, mode=mode)


# ============================================================
# HARNESS
# ============================================================

class ProbeHarness:
    def __init__(self, args):
        self.args = args
        self.results = ResultCollector()
        self.client = NASProbeClient(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            ssh_timeout=args.ssh_timeout,
            banner_timeout=args.banner_timeout,
            auth_timeout=args.auth_timeout,
            keepalive_sec=args.keepalive_sec,
        )

        self.session_id = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "_" + random_ascii_name(8)
        self.test_root = posixpath.join(args.base_dir.rstrip("/"), args.test_root.strip("/"), self.session_id)
        self.created_remote_paths: List[str] = []
        self.created_local_paths: List[str] = []

    def record(self, name: str, category: str, fn):
        started_ts = now_iso()
        t0 = time.monotonic()
        try:
            details = fn() or {}
            res = TestResult(
                name=name,
                category=category,
                ok=True,
                started_at=started_ts,
                ended_at=now_iso(),
                duration_ms=int((time.monotonic() - t0) * 1000),
                details=details,
                error=None,
            )
        except Exception as e:
            res = TestResult(
                name=name,
                category=category,
                ok=False,
                started_at=started_ts,
                ended_at=now_iso(),
                duration_ms=int((time.monotonic() - t0) * 1000),
                details={},
                error=safe_exc(e),
            )
        self.results.add(res)
        level = logging.INFO if res.ok else logging.ERROR
        log.log(level, "[%s] %s :: %s", category, name, "OK" if res.ok else "FAIL")
        return res

    def cleanup(self):
        if self.args.keep_artifacts:
            return

        # Nettoyage local
        for path in reversed(self.created_local_paths):
            try:
                if os.path.isfile(path):
                    os.remove(path)
            except Exception:
                pass

        # Nettoyage distant
        try:
            self.client.ensure()
        except Exception:
            return

        files = []
        dirs = []
        for p in self.created_remote_paths:
            try:
                st = self.client.lstat(p)
                if stat.S_ISDIR(st.st_mode):
                    dirs.append(p)
                else:
                    files.append(p)
            except Exception:
                pass

        for p in sorted(files, key=lambda x: len(x), reverse=True):
            try:
                self.client.remove(p)
            except Exception:
                pass

        for p in sorted(dirs, key=lambda x: len(x), reverse=True):
            try:
                self.client.rmdir(p)
            except Exception:
                pass

        try:
            self.client.rmdir(self.test_root)
        except Exception:
            pass

    def mark_remote(self, path: str):
        if path not in self.created_remote_paths:
            self.created_remote_paths.append(path)

    def mark_local(self, path: str):
        if path not in self.created_local_paths:
            self.created_local_paths.append(path)

    def write_report(self):
        report = {
            "meta": {
                "generated_at": now_iso(),
                "tool": "nas_diag.py",
                "session_id": self.session_id,
                "target": {
                    "host": self.args.host,
                    "port": self.args.port,
                    "user": self.args.user,
                    "base_dir": self.args.base_dir,
                    "test_root": self.test_root,
                },
                "params": vars(self.args),
            },
            "summary": self.results.summary(),
            "results": [r.to_dict() for r in self.results.results],
            "acceptance_matrix": self.build_acceptance_matrix(),
        }

        with open(self.args.report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        return report

    def build_acceptance_matrix(self) -> Dict[str, Any]:
        matrix: Dict[str, Any] = {
            "network": {},
            "auth": {},
            "path": {},
            "filesystem_ops": {},
            "file_names": {},
            "file_sizes": {},
            "throughput": {},
            "parallelism": {},
        }
        for r in self.results.results:
            key = r.name
            val = {
                "ok": r.ok,
                "details": r.details,
                "error": r.error,
            }
            if r.category == "network":
                matrix["network"][key] = val
            elif r.category == "auth":
                matrix["auth"][key] = val
            elif r.category == "path":
                matrix["path"][key] = val
            elif r.category == "filesystem":
                matrix["filesystem_ops"][key] = val
            elif r.category == "names":
                matrix["file_names"][key] = val
            elif r.category == "sizes":
                matrix["file_sizes"][key] = val
            elif r.category == "throughput":
                matrix["throughput"][key] = val
            elif r.category == "parallel":
                matrix["parallelism"][key] = val
        return matrix

    # ========================================================
    # TESTS
    # ========================================================

    def test_dns_resolution(self):
        def run():
            infos = socket.getaddrinfo(self.args.host, self.args.port, type=socket.SOCK_STREAM)
            addrs = sorted({item[4][0] for item in infos})
            return {"resolved_addresses": addrs}
        return self.record("dns_resolution", "network", run)

    def test_tcp_connectivity(self):
        def run():
            ok, rc, err = tcp_connect_ex(self.args.host, self.args.port, self.args.tcp_timeout)
            if not ok:
                raise ConnectionError(f"TCP KO rc={rc} err={err}")
            return {"tcp_connect": "ok", "rc": rc}
        return self.record("tcp_connectivity", "network", run)

    def test_ssh_banner(self):
        def run():
            sock = socket.create_connection((self.args.host, self.args.port), timeout=self.args.tcp_timeout)
            sock.settimeout(self.args.banner_timeout)
            try:
                banner = sock.recv(1024)
            finally:
                sock.close()
            text = banner.decode("utf-8", errors="replace").strip()
            if not text.startswith("SSH-"):
                raise RuntimeError(f"Bannière SSH invalide: {text!r}")
            return {"banner": text}
        return self.record("ssh_banner", "network", run)

    def test_ssh_auth_and_transport(self):
        def run():
            self.client.connect()
            tr = self.client.ssh.get_transport()
            if not tr or not tr.is_active():
                raise RuntimeError("Transport SSH inactif après auth")
            info = {
                "active": tr.is_active(),
                "remote_version": getattr(tr, "remote_version", None),
                "local_cipher": None,
                "remote_cipher": None,
                "local_mac": None,
                "remote_mac": None,
                "server_key_type": None,
            }
            try:
                info["local_cipher"] = tr.local_cipher
                info["remote_cipher"] = tr.remote_cipher
                info["local_mac"] = tr.local_mac
                info["remote_mac"] = tr.remote_mac
            except Exception:
                pass
            try:
                key = tr.get_remote_server_key()
                if key:
                    info["server_key_type"] = key.get_name()
                    info["server_key_bits"] = key.get_bits()
                    info["server_key_fingerprint_sha256"] = hashlib.sha256(key.asbytes()).hexdigest()
            except Exception:
                pass
            return info
        return self.record("ssh_auth_transport", "auth", run)

    def test_sftp_open(self):
        def run():
            self.client.ensure()
            if self.client.sftp is None:
                raise RuntimeError("SFTP non ouvert")
            return {"sftp_open": True}
        return self.record("sftp_open", "auth", run)

    def test_base_dir_access(self):
        def run():
            self.client.ensure()
            st = self.client.stat(self.args.base_dir)
            return {
                "base_dir": self.args.base_dir,
                "mode": mode_to_str(st.st_mode),
                "size": getattr(st, "st_size", None),
                "uid": getattr(st, "st_uid", None),
                "gid": getattr(st, "st_gid", None),
                "mtime": getattr(st, "st_mtime", None),
            }
        return self.record("base_dir_access", "path", run)

    def test_create_test_root(self):
        def run():
            self.client.mkdir_p(self.test_root)
            self.mark_remote(self.test_root)
            st = self.client.stat(self.test_root)
            return {
                "created": self.test_root,
                "mode": mode_to_str(st.st_mode),
            }
        return self.record("create_test_root", "path", run)

    def test_listdir_test_root(self):
        def run():
            entries = self.client.listdir(self.test_root)
            return {"entries_count": len(entries), "entries": entries[:100]}
        return self.record("listdir_test_root", "path", run)

    def test_mkdir_nested(self):
        def run():
            nested = posixpath.join(self.test_root, "a", "b", "c", "d")
            self.client.mkdir_p(nested)
            for p in [
                posixpath.join(self.test_root, "a"),
                posixpath.join(self.test_root, "a", "b"),
                posixpath.join(self.test_root, "a", "b", "c"),
                nested,
            ]:
                self.mark_remote(p)
            st = self.client.stat(nested)
            return {"nested_dir": nested, "mode": mode_to_str(st.st_mode)}
        return self.record("mkdir_nested", "filesystem", run)

    def test_small_upload_download(self):
        def run():
            payload = b"NAS-DIAG-SMALL-FILE\n" * 16
            local = ensure_local_temp_file(0, 1, suffix=".bin")
            self.mark_local(local)
            with open(local, "wb") as f:
                f.write(payload)

            remote = posixpath.join(self.test_root, "small_upload_download.bin")
            dl = local + ".download"
            self.mark_local(dl)
            self.client.put(local, remote)
            self.mark_remote(remote)
            self.client.get(remote, dl)

            h1 = sha256_file(local)
            h2 = sha256_file(dl)
            if h1 != h2:
                raise RuntimeError("Hash mismatch upload/download")
            st = self.client.stat(remote)
            return {
                "remote": remote,
                "size": st.st_size,
                "sha256": h1,
            }
        return self.record("small_upload_download", "filesystem", run)

    def test_stream_write_read(self):
        def run():
            remote = posixpath.join(self.test_root, "stream_write_read.txt")
            self.mark_remote(remote)
            content = ("stream test " + now_iso() + "\n").encode("utf-8") * 32

            with self.client.open_write(remote) as f:
                f.write(content)
                f.flush()

            with self.client.open_read(remote) as f:
                got = f.read()

            if got != content:
                raise RuntimeError("Contenu lu différent du contenu écrit")
            return {"remote": remote, "size": len(content), "sha256": sha256_bytes(content)}
        return self.record("stream_write_read", "filesystem", run)

    def test_rename(self):
        def run():
            src = posixpath.join(self.test_root, "rename_src.txt")
            dst = posixpath.join(self.test_root, "rename_dst.txt")
            self.mark_remote(src)
            self.mark_remote(dst)

            with self.client.open_write(src) as f:
                f.write(b"rename-test")

            self.client.rename(src, dst)
            if self.client.exists(src):
                raise RuntimeError("La source existe encore après rename")
            if not self.client.exists(dst):
                raise RuntimeError("La destination n'existe pas après rename")
            return {"src": src, "dst": dst}
        return self.record("rename", "filesystem", run)

    def test_overwrite_behavior(self):
        def run():
            a = posixpath.join(self.test_root, "overwrite_target.txt")
            b = posixpath.join(self.test_root, "overwrite_source.txt")
            self.mark_remote(a)
            self.mark_remote(b)

            with self.client.open_write(a) as f:
                f.write(b"A" * 32)
            with self.client.open_write(b) as f:
                f.write(b"B" * 64)

            overwrite_ok = None
            overwrite_mode = "unknown"
            try:
                self.client.rename(b, a)
                overwrite_ok = True
                overwrite_mode = "rename_over_existing_allowed"
            except Exception as e:
                overwrite_ok = False
                overwrite_mode = f"rename_over_existing_refused: {e}"

            details = {
                "target": a,
                "source": b,
                "overwrite_ok": overwrite_ok,
                "behavior": overwrite_mode,
            }

            if self.client.exists(a):
                st = self.client.stat(a)
                details["target_size_after"] = st.st_size
            details["source_exists_after"] = self.client.exists(b)
            return details
        return self.record("overwrite_behavior", "filesystem", run)

    def test_remove_file(self):
        def run():
            remote = posixpath.join(self.test_root, "remove_me.txt")
            with self.client.open_write(remote) as f:
                f.write(b"delete-me")
            self.mark_remote(remote)
            self.client.remove(remote)
            if self.client.exists(remote):
                raise RuntimeError("Le fichier existe encore après remove")
            return {"removed": remote}
        return self.record("remove_file", "filesystem", run)

    def test_rmdir_empty(self):
        def run():
            d = posixpath.join(self.test_root, "empty_dir_to_remove")
            self.client.mkdir(d)
            self.mark_remote(d)
            self.client.rmdir(d)
            if self.client.exists(d):
                raise RuntimeError("Le dossier existe encore après rmdir")
            return {"removed_dir": d}
        return self.record("rmdir_empty", "filesystem", run)

    def test_stat_listdir_attr(self):
        def run():
            entries = self.client.listdir_attr(self.test_root)
            out = []
            for e in entries[:50]:
                out.append({
                    "filename": e.filename,
                    "size": getattr(e, "st_size", None),
                    "mode": mode_to_str(getattr(e, "st_mode", None)),
                    "mtime": getattr(e, "st_mtime", None),
                    "uid": getattr(e, "st_uid", None),
                    "gid": getattr(e, "st_gid", None),
                })
            return {"entries": out}
        return self.record("stat_listdir_attr", "filesystem", run)

    def test_chmod(self):
        def run():
            remote = posixpath.join(self.test_root, "chmod_target.txt")
            self.mark_remote(remote)
            with self.client.open_write(remote) as f:
                f.write(b"chmod test")
            before = self.client.stat(remote)
            self.client.chmod(remote, 0o640)
            after = self.client.stat(remote)
            return {
                "remote": remote,
                "before_mode": mode_to_str(before.st_mode),
                "after_mode": mode_to_str(after.st_mode),
            }
        return self.record("chmod", "filesystem", run)

    def test_utime(self):
        def run():
            remote = posixpath.join(self.test_root, "utime_target.txt")
            self.mark_remote(remote)
            with self.client.open_write(remote) as f:
                f.write(b"utime test")
            ts = int(time.time()) - 3600
            self.client.utime(remote, (ts, ts))
            st = self.client.stat(remote)
            return {
                "remote": remote,
                "expected_mtime": ts,
                "actual_mtime": getattr(st, "st_mtime", None),
            }
        return self.record("utime", "filesystem", run)

    def test_filename_matrix(self):
        cases = [
            ("plain_ascii", "file.txt"),
            ("space", "file with spaces.txt"),
            ("dash_underscore_dot", "a-b_c.d.txt"),
            ("leading_dot", ".hidden_file"),
            ("multiple_dots", "archive.tar.gz"),
            ("long_120", "x" * 120 + ".txt"),
            ("long_200", "y" * 200 + ".txt"),
            ("unicode_latin", "fichier_éàçù.txt"),
            ("unicode_mixed", "測試_пример_é.txt"),
            ("reserved_like_windows", "aux.txt"),
            ("semicolon", "a;b.txt"),
            ("comma", "a,b.txt"),
            ("plus", "a+b.txt"),
            ("equals", "a=b.txt"),
            ("brackets", "a{3}.txt"),
        ]

        for case_name, filename in cases:
            def run_one(case_name=case_name, filename=filename):
                remote = posixpath.join(self.test_root, filename)
                self.mark_remote(remote)
                payload = f"{case_name}::{filename}::{now_iso()}".encode("utf-8")
                with self.client.open_write(remote) as f:
                    f.write(payload)
                st = self.client.stat(remote)
                return {
                    "remote": remote,
                    "size": st.st_size,
                    "filename_len": len(filename),
                }
            self.record(f"filename_{case_name}", "names", run_one)

    def test_path_depth(self):
        def run():
            parts = ["depth"] + [f"l{i:02d}" for i in range(1, 16)]
            deep_dir = posixpath.join(self.test_root, *parts)
            self.client.mkdir_p(deep_dir)
            cur = self.test_root
            for part in parts:
                cur = posixpath.join(cur, part)
                self.mark_remote(cur)

            remote = posixpath.join(deep_dir, "deep_file.txt")
            self.mark_remote(remote)
            with self.client.open_write(remote) as f:
                f.write(b"deep path test")

            st = self.client.stat(remote)
            return {
                "deep_dir": deep_dir,
                "remote_file": remote,
                "full_path_len": len(remote),
                "size": st.st_size,
            }
        return self.record("path_depth", "path", run)

    def test_file_size(self, size_mb: int):
        def run():
            size_bytes = size_mb * 1024 * 1024
            local = ensure_local_temp_file(size_bytes, seed=size_mb + 1000, suffix=f"_{size_mb}mb.bin")
            self.mark_local(local)
            remote = posixpath.join(self.test_root, f"size_{size_mb}mb.bin")
            self.mark_remote(remote)

            t0 = time.monotonic()
            self.client.put(local, remote)
            elapsed = max(time.monotonic() - t0, 0.001)

            st = self.client.stat(remote)
            local_hash = sha256_file(local)
            dl = local + ".download"
            self.mark_local(dl)
            self.client.get(remote, dl)
            dl_hash = sha256_file(dl)

            if local_hash != dl_hash:
                raise RuntimeError("Hash mismatch après upload/download")

            mbps = round(size_mb / elapsed, 2)
            return {
                "remote": remote,
                "size_bytes": size_bytes,
                "size_mb": size_mb,
                "upload_seconds": round(elapsed, 3),
                "throughput_MBps": mbps,
                "remote_stat_size": st.st_size,
                "sha256": local_hash,
            }
        return self.record(f"file_size_{size_mb}mb", "sizes", run)

    def test_throughput_large(self):
        size_mb = self.args.max_upload_mb
        def run():
            size_bytes = size_mb * 1024 * 1024
            local = ensure_local_temp_file(size_bytes, seed=424242, suffix=".throughput.bin")
            self.mark_local(local)
            remote = posixpath.join(self.test_root, f"throughput_{size_mb}mb.bin")
            self.mark_remote(remote)

            t0 = time.monotonic()
            self.client.put(local, remote)
            upload_elapsed = max(time.monotonic() - t0, 0.001)

            dl = local + ".download"
            self.mark_local(dl)
            t1 = time.monotonic()
            self.client.get(remote, dl)
            download_elapsed = max(time.monotonic() - t1, 0.001)

            return {
                "size_mb": size_mb,
                "upload_seconds": round(upload_elapsed, 3),
                "upload_MBps": round(size_mb / upload_elapsed, 2),
                "download_seconds": round(download_elapsed, 3),
                "download_MBps": round(size_mb / download_elapsed, 2),
            }
        return self.record("throughput_large_file", "throughput", run)

    def test_parallel_connections(self):
        workers = self.args.parallel
        if workers <= 1:
            return

        lock = threading.Lock()
        out: List[Dict[str, Any]] = []

        def worker_fn(idx: int):
            c = NASProbeClient(
                host=self.args.host,
                port=self.args.port,
                user=self.args.user,
                password=self.args.password,
                ssh_timeout=self.args.ssh_timeout,
                banner_timeout=self.args.banner_timeout,
                auth_timeout=self.args.auth_timeout,
                keepalive_sec=self.args.keepalive_sec,
            )
            local = None
            try:
                c.connect()
                remote_dir = posixpath.join(self.test_root, "parallel")
                c.mkdir_p(remote_dir)
                with lock:
                    if remote_dir not in self.created_remote_paths:
                        self.mark_remote(remote_dir)

                local = ensure_local_temp_file(self.args.parallel_file_mb * 1024 * 1024, seed=9000 + idx)
                with lock:
                    self.mark_local(local)

                remote = posixpath.join(remote_dir, f"parallel_{idx}.bin")
                t0 = time.monotonic()
                c.put(local, remote)
                elapsed = max(time.monotonic() - t0, 0.001)
                st = c.stat(remote)

                with lock:
                    self.mark_remote(remote)
                    out.append({
                        "worker": idx,
                        "ok": True,
                        "remote": remote,
                        "size_bytes": st.st_size,
                        "seconds": round(elapsed, 3),
                        "MBps": round(self.args.parallel_file_mb / elapsed, 2),
                    })
            except Exception as e:
                with lock:
                    out.append({
                        "worker": idx,
                        "ok": False,
                        "error": safe_exc(e),
                    })
            finally:
                c.close()

        def run():
            threads = [threading.Thread(target=worker_fn, args=(i,), name=f"p{i}") for i in range(1, workers + 1)]
            t0 = time.monotonic()
            for th in threads:
                th.start()
            for th in threads:
                th.join()
            elapsed = max(time.monotonic() - t0, 0.001)
            failed = [x for x in out if not x["ok"]]
            return {
                "workers": workers,
                "parallel_file_mb": self.args.parallel_file_mb,
                "total_seconds": round(elapsed, 3),
                "results": sorted(out, key=lambda x: x["worker"]),
                "all_ok": len(failed) == 0,
                "failed_workers": [x["worker"] for x in failed],
            }
        return self.record("parallel_connections_uploads", "parallel", run)

    # ========================================================
    # EXÉCUTION
    # ========================================================

    def run_all(self):
        try:
            self.test_dns_resolution()
            self.test_tcp_connectivity()
            self.test_ssh_banner()
            self.test_ssh_auth_and_transport()
            self.test_sftp_open()
            self.test_base_dir_access()
            self.test_create_test_root()
            self.test_listdir_test_root()
            self.test_mkdir_nested()
            self.test_small_upload_download()
            self.test_stream_write_read()
            self.test_rename()
            self.test_overwrite_behavior()
            self.test_remove_file()
            self.test_rmdir_empty()
            self.test_stat_listdir_attr()
            self.test_chmod()
            self.test_utime()
            self.test_filename_matrix()
            self.test_path_depth()

            sizes = [0, 1, 8]
            if self.args.max_upload_mb >= 32:
                sizes.append(32)
            if self.args.max_upload_mb >= 64:
                sizes.append(64)
            if self.args.max_upload_mb >= 128:
                sizes.append(128)

            sizes = [s for s in sizes if s <= self.args.max_upload_mb]
            sizes = sorted(set(sizes))

            for size_mb in sizes:
                self.test_file_size(size_mb)

            self.test_throughput_large()
            self.test_parallel_connections()

        finally:
            self.write_report()
            self.cleanup()
            self.client.close()


# ============================================================
# CLI
# ============================================================

def parse_args():
    p = argparse.ArgumentParser(description="Diagnostic NAS/SFTP ultra-complet")
    p.add_argument("--host", required=True, help="Hôte ou IP du NAS")
    p.add_argument("--port", type=int, default=22, help="Port SSH/SFTP")
    p.add_argument("--user", required=True, help="Utilisateur SSH/SFTP")
    p.add_argument("--password", required=True, help="Mot de passe SSH/SFTP")

    p.add_argument("--base-dir", required=True, help="Répertoire distant racine autorisé")
    p.add_argument("--test-root", default="bronze/_diag", help="Sous-répertoire de test sous base-dir")

    p.add_argument("--report-file", default="nas_diag_report.json", help="Fichier JSON de rapport")
    p.add_argument("--keep-artifacts", action="store_true", help="Conserver les fichiers/dossiers de test sur le NAS et en local")

    p.add_argument("--tcp-timeout", type=float, default=5.0)
    p.add_argument("--ssh-timeout", type=float, default=20.0)
    p.add_argument("--banner-timeout", type=float, default=60.0)
    p.add_argument("--auth-timeout", type=float, default=30.0)
    p.add_argument("--keepalive-sec", type=int, default=30)

    p.add_argument("--max-upload-mb", type=int, default=64, help="Taille max du test de gros fichier")
    p.add_argument("--parallel", type=int, default=1, help="Nombre de connexions parallèles à tester")
    p.add_argument("--parallel-file-mb", type=int, default=8, help="Taille de chaque fichier du test parallèle")

    return p.parse_args()


def print_console_summary(report: Dict[str, Any]):
    s = report["summary"]
    print()
    print("===== RÉSUMÉ DIAGNOSTIC NAS =====")
    print(f"Total tests : {s['total']}")
    print(f"Succès      : {s['passed']}")
    print(f"Échecs      : {s['failed']}")
    print()

    print("Par catégorie :")
    for cat, vals in sorted(s["by_category"].items()):
        print(f"  - {cat:12s} total={vals['total']:2d} ok={vals['passed']:2d} fail={vals['failed']:2d}")

    print()
    print("Tests en échec :")
    failed = [r for r in report["results"] if not r["ok"]]
    if not failed:
        print("  Aucun")
    else:
        for r in failed:
            msg = r["error"]["message"] if r.get("error") else "erreur inconnue"
            print(f"  - [{r['category']}] {r['name']} :: {msg}")

    print()
    print(f"Rapport JSON : {report['meta']['params']['report_file']}")
    print(f"Répertoire de test : {report['meta']['target']['test_root']}")
    print("=================================")


def main():
    args = parse_args()

    if args.max_upload_mb < 1:
        print("ERREUR: --max-upload-mb doit être >= 1", file=sys.stderr)
        sys.exit(2)

    if args.parallel < 1:
        print("ERREUR: --parallel doit être >= 1", file=sys.stderr)
        sys.exit(2)

    if args.parallel_file_mb < 1:
        print("ERREUR: --parallel-file-mb doit être >= 1", file=sys.stderr)
        sys.exit(2)

    harness = ProbeHarness(args)
    harness.run_all()
    with open(args.report_file, "r", encoding="utf-8") as f:
        report = json.load(f)

    print_console_summary(report)

    if report["summary"]["failed"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
