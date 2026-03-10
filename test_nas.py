#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_nas.py — Diagnostic complet de la connexion NAS + écriture SFTP
Lance ce script directement sur le serveur spool.

Usage : python3 test_nas.py
"""

import os
import sys
import time
import socket
import tempfile
import traceback

try:
    import paramiko
except ImportError:
    print("ERREUR : paramiko non installé. Fais : pip install paramiko")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
NAS_HOST  = "192.168.88.82"
NAS_PORT  = 22
NAS_USER  = "root"
NAS_PASS  = "Exori@2026!"
NAS_DIR   = "/data/INBOX"
# ─────────────────────────────────────────────────────────────────────────────

OK   = "\033[92m[OK]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[96m[INFO]\033[0m"

def step(label):
    print(f"\n{'─'*55}")
    print(f"  {label}")
    print(f"{'─'*55}")

def ok(msg):   print(f"  {OK}   {msg}")
def fail(msg): print(f"  {FAIL} {msg}")
def info(msg): print(f"  {INFO} {msg}")


# ── 1. TCP ────────────────────────────────────────────────────────────────────
step("1. Ping TCP  192.168.88.82:22")
try:
    s = socket.create_connection((NAS_HOST, NAS_PORT), timeout=5)
    s.close()
    ok(f"Port {NAS_PORT} accessible")
except Exception as e:
    fail(f"Impossible de joindre {NAS_HOST}:{NAS_PORT} → {e}")
    sys.exit(1)


# ── 2. Connexion SSH ──────────────────────────────────────────────────────────
step(f"2. Connexion SSH  user={NAS_USER}")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(
        hostname=NAS_HOST, port=NAS_PORT,
        username=NAS_USER, password=NAS_PASS,
        look_for_keys=False, allow_agent=False,
        timeout=15, banner_timeout=30, auth_timeout=20,
    )
    ok(f"Authentification réussie en tant que {NAS_USER}")
except paramiko.AuthenticationException as e:
    fail(f"Échec d'authentification : {e}")
    info("→ Vérifie NAS_USER / NAS_PASS")
    sys.exit(1)
except Exception as e:
    fail(f"Connexion SSH échouée : {e}")
    traceback.print_exc()
    sys.exit(1)


# ── 3. SFTP ───────────────────────────────────────────────────────────────────
step("3. Ouverture session SFTP")
try:
    sftp = ssh.open_sftp()
    ok("Session SFTP ouverte")
except Exception as e:
    fail(f"Impossible d'ouvrir SFTP : {e}")
    sys.exit(1)


# ── 4. Stat /data/INBOX ───────────────────────────────────────────────────────
step(f"4. Vérification de {NAS_DIR}")
try:
    attr = sftp.stat(NAS_DIR)
    ok(f"{NAS_DIR} existe  (mode={oct(attr.st_mode)}  uid={attr.st_uid}  gid={attr.st_gid})")
except Exception as e:
    fail(f"{NAS_DIR} inaccessible : {e}")
    info("→ Le répertoire n'existe peut-être pas sur le NAS")


# ── 5. Listdir /data/INBOX ────────────────────────────────────────────────────
step(f"5. Listdir {NAS_DIR}")
try:
    entries = sftp.listdir(NAS_DIR)
    ok(f"{len(entries)} entrées dans {NAS_DIR}")
    for e in entries[:5]:
        info(f"  {e}")
    if len(entries) > 5:
        info(f"  ... ({len(entries)-5} de plus)")
except Exception as e:
    fail(f"Impossible de lister {NAS_DIR} : {e}")


# ── 6. Mkdir test ─────────────────────────────────────────────────────────────
TEST_DIR = f"{NAS_DIR}/_test_spool_{int(time.time())}"
step(f"6. Création dossier test  {TEST_DIR}")
try:
    sftp.mkdir(TEST_DIR)
    ok(f"mkdir OK → {TEST_DIR}")
except Exception as e:
    fail(f"mkdir échoué : {e}")
    info("→ C'est ici que ça plante normalement — problème de permissions")
    info(f"→ L'utilisateur '{NAS_USER}' n'a pas le droit d'écrire dans {NAS_DIR}")
    info("→ Solutions :")
    info("    ssh root@192.168.88.82 'chmod 775 /data/INBOX && chown root:domain\\ users /data/INBOX'")
    info("    ou ajouter admin1 au groupe qui a RWX sur /data/INBOX")
    sftp.close()
    ssh.close()
    sys.exit(1)


# ── 7. Upload fichier test ────────────────────────────────────────────────────
step("7. Upload fichier test")
TEST_FILE_REMOTE = f"{TEST_DIR}/test_file.txt"
try:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(b"spool test " * 1000)
        local_path = f.name

    t0 = time.monotonic()
    sftp.put(local_path, TEST_FILE_REMOTE)
    elapsed = time.monotonic() - t0
    size = os.path.getsize(local_path)
    speed = round(size / elapsed / 1e6, 2)
    os.unlink(local_path)

    ok(f"Upload OK  {size} octets en {elapsed:.2f}s  ({speed} MB/s)")
except Exception as e:
    fail(f"Upload échoué : {e}")
    traceback.print_exc()


# ── 8. Rename (atomic write) ──────────────────────────────────────────────────
step("8. Rename (écriture atomique .part → final)")
FINAL = f"{TEST_DIR}/test_file_final.txt"
try:
    sftp.rename(TEST_FILE_REMOTE, FINAL)
    ok(f"Rename OK → {FINAL}")
except Exception as e:
    fail(f"Rename échoué : {e}")


# ── 9. Nettoyage ──────────────────────────────────────────────────────────────
step("9. Nettoyage dossier test")
try:
    for f in sftp.listdir(TEST_DIR):
        sftp.remove(f"{TEST_DIR}/{f}")
    sftp.rmdir(TEST_DIR)
    ok(f"Nettoyage OK — {TEST_DIR} supprimé")
except Exception as e:
    fail(f"Nettoyage échoué (pas bloquant) : {e}")


# ── 10. Espace disque ─────────────────────────────────────────────────────────
step("10. Espace disque NAS")
try:
    _, stdout, _ = ssh.exec_command("df -h /data")
    out = stdout.read().decode().strip()
    for line in out.splitlines():
        info(line)
except Exception as e:
    fail(f"df échoué : {e}")


sftp.close()
ssh.close()

print(f"\n{'═'*55}")
print(f"  RÉSULTAT : TOUT OK — NAS opérationnel pour {NAS_USER}")
print(f"{'═'*55}\n")
