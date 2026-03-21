#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard temps réel du spool — queue locale + données sur le NAS
Usage : python3 dashboard.py [--interval 3]
"""

import os
import time
import sqlite3
import argparse
import shutil
import paramiko

# ── Config (même que spool.py) ──────────────────────────────────────────────
DB_PATH       = "/srv/exoria/queue.db"
SPOOL_DIR     = "/srv/exoria/spool"
INBOX_DIR     = "/srv/exoria/inbox"
QUARANTINE_DIR= "/srv/exoria/quarantine"

NAS_HOST      = "192.168.88.82"
NAS_PORT      = 22
NAS_USER      = "sftpuser"
NAS_PASS      = "Exori@2026!"
NAS_INBOX     = "/inbox"
# ─────────────────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
GRAY   = "\033[90m"


def clear():
    os.system("clear")


def disk_info(path):
    try:
        st = shutil.disk_usage(path)
        used_pct = st.used / st.total * 100
        color = RED if used_pct > 90 else YELLOW if used_pct > 75 else GREEN
        return (f"{color}{used_pct:.1f}%{RESET} "
                f"({st.used/1e9:.1f}/{st.total/1e9:.1f} Go)")
    except Exception:
        return GRAY + "N/A" + RESET


def db_stats():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM jobs GROUP BY status"
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        return {"error": str(e)}


def db_recent(limit=10):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        rows = conn.execute(
            """SELECT session_id, status, retries, error, updated_at
               FROM jobs
               ORDER BY updated_at DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def db_failed(limit=8):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        rows = conn.execute(
            """SELECT session_id, retries, error, updated_at
               FROM jobs WHERE status='failed'
               ORDER BY updated_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def nas_stats():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(NAS_HOST, port=NAS_PORT, username=NAS_USER,
                    password=NAS_PASS, timeout=5, banner_timeout=10)
        sftp = ssh.open_sftp()

        # Compter les sessions sur le NAS
        try:
            entries = sftp.listdir(NAS_INBOX)
            count = len(entries)
        except Exception:
            entries = []
            count = -1

        # Espace disque via statvfs (pas de shell avec chroot SFTP)
        df_line = ""
        try:
            vfs = sftp.statvfs(NAS_INBOX)
            total_gb = vfs.f_blocks * vfs.f_bsize / (1024 ** 3)
            free_gb  = vfs.f_bavail * vfs.f_bsize / (1024 ** 3)
            used_pct = int((1 - vfs.f_bavail / vfs.f_blocks) * 100) if vfs.f_blocks else 0
            df_line  = f"{total_gb:.1f}G total  {free_gb:.1f}G libre  {used_pct}% utilisé"
        except Exception:
            pass

        sftp.close()
        ssh.close()
        return {"count": count, "df": df_line, "entries": entries[-5:]}
    except Exception as e:
        return {"error": str(e)}


def color_status(s):
    if s == "done":      return GREEN + s + RESET
    if s == "failed":    return RED + s + RESET
    if s == "processing":return YELLOW + s + RESET
    if s == "queued":    return CYAN + s + RESET
    return s


def render(nas):
    clear()
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  SPOOL DASHBOARD  {RESET}{GRAY}{now}{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")

    # ── Disque local ────────────────────────────────────────────────
    print(f"\n{BOLD}DISQUE LOCAL{RESET}")
    print(f"  /srv/exoria  : {disk_info('/srv/exoria')}")

    # ── Queue SQLite ─────────────────────────────────────────────────
    stats = db_stats()
    if "error" in stats:
        print(f"\n{RED}DB inaccessible : {stats['error']}{RESET}")
    else:
        total = sum(stats.values())
        queued     = stats.get("queued", 0)
        processing = stats.get("processing", 0)
        done       = stats.get("done", 0)
        failed     = stats.get("failed", 0)

        print(f"\n{BOLD}QUEUE  (total {total}){RESET}")
        print(f"  {CYAN}En attente  {RESET}: {queued}")
        print(f"  {YELLOW}En cours    {RESET}: {processing}")
        print(f"  {GREEN}Terminées   {RESET}: {done}")
        print(f"  {RED}Échouées    {RESET}: {failed}")

    # ── Dossiers locaux ──────────────────────────────────────────────
    def count_dir(p):
        try: return len(os.listdir(p))
        except: return "?"

    print(f"\n{BOLD}DOSSIERS LOCAUX{RESET}")
    print(f"  inbox/      : {count_dir(INBOX_DIR)}")
    print(f"  spool/      : {count_dir(SPOOL_DIR)}")
    print(f"  quarantine/ : {count_dir(QUARANTINE_DIR)}")

    # ── NAS ──────────────────────────────────────────────────────────
    print(f"\n{BOLD}NAS  {GRAY}({NAS_HOST}){RESET}")
    if "error" in nas:
        print(f"  {RED}Erreur : {nas['error']}{RESET}")
    else:
        print(f"  Sessions reçues : {GREEN}{nas['count']}{RESET}")
        if nas.get("df"):
            parts = nas["df"].split()
            if len(parts) >= 5:
                print(f"  Disque NAS      : {parts[2]} utilisés / {parts[1]} total ({parts[4]} plein)")
        if nas.get("entries"):
            print(f"  Dernières sessions :")
            for e in reversed(nas["entries"]):
                print(f"    {GRAY}• {e}{RESET}")

    # ── Activité récente ─────────────────────────────────────────────
    print(f"\n{BOLD}ACTIVITÉ RÉCENTE{RESET}")
    recent = db_recent(8)
    if recent:
        for sid, status, retries, error, updated in recent:
            err_str = f" {GRAY}({error[:40]}...){RESET}" if error else ""
            retry_str = f" {YELLOW}[retry {retries}]{RESET}" if retries else ""
            print(f"  {color_status(status):30s} {GRAY}{sid}{RESET}{retry_str}{err_str}")
    else:
        print(f"  {GRAY}Aucune activité{RESET}")

    # ── Sessions échouées ────────────────────────────────────────────
    failed_rows = db_failed(5)
    if failed_rows:
        print(f"\n{BOLD}{RED}ÉCHECS{RESET}")
        for sid, retries, error, updated in failed_rows:
            err = (error or "")[:60]
            print(f"  {RED}✗{RESET} {sid}  {GRAY}retries={retries}  {err}{RESET}")

    print(f"\n{GRAY}Rafraîchissement toutes les {args.interval}s — Ctrl+C pour quitter{RESET}")


def main():
    print("Connexion au NAS pour le premier snapshot...")
    nas = nas_stats()

    last_nas_refresh = time.time()
    NAS_REFRESH_INTERVAL = 30  # le NAS est interrogé toutes les 30s seulement

    while True:
        if time.time() - last_nas_refresh >= NAS_REFRESH_INTERVAL:
            nas = nas_stats()
            last_nas_refresh = time.time()

        render(nas)
        time.sleep(args.interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=3,
                        help="Intervalle de rafraîchissement en secondes (défaut: 3)")
    args = parser.parse_args()
    try:
        main()
    except KeyboardInterrupt:
        print("\nAu revoir.")
