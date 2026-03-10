#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
retry.py — Remet en queue toutes les sessions échouées/bloquées du spool.

Actions possibles :
  --requeue-failed      : remet les jobs 'failed' en 'queued'
  --requeue-processing  : remet les jobs bloqués en 'processing' en 'queued'
  --requeue-all         : les deux
  --move-quarantine     : déplace les sessions de quarantine/ vers spool/ et les remet en queue
  --move-inbox          : déplace les sessions de inbox/*__FAILED vers spool/ et les remet en queue
  --status              : affiche juste l'état sans rien modifier

Usage :
  python3 retry.py --status
  python3 retry.py --requeue-all
  python3 retry.py --move-quarantine --requeue-all
  python3 retry.py --move-quarantine --move-inbox --requeue-all
"""

import os
import re
import sys
import shutil
import sqlite3
import argparse
import datetime as dt

# ── Config ───────────────────────────────────────────────────────────────────
DB_PATH        = "/srv/exoria/queue.db"
SPOOL_DIR      = "/srv/exoria/spool"
INBOX_DIR      = "/srv/exoria/inbox"
QUARANTINE_DIR = "/srv/exoria/quarantine"

SESSION_RE = re.compile(r"^session_\d{8}_\d{6}")
# ─────────────────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
GRAY   = "\033[90m"


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def print_status(conn):
    rows = conn.execute("SELECT status, COUNT(*) as n FROM jobs GROUP BY status").fetchall()
    total = sum(r["n"] for r in rows)
    print(f"\n{BOLD}État de la queue ({DB_PATH}){RESET}")
    print(f"  Total : {total}")
    for r in rows:
        s, n = r["status"], r["n"]
        if s == "done":       color = GREEN
        elif s == "failed":   color = RED
        elif s == "processing": color = YELLOW
        else:                 color = CYAN
        print(f"  {color}{s:15s}{RESET} : {n}")

    # Dossiers
    def count_dir(p):
        try:
            return len([x for x in os.listdir(p) if os.path.isdir(os.path.join(p, x))])
        except Exception:
            return "?"

    print(f"\n{BOLD}Dossiers{RESET}")
    print(f"  spool/      : {count_dir(SPOOL_DIR)}")
    print(f"  quarantine/ : {count_dir(QUARANTINE_DIR)}")
    print(f"  inbox/      : {count_dir(INBOX_DIR)}")

    # Échecs avec erreurs
    failed = conn.execute(
        "SELECT session_id, retries, substr(error,1,120) as err FROM jobs "
        "WHERE status='failed' ORDER BY updated_at DESC LIMIT 10"
    ).fetchall()
    if failed:
        print(f"\n{BOLD}{RED}Derniers échecs{RESET}")
        for r in failed:
            print(f"  {RED}✗{RESET} {r['session_id']}  retries={r['retries']}")
            if r["err"]:
                print(f"    {GRAY}{r['err']}{RESET}")


def requeue_failed(conn, dry_run=False):
    rows = conn.execute(
        "SELECT session_id FROM jobs WHERE status='failed'"
    ).fetchall()
    if not rows:
        print(f"{GRAY}Aucun job 'failed' à remettre en queue.{RESET}")
        return 0

    print(f"\n{YELLOW}Requeue {len(rows)} jobs 'failed'...{RESET}")
    count = 0
    for r in rows:
        sid = r["session_id"]
        # Vérifie que le dossier existe dans spool/ ou quarantine/
        spool_path = os.path.join(SPOOL_DIR, sid)
        quar_path  = os.path.join(QUARANTINE_DIR, sid)
        if not os.path.isdir(spool_path) and not os.path.isdir(quar_path):
            print(f"  {RED}✗ {sid} — dossier introuvable (spool/ et quarantine/), ignoré{RESET}")
            continue
        if not dry_run:
            conn.execute(
                "UPDATE jobs SET status='queued', retries=0, error=NULL, "
                "updated_at=? WHERE session_id=? AND status='failed'",
                (dt.datetime.utcnow().isoformat(), sid)
            )
        print(f"  {GREEN}✓ {sid}{RESET}")
        count += 1

    if not dry_run:
        conn.commit()
    print(f"{GREEN}{count} jobs remis en 'queued'.{RESET}")
    return count


def requeue_processing(conn, dry_run=False):
    rows = conn.execute(
        "SELECT session_id FROM jobs WHERE status='processing'"
    ).fetchall()
    if not rows:
        print(f"{GRAY}Aucun job 'processing' bloqué.{RESET}")
        return 0

    print(f"\n{YELLOW}Reset {len(rows)} jobs 'processing' bloqués...{RESET}")
    count = 0
    for r in rows:
        sid = r["session_id"]
        if not dry_run:
            conn.execute(
                "UPDATE jobs SET status='queued', retries=0, error=NULL, "
                "updated_at=? WHERE session_id=? AND status='processing'",
                (dt.datetime.utcnow().isoformat(), sid)
            )
        print(f"  {CYAN}↺ {sid}{RESET}")
        count += 1

    if not dry_run:
        conn.commit()
    print(f"{GREEN}{count} jobs remis en 'queued'.{RESET}")
    return count


def move_quarantine_to_spool(conn, dry_run=False):
    """Déplace les sessions de quarantine/ vers spool/ et les réinsère en DB."""
    try:
        entries = [
            e for e in os.listdir(QUARANTINE_DIR)
            if SESSION_RE.match(e) and os.path.isdir(os.path.join(QUARANTINE_DIR, e))
        ]
    except FileNotFoundError:
        print(f"{GRAY}quarantine/ vide ou inexistant.{RESET}")
        return 0

    if not entries:
        print(f"{GRAY}Aucune session dans quarantine/.{RESET}")
        return 0

    print(f"\n{YELLOW}Déplacement de {len(entries)} sessions quarantine/ → spool/...{RESET}")
    count = 0
    for name in entries:
        src = os.path.join(QUARANTINE_DIR, name)
        dst = os.path.join(SPOOL_DIR, name)

        if os.path.exists(dst):
            print(f"  {YELLOW}⚠ {name} déjà dans spool/, ignoré{RESET}")
            continue

        if not dry_run:
            shutil.move(src, dst)
            # Upsert en DB
            existing = conn.execute(
                "SELECT status FROM jobs WHERE session_id=?", (name,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE jobs SET status='queued', retries=0, error=NULL, "
                    "updated_at=? WHERE session_id=?",
                    (dt.datetime.utcnow().isoformat(), name)
                )
            else:
                conn.execute(
                    "INSERT INTO jobs (session_id, status, retries, created_at, updated_at) "
                    "VALUES (?, 'queued', 0, ?, ?)",
                    (name, dt.datetime.utcnow().isoformat(), dt.datetime.utcnow().isoformat())
                )

        print(f"  {GREEN}✓ {name}{RESET}")
        count += 1

    if not dry_run:
        conn.commit()
    print(f"{GREEN}{count} sessions déplacées vers spool/.{RESET}")
    return count


def move_inbox_failed_to_spool(conn, dry_run=False):
    """Déplace les sessions inbox/*__FAILED vers spool/ et les réinsère en DB."""
    try:
        entries = [
            e for e in os.listdir(INBOX_DIR)
            if SESSION_RE.match(e) and os.path.isdir(os.path.join(INBOX_DIR, e))
        ]
    except FileNotFoundError:
        print(f"{GRAY}inbox/ vide ou inexistant.{RESET}")
        return 0

    if not entries:
        print(f"{GRAY}Aucune session FAILED dans inbox/.{RESET}")
        return 0

    print(f"\n{YELLOW}Déplacement de {len(entries)} sessions inbox/ → spool/...{RESET}")
    count = 0
    for name in entries:
        src = os.path.join(INBOX_DIR, name)
        # Nom propre sans __FAILED
        clean_name = name.replace("__FAILED", "")
        dst = os.path.join(SPOOL_DIR, clean_name)

        if os.path.exists(dst):
            print(f"  {YELLOW}⚠ {clean_name} déjà dans spool/, ignoré{RESET}")
            continue

        if not dry_run:
            shutil.move(src, dst)
            existing = conn.execute(
                "SELECT status FROM jobs WHERE session_id=?", (clean_name,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE jobs SET status='queued', retries=0, error=NULL, "
                    "updated_at=? WHERE session_id=?",
                    (dt.datetime.utcnow().isoformat(), clean_name)
                )
            else:
                conn.execute(
                    "INSERT INTO jobs (session_id, status, retries, created_at, updated_at) "
                    "VALUES (?, 'queued', 0, ?, ?)",
                    (clean_name, dt.datetime.utcnow().isoformat(), dt.datetime.utcnow().isoformat())
                )

        print(f"  {GREEN}✓ {clean_name}{RESET}")
        count += 1

    if not dry_run:
        conn.commit()
    print(f"{GREEN}{count} sessions déplacées vers spool/.{RESET}")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Remet en queue les sessions échouées du spool"
    )
    parser.add_argument("--status",             action="store_true", help="Affiche l'état sans modifier")
    parser.add_argument("--requeue-failed",     action="store_true", help="Remet les jobs 'failed' en 'queued'")
    parser.add_argument("--requeue-processing", action="store_true", help="Remet les jobs 'processing' bloqués en 'queued'")
    parser.add_argument("--requeue-all",        action="store_true", help="--requeue-failed + --requeue-processing")
    parser.add_argument("--move-quarantine",    action="store_true", help="Déplace quarantine/ → spool/")
    parser.add_argument("--move-inbox",         action="store_true", help="Déplace inbox/*__FAILED → spool/")
    parser.add_argument("--dry-run",            action="store_true", help="Simule sans modifier")
    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    if args.dry_run:
        print(f"{YELLOW}[DRY-RUN] Simulation — aucune modification{RESET}")

    conn = db_connect()

    print_status(conn)

    if args.status:
        conn.close()
        return

    total = 0

    if args.move_quarantine:
        total += move_quarantine_to_spool(conn, dry_run=args.dry_run)

    if args.move_inbox:
        total += move_inbox_failed_to_spool(conn, dry_run=args.dry_run)

    if args.requeue_all or args.requeue_failed:
        total += requeue_failed(conn, dry_run=args.dry_run)

    if args.requeue_all or args.requeue_processing:
        total += requeue_processing(conn, dry_run=args.dry_run)

    conn.close()

    print(f"\n{BOLD}Total opérations : {GREEN}{total}{RESET}")
    if total > 0 and not args.dry_run:
        print(f"{CYAN}→ Redémarre le spool pour traiter les jobs remis en queue.{RESET}")


if __name__ == "__main__":
    main()
