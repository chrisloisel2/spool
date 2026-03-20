#!/usr/bin/env python3
"""
fix_nested_sessions.py — Répare les sessions imbriquées dans spool/.

Problème : shutil.move(inbox/session_X, spool/session_X) quand spool/session_X
existait déjà déplaçait la source *dans* la destination, créant
spool/session_X/session_X au lieu de remplacer spool/session_X.

Ce script :
  1. Détecte tous les cas spool/session_X/session_X (imbrication)
  2. Remonte session_X au bon niveau (spool/session_X) en écrasant le résidu
  3. Met à jour la DB si nécessaire (insère le job manquant en status 'queued')
  4. Mode --dry-run par défaut — aucune modification sans --apply

Usage :
  python3 fix_nested_sessions.py           # dry-run, affiche ce qui serait fait
  python3 fix_nested_sessions.py --apply   # applique les corrections
"""

import os
import re
import sys
import shutil
import sqlite3
import uuid
import argparse
from datetime import datetime, timezone

SPOOL_DIR     = "/srv/exoria/spool"
DB_PATH       = "/srv/exoria/queue.db"
SESSION_RE    = re.compile(r"^session_\d{8}_\d{6}$")


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def find_nested(spool_dir):
    """Retourne la liste des (outer_dir, inner_dir) imbriqués."""
    nested = []
    try:
        for name in os.listdir(spool_dir):
            if not SESSION_RE.match(name):
                continue
            outer = os.path.join(spool_dir, name)
            if not os.path.isdir(outer):
                continue
            inner = os.path.join(outer, name)
            if os.path.isdir(inner):
                nested.append((outer, inner))
    except Exception as e:
        print(f"[ERREUR] Impossible de lire {spool_dir} : {e}")
    return nested


def db_has_session(conn, session_id):
    row = conn.execute("SELECT id, status FROM jobs WHERE session_id=?", (session_id,)).fetchone()
    return row  # None ou (id, status)


def dir_size_and_count(path):
    total, count = 0, 0
    for dirpath, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
            count += 1
    return total, count


def fix_nested(outer, inner, session_id, conn, apply):
    """
    Répare un cas d'imbrication :
      outer = spool/session_X   (résidu vide ou avec d'autres fichiers)
      inner = spool/session_X/session_X  (la vraie session)
    """
    # Contenu de outer en dehors de inner
    try:
        outer_contents = set(os.listdir(outer))
    except Exception as e:
        print(f"  [ERREUR] listdir({outer}) : {e}")
        return False

    other_in_outer = outer_contents - {session_id}

    print(f"\n[SESSION] {session_id}")
    print(f"  outer  : {outer}")
    print(f"  inner  : {inner}")
    if other_in_outer:
        print(f"  autres fichiers dans outer : {other_in_outer}")

    db_row = db_has_session(conn, session_id)
    if db_row:
        print(f"  DB     : job existant id={db_row[0][:8]}… status={db_row[1]}")
    else:
        print(f"  DB     : aucune entrée — sera insérée en 'queued'")

    if not apply:
        print(f"  ACTION : (dry-run) déplacerait inner → outer")
        return True

    # ── Appliquer ────────────────────────────────────────────────────────────
    tmp = outer + "_fixing_" + uuid.uuid4().hex[:6]
    try:
        # 1. Déplace inner vers un nom temporaire hors de outer
        os.rename(inner, tmp)

        # 2. Supprime outer (résidu, maintenant vide ou avec les autres fichiers)
        if other_in_outer:
            print(f"  WARN   : outer contient d'autres entrées — rmtree quand même")
        shutil.rmtree(outer)

        # 3. Renomme tmp → outer (position correcte)
        os.rename(tmp, outer)

        print(f"  OK     : inner remonté vers {outer}")
    except Exception as e:
        print(f"  [ERREUR] Opération filesystem : {e}")
        # Rollback best-effort
        if os.path.isdir(tmp) and not os.path.exists(outer):
            try:
                os.rename(tmp, outer)
                print(f"  ROLLBACK : tmp renommé en outer")
            except Exception as e2:
                print(f"  ROLLBACK ÉCHOUÉ : {e2} — session dans {tmp}")
        return False

    # ── DB ───────────────────────────────────────────────────────────────────
    if not db_row:
        try:
            size_bytes, file_count = dir_size_and_count(outer)
            jid = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO jobs(id,session_dir,session_id,size_bytes,file_count,"
                "status,attempts,last_error,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (jid, outer, session_id, size_bytes, file_count,
                 "queued", 0, "", now_iso(), now_iso()),
            )
            conn.commit()
            print(f"  DB     : job inséré id={jid[:8]}… queued ({file_count} fichiers, {size_bytes/1e6:.1f} MB)")
        except Exception as e:
            print(f"  [ERREUR] DB insert : {e}")
    else:
        # Mettre à jour session_dir si le chemin a changé
        try:
            conn.execute("UPDATE jobs SET session_dir=? WHERE session_id=?", (outer, session_id))
            conn.commit()
        except Exception:
            pass

    return True


def main():
    parser = argparse.ArgumentParser(description="Répare les sessions imbriquées dans spool/")
    parser.add_argument("--apply", action="store_true", help="Applique les corrections (sans cette option : dry-run)")
    parser.add_argument("--spool-dir", default=SPOOL_DIR)
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    if not args.apply:
        print("=== DRY-RUN — aucune modification. Ajoutez --apply pour corriger. ===\n")

    nested = find_nested(args.spool_dir)

    if not nested:
        print("Aucune session imbriquée trouvée. Rien à faire.")
        return 0

    print(f"{len(nested)} session(s) imbriquée(s) trouvée(s).")

    try:
        conn = sqlite3.connect(args.db, timeout=30)
    except Exception as e:
        print(f"[ERREUR] Impossible d'ouvrir la DB {args.db} : {e}")
        return 1

    ok = fail = 0
    for outer, inner in sorted(nested):
        session_id = os.path.basename(outer)
        success = fix_nested(outer, inner, session_id, conn, apply=args.apply)
        if success:
            ok += 1
        else:
            fail += 1

    conn.close()

    print(f"\n{'─'*60}")
    if args.apply:
        print(f"Résultat : {ok} corrigée(s), {fail} erreur(s)")
    else:
        print(f"Résultat dry-run : {ok} à corriger, {fail} erreur(s) détectée(s)")
        print("\nLancez avec --apply pour appliquer.")

    return 0 if fail == 0 else 1


def cmd_status(db_path):
    """Affiche un résumé de la DB sans sqlite3 CLI."""
    try:
        conn = sqlite3.connect(db_path, timeout=30)
    except Exception as e:
        print(f"[ERREUR] DB : {e}")
        return 1
    rows = conn.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status ORDER BY status").fetchall()
    conn.close()
    print(f"{'Status':<12} {'Count':>8}")
    print("─" * 22)
    for status, count in rows:
        print(f"{status:<12} {count:>8}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "status":
        sys.exit(cmd_status(DB_PATH))
    sys.exit(main())
