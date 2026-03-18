#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
server_heartbeat.py — Heartbeat serveur spool vers Kafka
Topic : monitoring  /  source : server_heartbeat
Intervalle : 10s (configurable via --interval)

Structure du message :
{
    "source":           "server_heartbeat",
    "ts":               1742300000.123,
    "ts_iso":           "2026-03-18T12:00:00Z",
    "server_id":        "spool-01",            # hostname
    "role":             "spool",
    "disk_free_gb":     142.5,
    "disk_total_gb":    500.0,
    "disk_used_pct":    71.5,
    "sessions_inbox":   ["session_..."],       # sessions présentes dans inbox/
    "sessions_inbox_count": 2,
    "sessions_spool":   ["session_..."],       # sessions en cours de traitement
    "sessions_spool_count": 0,
    "queue_pending":    3,                     # jobs en attente dans la DB SQLite
    "queue_processing": 1,
    "queue_done":       412,
    "queue_failed":     2,
    "uptime_s":         3600
}
"""

import os
import sys
import time
import json
import shutil
import socket
import sqlite3
import logging
import argparse
import datetime as dt

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = "192.168.88.4"
KAFKA_PORT     = 9092
KAFKA_TOPIC    = "monitoring"

INBOX_DIR      = "/srv/exoria/inbox/videos"
SPOOL_DIR      = "/srv/exoria/spool"
DB_PATH        = "/srv/exoria/queue.db"

HEARTBEAT_INTERVAL = 10  # secondes
SERVER_ID          = socket.gethostname()
ROLE               = "spool"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [heartbeat] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("heartbeat")

# ── Kafka ─────────────────────────────────────────────────────────────────────
try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    KafkaProducer = None
    HAS_KAFKA = False
    log.warning("kafka-python non installé — les messages seront loggés uniquement")

_producer = None


def _get_producer():
    global _producer
    if _producer:
        return _producer
    if not HAS_KAFKA:
        return None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=[f"{KAFKA_BROKER}:{KAFKA_PORT}"],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            api_version=(2, 0, 0),
            retries=5,
            acks="all",
            request_timeout_ms=10000,
        )
        log.info("Kafka connecté — %s:%d  topic=%s", KAFKA_BROKER, KAFKA_PORT, KAFKA_TOPIC)
        return _producer
    except Exception as e:
        log.error("Kafka connexion échouée : %s", e)
        _producer = None
        return None


def _emit(payload: dict):
    producer = _get_producer()
    if producer:
        try:
            producer.send(KAFKA_TOPIC, payload)
            producer.flush(timeout=5)
        except Exception as e:
            log.error("Kafka emit échoué : %s — payload loggé ci-dessous", e)
            global _producer
            _producer = None  # reset pour reconnecter au prochain cycle
            log.info("HEARTBEAT (kafka_fail) %s", json.dumps(payload))
    else:
        log.info("HEARTBEAT %s", json.dumps(payload))


# ── Collecte des données ──────────────────────────────────────────────────────

def _disk_info(path: str) -> tuple[float, float, float]:
    """Retourne (free_gb, total_gb, used_pct)."""
    try:
        total, used, free = shutil.disk_usage(path)
        free_gb  = round(free  / 1e9, 2)
        total_gb = round(total / 1e9, 2)
        used_pct = round(used / total * 100, 1) if total else 0.0
        return free_gb, total_gb, used_pct
    except Exception:
        return 0.0, 0.0, 0.0


def _list_sessions(directory: str) -> list[str]:
    """Liste les dossiers session_* dans un répertoire."""
    try:
        return sorted(
            e for e in os.listdir(directory)
            if e.startswith("session_") and os.path.isdir(os.path.join(directory, e))
        )
    except Exception:
        return []


def _db_stats() -> dict:
    """Lit les compteurs de jobs depuis la DB SQLite."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=3)
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM jobs GROUP BY status"
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


# ── Boucle principale ─────────────────────────────────────────────────────────

def run(interval: int):
    start_ts = time.time()
    log.info("Démarrage — server_id=%s  interval=%ds  broker=%s:%d",
             SERVER_ID, interval, KAFKA_BROKER, KAFKA_PORT)

    while True:
        try:
            now        = time.time()
            ts_iso     = dt.datetime.fromtimestamp(now, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            uptime_s   = int(now - start_ts)

            # Disque (on prend le path inbox comme référence du volume principal)
            ref_path = INBOX_DIR if os.path.exists(INBOX_DIR) else SPOOL_DIR
            free_gb, total_gb, used_pct = _disk_info(ref_path)

            # Sessions
            sessions_inbox = _list_sessions(INBOX_DIR)
            sessions_spool = _list_sessions(SPOOL_DIR)

            # Queue SQLite
            db = _db_stats()

            payload = {
                "source":                "server_heartbeat",
                "ts":                    round(now, 3),
                "ts_iso":                ts_iso,
                "server_id":             SERVER_ID,
                "role":                  ROLE,
                "disk_free_gb":          free_gb,
                "disk_total_gb":         total_gb,
                "disk_used_pct":         used_pct,
                "sessions_inbox":        sessions_inbox,
                "sessions_inbox_count":  len(sessions_inbox),
                "sessions_spool":        sessions_spool,
                "sessions_spool_count":  len(sessions_spool),
                "queue_pending":         db.get("queued", 0),
                "queue_processing":      db.get("processing", 0),
                "queue_done":            db.get("done", 0),
                "queue_failed":          db.get("failed", 0),
                "uptime_s":              uptime_s,
            }

            _emit(payload)
            log.debug("heartbeat émis — inbox=%d spool=%d disk=%.1f%%",
                      len(sessions_inbox), len(sessions_spool), used_pct)

        except Exception as e:
            log.error("Erreur lors de la collecte : %s", e)

        time.sleep(interval)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heartbeat serveur spool → Kafka")
    parser.add_argument("--interval", type=int, default=HEARTBEAT_INTERVAL,
                        help=f"Intervalle en secondes (défaut: {HEARTBEAT_INTERVAL})")
    parser.add_argument("--broker", default=f"{KAFKA_BROKER}:{KAFKA_PORT}",
                        help="Broker Kafka host:port")
    parser.add_argument("--server-id", default=SERVER_ID,
                        help="Identifiant du serveur (défaut: hostname)")
    args = parser.parse_args()

    # Override depuis args
    KAFKA_BROKER, KAFKA_PORT = args.broker.rsplit(":", 1)
    KAFKA_PORT = int(KAFKA_PORT)
    SERVER_ID  = args.server_id

    run(args.interval)
