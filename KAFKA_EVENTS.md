# Kafka Events — spool

## Infra

| Paramètre | Valeur |
|-----------|--------|
| Broker | `192.168.88.4:9092` |
| Topic | `monitoring` |
| Acks | `all` (spool.py) / non configuré (run.sh) |
| Retries | `10` (spool.py) / `3` (run.sh) |
| API version | `2.0` |

---

## Sources

| `source` | Émetteur | Fréquence |
|----------|----------|-----------|
| `spool_daemon` | `run.sh` | À chaque start / stop / erreur démarrage |
| `spool_status` | `spool.py` → `SpoolReporter` | Toutes les secondes |
| `pc` | `spool.py` → `SpoolReporter` | Toutes les secondes (un msg par pc_id actif) |

---

## Events émis par `run.sh` (`source = "spool_daemon"`)

### Structure commune

Tous les messages de `run.sh` partagent ces champs de base :

```json
{
  "source":  "spool_daemon",
  "ts":      1772984655.162,
  "ts_iso":  "2026-03-13T02:25:00Z",
  "step":    "<step>",
  "status":  "<status>"
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `source` | `string` | Toujours `"spool_daemon"` |
| `ts` | `float` | Unix timestamp (secondes) |
| `ts_iso` | `string` | UTC ISO 8601, précision seconde (`Z`) |
| `step` | `string` | Catégorie de l'event |
| `status` | `string` | État dans la catégorie |

---

### `step = "daemon"` — Lifecycle du processus spool

#### `daemon / started`
Émis après que le daemon a démarré avec succès (vérification 2s après `nohup`).

```json
{
  "source":   "spool_daemon",
  "ts":       1772984655.162,
  "ts_iso":   "2026-03-13T02:25:00Z",
  "step":     "daemon",
  "status":   "started",
  "pid":      9677,
  "log":      "/srv/exoria/spool.log",
  "workers":  16,
  "nas_host": "192.168.88.82"
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `pid` | `int` | PID du processus daemon |
| `log` | `string` | Chemin du fichier de log |
| `workers` | `int` | Nombre de workers SFTP configurés (16) |
| `nas_host` | `string` | IP du NAS cible |

---

#### `daemon / start_failed`
Émis si le daemon crashe dans les 2 premières secondes après le lancement.

```json
{
  "source":  "spool_daemon",
  "ts":      1772984655.162,
  "ts_iso":  "2026-03-13T02:25:00Z",
  "step":    "daemon",
  "status":  "start_failed",
  "log":     "/srv/exoria/spool.log"
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `log` | `string` | Chemin du fichier de log (consulter pour la cause) |

---

#### `daemon / stopped`
Émis après l'arrêt propre du daemon (`./run.sh stop` ou `./run.sh restart`).

```json
{
  "source":  "spool_daemon",
  "ts":      1772984655.162,
  "ts_iso":  "2026-03-13T02:25:00Z",
  "step":    "daemon",
  "status":  "stopped",
  "pid":     9677
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `pid` | `int` | PID du processus arrêté |

> **Note :** Si le process résiste au SIGTERM et reçoit un SIGKILL, l'event `stopped` est quand même émis après la fin effective du process.

---

### Séquence lifecycle complète

```
./run.sh run / restart
  └─ daemon/started   {pid, log, workers, nas_host}

./run.sh stop / restart
  └─ daemon/stopped   {pid}

./run.sh run  (crash au démarrage)
  └─ daemon/start_failed  {log}
```

---

## Events émis par `spool.py` (`source = "spool_status"`)

> Voir la section suivante — ces events sont émis indépendamment de `run.sh`.

---

# Kafka Events — inspect_session

## Infra

| Paramètre | Valeur par défaut |
|-----------|-------------------|
| Broker | `192.168.88.4:9092` |
| Topic | `monitoring` |
| Source (champ) | `"inspect_session"` |
| Acks | `all` |
| Retries | `5` |
| API version | `2.0` |

Tous les messages sont publiés sur **`monitoring`** (topic unique partagé avec `spool.py`).

---

## Structure commune

Chaque message est un objet JSON avec les champs de base suivants, auxquels s'ajoutent les champs spécifiques à l'event :

```json
{
  "source":     "inspect_session",
  "ts":         1772984655.162,
  "ts_iso":     "2026-03-08T15:44:15Z",
  "step":       "<step>",
  "status":     "<status>",
  "session_id": "session_20260308_161838",
  ...champs spécifiques
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `source` | `string` | Toujours `"inspect_session"` |
| `ts` | `float` | Unix timestamp (secondes, précision µs) |
| `ts_iso` | `string` | UTC ISO 8601, précision seconde (`Z`) |
| `step` | `string` | Catégorie de l'event (voir tableau ci-dessous) |
| `status` | `string` | État spécifique dans la catégorie |
| `session_id` | `string` | Identifiant de la session (ex : `session_20260308_161838`) |

---

## Référence complète des events

### `step = "app"` — Démarrage du processus

#### `app / start`
Émis une seule fois au lancement du script, avant tout traitement.

```json
{
  "step":           "app",
  "status":         "start",
  "session_id":     "",
  "has_kafka":      true,
  "has_pika":       true,
  "has_paramiko":   true,
  "kafka_broker":   "192.168.88.4:9092",
  "nas_host":       "192.168.88.248",
  "rabbitmq_host":  "192.168.88.4",
  "rabbitmq_queue": "annotation_queue"
}
```

---

### `step = "consumer"` — Lifecycle du consumer RabbitMQ

#### `consumer / started`
Le consumer RabbitMQ est connecté et écoute la queue.

```json
{
  "step":            "consumer",
  "status":          "started",
  "session_id":      "",
  "rabbitmq_host":   "192.168.88.4",
  "rabbitmq_queue":  "annotation_queue"
}
```

#### `consumer / message_received`
Un message a été dépilé de `annotation_queue`.

```json
{
  "step":          "consumer",
  "status":        "message_received",
  "session_id":    "session_20260308_161838",
  "delivery_tag":  42,
  "body_keys":     ["session_id", "session_dir"]
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `delivery_tag` | `int` | Tag RabbitMQ du message |
| `body_keys` | `string[]` | Clés présentes dans le message |

#### `consumer / session_dir_unresolved`
Impossible de trouver le dossier de session correspondant au message.

```json
{
  "step":       "consumer",
  "status":     "session_dir_unresolved",
  "session_id": "?",
  "body":       { "session_id": "unknown_id" }
}
```

#### `consumer / pipeline_exception`
Exception non gérée pendant l'exécution du pipeline.

```json
{
  "step":       "consumer",
  "status":     "pipeline_exception",
  "session_id": "session_20260308_161838",
  "error":      "FileNotFoundError: ..."
}
```

#### `consumer / message_acked`
Pipeline terminé avec succès → ACK RabbitMQ envoyé.

```json
{
  "step":         "consumer",
  "status":       "message_acked",
  "session_id":   "session_20260308_161838",
  "delivery_tag": 42
}
```

#### `consumer / message_nacked`
Pipeline échoué → NACK RabbitMQ envoyé (`requeue=False`).

```json
{
  "step":         "consumer",
  "status":       "message_nacked",
  "session_id":   "session_20260308_161838",
  "delivery_tag": 42
}
```

---

### `step = "pipeline"` — Orchestration globale

#### `pipeline / started`
Début du pipeline pour une session.

```json
{
  "step":        "pipeline",
  "status":      "started",
  "session_id":  "session_20260308_161838",
  "session_dir": "/srv/exoria/sessions/session_20260308_161838"
}
```

#### `pipeline / inspection_passed`
L'inspection locale est OK, on passe à l'upload.

```json
{
  "step":         "pipeline",
  "status":       "inspection_passed",
  "session_id":   "session_20260308_161838",
  "checks_count": 22,
  "metadata": {
    "scenario":         "Pyramide",
    "start_time":       "2026-03-08T15:18:38.555501+00:00",
    "end_time":         "2026-03-08T15:18:58.812008+00:00",
    "duration_seconds": 20.26,
    "failed":           false,
    "cameras_count":    3,
    "trackers_count":   3,
    "video_config":     { "width": 1920, "height": 1200, "fps": 30 }
  }
}
```

#### `pipeline / inspection_failed`
L'inspection a détecté des erreurs → session envoyée en quarantaine NAS.

```json
{
  "step":          "pipeline",
  "status":        "inspection_failed",
  "session_id":    "session_20260308_161838",
  "errors":        ["Fichier manquant : videos/head.mp4"],
  "failed_checks": ["file_present:videos/head.mp4", "video_size:videos/head.mp4"]
}
```

#### `pipeline / quarantine_upload_failed`
L'upload en quarantaine NAS a lui-même échoué.

```json
{
  "step":       "pipeline",
  "status":     "quarantine_upload_failed",
  "session_id": "session_20260308_161838",
  "error":      "NAS 192.168.88.248:22 injoignable"
}
```

#### `pipeline / upload_failed`
L'upload NAS landing a échoué (exception).

```json
{
  "step":       "pipeline",
  "status":     "upload_failed",
  "session_id": "session_20260308_161838",
  "error":      "NAS operation failed op=UPLOAD_FILE"
}
```

#### `pipeline / upload_partial_failure`
L'upload est terminé mais certains fichiers n'ont pas été transférés.

```json
{
  "step":         "pipeline",
  "status":       "upload_partial_failure",
  "session_id":   "session_20260308_161838",
  "failed_files": ["videos/right.mp4"]
}
```

#### `pipeline / completed`
Pipeline terminé avec succès, tous les fichiers sont sur le NAS.

```json
{
  "step":        "pipeline",
  "status":      "completed",
  "session_id":  "session_20260308_161838",
  "zone":        "bronze/landing",
  "files_count": 10,
  "metadata": {
    "scenario":         "Pyramide",
    "duration_seconds": 20.26,
    "cameras_count":    3,
    "trackers_count":   3
  },
  "upload_summary": {
    "metadata.json": {
      "remote":     "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/metadata.json",
      "speed_mbps": 0.13,
      "sha256":     "d9cca827..."
    },
    "videos/head.mp4": {
      "remote":     "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/head.mp4",
      "speed_mbps": 7.15,
      "sha256":     "e1c49471..."
    }
  }
}
```

---

### `step = "inspection"` — Inspection locale de la session

#### `inspection / started`
Début de l'inspection d'un dossier de session.

```json
{
  "step":        "inspection",
  "status":      "started",
  "session_id":  "session_20260308_161838",
  "session_dir": "/srv/exoria/sessions/session_20260308_161838"
}
```

#### `inspection / aborted`
Inspection interrompue immédiatement car le dossier est introuvable.

```json
{
  "step":       "inspection",
  "status":     "aborted",
  "session_id": "session_20260308_161838",
  "reason":     "session_dir_not_found"
}
```

#### `inspection / completed`
Inspection terminée (succès ou échec — voir le champ `ok`).

```json
{
  "step":          "inspection",
  "status":        "completed",
  "session_id":    "session_20260308_161838",
  "ok":            true,
  "total_checks":  22,
  "failed_checks": [],
  "errors":        [],
  "metadata": {
    "scenario":         "Pyramide",
    "duration_seconds": 20.26,
    "failed":           false,
    "cameras_count":    3,
    "trackers_count":   3
  }
}
```

---

### `step = "check"` — Checks individuels d'inspection

Ces events encadrent chaque vérification. Ils sont émis par paires `*_start` / `*_done`.

#### `check / required_files_start` & `check / required_files_done`

```json
{ "step": "check", "status": "required_files_start", "session_id": "..." }
```

```json
{
  "step":          "check",
  "status":        "required_files_done",
  "session_id":    "session_20260308_161838",
  "ok":            true,
  "total_size_mb": 52.54
}
```

#### `check / metadata_start` & `check / metadata_done`

```json
{ "step": "check", "status": "metadata_start", "session_id": "..." }
```

```json
{
  "step":       "check",
  "status":     "metadata_done",
  "session_id": "session_20260308_161838",
  "ok":         true,
  "metadata": {
    "scenario":         "Pyramide",
    "start_time":       "2026-03-08T15:18:38.555501+00:00",
    "end_time":         "2026-03-08T15:18:58.812008+00:00",
    "duration_seconds": 20.26,
    "failed":           false,
    "cameras_count":    3,
    "trackers_count":   3,
    "video_config":     { "width": 1920, "height": 1200, "fps": 30 }
  }
}
```

#### `check / csv_start` & `check / csv_done`
Émis pour chaque CSV : `tracker_positions.csv`, `gripper_left_data.csv`, `gripper_right_data.csv`.

```json
{ "step": "check", "status": "csv_start", "session_id": "...", "file": "tracker_positions.csv" }
```

```json
{
  "step":       "check",
  "status":     "csv_done",
  "session_id": "session_20260308_161838",
  "file":       "tracker_positions.csv",
  "ok":         true,
  "rows":       179
}
```

#### `check / jsonl_start` & `check / jsonl_done`
Émis pour chaque JSONL : `videos/head.jsonl`, `videos/left.jsonl`, `videos/right.jsonl`.

```json
{ "step": "check", "status": "jsonl_start", "session_id": "...", "file": "videos/head.jsonl" }
```

```json
{
  "step":       "check",
  "status":     "jsonl_done",
  "session_id": "session_20260308_161838",
  "file":       "videos/head.jsonl",
  "ok":         true,
  "entries":    479
}
```

#### `check / sha256_all_start` & `check / sha256_all_done`

```json
{
  "step":       "check",
  "status":     "sha256_all_start",
  "session_id": "session_20260308_161838",
  "file_count": 10
}
```

```json
{
  "step":       "check",
  "status":     "sha256_all_done",
  "session_id": "session_20260308_161838",
  "file_count": 10,
  "checksums": {
    "metadata.json":          "d9cca8279754fe08dec6957c53495f2438dc899d446fa96e4a71075713eb6ba9",
    "tracker_positions.csv":  "d14643c0893e169a9d54924ca469bea7f6def9e89136046242285a1bfcedbef2",
    "gripper_left_data.csv":  "7c43233a67f249ece583dd7da85e6373dd093ec3feb358a7cfd403022bad210d",
    "gripper_right_data.csv": "9019a363f2340fc4cf32b8214839b1b29373e8f61d30924e8450a326927b9449",
    "videos/head.mp4":        "e1c49471015caa2969ab5738595f2e0836cc32073c1be06bfe4faaf06cd1004b",
    "videos/left.mp4":        "69747e197ce139d48c5d4b7e5038f55e9a0b9d30b22a1318f3b20d3870958ba0",
    "videos/right.mp4":       "e9d8fdc841617f221e77b7577e217f95e7e27de55446818c007f9a151c156ab4",
    "videos/head.jsonl":      "0454154ccdadf6ad1e2cc0f6c252de6155b95283ad78a86bd317e309f954e24f",
    "videos/left.jsonl":      "89d926d449b8fc534dc26fc53b1d48f6626b7c4e7ce5c5c628f3dafb439f33b8",
    "videos/right.jsonl":     "eaaddbe2c2a29f6ca5e807c1ebdd7eae288b7d04f410573e45b311623615c246"
  }
}
```

---

### `step = "sha256_compute"` — Calcul SHA256 par fichier

Émis pour chaque fichier, en paire `started` / `done`.

#### `sha256_compute / started`

```json
{
  "step":       "sha256_compute",
  "status":     "started",
  "session_id": "session_20260308_161838",
  "file":       "videos/right.mp4",
  "size_mb":    26.417
}
```

#### `sha256_compute / done`

```json
{
  "step":       "sha256_compute",
  "status":     "done",
  "session_id": "session_20260308_161838",
  "file":       "videos/right.mp4",
  "sha256":     "e9d8fdc841617f221e77b7577e217f95e7e27de55446818c007f9a151c156ab4"
}
```

---

### `step = "upload"` — Transfert SFTP vers le NAS

#### `upload / started`
Début de la phase d'upload (connexion NAS pas encore établie).

```json
{
  "step":       "upload",
  "status":     "started",
  "session_id": "session_20260308_161838",
  "zone":       "bronze/landing",
  "file_count": 10
}
```

#### `upload / nas_connect_failed`
Connexion SFTP impossible.

```json
{
  "step":       "upload",
  "status":     "nas_connect_failed",
  "session_id": "session_20260308_161838",
  "error":      "NAS 192.168.88.248:22 injoignable"
}
```

#### `upload / nas_connected`
Connexion SFTP établie avec succès.

```json
{
  "step":       "upload",
  "status":     "nas_connected",
  "session_id": "session_20260308_161838",
  "nas_host":   "192.168.88.248",
  "nas_port":   22
}
```

#### `upload / file_start`
Début du transfert d'un fichier individuel.

```json
{
  "step":       "upload",
  "status":     "file_start",
  "session_id": "session_20260308_161838",
  "file_index": 5,
  "file_total": 10,
  "rel":        "videos/head.mp4",
  "local":      "/srv/exoria/sessions/session_20260308_161838/videos/head.mp4",
  "remote":     "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/head.mp4",
  "size_bytes": 15153418,
  "size_mb":    14.451,
  "sha256":     "e1c49471015caa2969ab5738595f2e0836cc32073c1be06bfe4faaf06cd1004b"
}
```

#### `upload / file_done`
Fichier transféré avec succès (après rename `.part` → final).

```json
{
  "step":       "upload",
  "status":     "file_done",
  "session_id": "session_20260308_161838",
  "file_index": 5,
  "file_total": 10,
  "rel":        "videos/head.mp4",
  "remote":     "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/head.mp4",
  "size_bytes": 15153418,
  "size_mb":    14.451,
  "speed_mbps": 7.15,
  "elapsed_s":  2.022,
  "sha256":     "e1c49471015caa2969ab5738595f2e0836cc32073c1be06bfe4faaf06cd1004b"
}
```

#### `upload / file_error`
Erreur lors du transfert d'un fichier.

```json
{
  "step":       "upload",
  "status":     "file_error",
  "session_id": "session_20260308_161838",
  "rel":        "videos/right.mp4",
  "error":      "Socket closed",
  "traceback":  "Traceback (most recent call last):\n  ..."
}
```

#### `upload / manifest_uploaded`
Le fichier `_manifest.json` de session a été uploadé sur le NAS.

```json
{
  "step":       "upload",
  "status":     "manifest_uploaded",
  "session_id": "session_20260308_161838",
  "remote":     "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/_manifest.json"
}
```

#### `upload / completed`
Upload de tous les fichiers terminé.

```json
{
  "step":            "upload",
  "status":          "completed",
  "session_id":      "session_20260308_161838",
  "zone":            "bronze/landing",
  "files_uploaded":  10,
  "files_total":     10,
  "total_bytes":     55090788,
  "total_mb":        52.539,
  "avg_speed_mbps":  3.01,
  "manifest_remote": "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/_manifest.json"
}
```

---

## Séquence complète d'une session OK

```
app/start
  └─ consumer/started
       └─ consumer/message_received
            └─ pipeline/started
                 ├─ inspection/started
                 │    ├─ check/required_files_start
                 │    ├─ check/required_files_done
                 │    ├─ check/metadata_start
                 │    ├─ check/metadata_done
                 │    ├─ check/csv_start          (× 3)
                 │    ├─ check/csv_done           (× 3)
                 │    ├─ check/jsonl_start        (× 3)
                 │    ├─ check/jsonl_done         (× 3)
                 │    ├─ check/sha256_all_start
                 │    ├─ sha256_compute/started   (× 10)
                 │    ├─ sha256_compute/done      (× 10)
                 │    ├─ check/sha256_all_done
                 │    └─ inspection/completed  [ok=true]
                 ├─ pipeline/inspection_passed
                 ├─ upload/started
                 │    ├─ upload/nas_connected
                 │    ├─ upload/file_start        (× 10)
                 │    ├─ upload/file_done         (× 10)
                 │    ├─ upload/manifest_uploaded
                 │    └─ upload/completed
                 ├─ pipeline/completed
                 └─ consumer/message_acked
```

## Séquence d'une session KO (inspection échouée)

```
pipeline/started
  ├─ inspection/started
  │    └─ inspection/completed  [ok=false]
  ├─ pipeline/inspection_failed
  ├─ upload/started              ← quarantaine NAS
  │    └─ upload/completed       zone="bronze/quarantine"
  └─ consumer/message_nacked
```

---

## Nombre total d'events par session

| Condition | Events émis |
|-----------|-------------|
| Session OK (10 fichiers) | **~56 events** |
| Session KO (inspection) | **~34 events** |
| Session KO (upload NAS) | **~57 events** |

> Les checks `video_size` et `frames_dir` ne génèrent pas d'event Kafka, uniquement des logs.
