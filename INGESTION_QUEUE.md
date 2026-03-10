# Ingestion Queue — Référence des jobs

## Infra

| Paramètre        | Valeur par défaut         |
|------------------|---------------------------|
| Broker RabbitMQ  | `192.168.88.246:5672`     |
| Queue            | `ingestion_queue`         |
| Durable          | `true`                    |
| Delivery mode    | `2` (persistent)          |
| Content-Type     | `application/json`        |
| Env var override | `INGESTION_QUEUE`         |

La queue est déclarée automatiquement au moment de la publication si elle n'existe pas encore.

---

## Quand un job est publié

Un job est publié dans `ingestion_queue` à la **fin du pipeline de succès**, après que tous les fichiers de la session ont été uploadés sur le NAS (`bronze/landing`) sans erreur.

```
upload/completed  →  ingestion_queue/job_published
```

Les sessions en **quarantaine** (`bronze/quarantine`) ne génèrent **pas** de job dans cette queue.

---

## Structure du message

```json
{
  "session_id":      "session_20260308_161838",
  "zone":            "bronze/landing",
  "nas_paths":       {
    "metadata.json":            "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/metadata.json",
    "tracker_positions.csv":    "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/tracker_positions.csv",
    "gripper_left_data.csv":    "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/gripper_left_data.csv",
    "gripper_right_data.csv":   "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/gripper_right_data.csv",
    "videos/head.mp4":          "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/head.mp4",
    "videos/left.mp4":          "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/left.mp4",
    "videos/right.mp4":         "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/right.mp4",
    "videos/head.jsonl":        "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/head.jsonl",
    "videos/left.jsonl":        "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/left.jsonl",
    "videos/right.jsonl":       "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/videos/right.jsonl"
  },
  "manifest_remote": "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/_manifest.json",
  "created_at":      "2026-03-08T16:18:58Z"
}
```

---

## Référence des champs

| Champ             | Type     | Description |
|-------------------|----------|-------------|
| `session_id`      | `string` | Identifiant complet de la session (`session_YYYYMMDD_HHMMSS`) |
| `zone`            | `string` | Zone NAS où les fichiers ont été déposés. Toujours `"bronze/landing"` pour les jobs de cette queue |
| `nas_paths`       | `object` | Map `{ chemin_relatif → chemin_absolu_NAS }` de tous les fichiers uploadés avec succès |
| `manifest_remote` | `string` | Chemin NAS absolu du fichier `_manifest.json` qui contient les checksums et métadonnées de l'upload |
| `created_at`      | `string` | Horodatage UTC ISO 8601 de la publication du job |

### Détail de `nas_paths`

`nas_paths` ne contient que les fichiers dont l'upload a réussi (`ok: true`). La clé est le chemin relatif dans la session (ex : `"videos/head.mp4"`), la valeur est le chemin absolu SFTP sur le NAS.

Le pattern de construction des chemins NAS est :

```
/DB-EXORIA/lakehouse/<zone>/<YYYY>/<MM>/<DD>/<session_id>/<rel>
```

### Fichiers attendus dans `nas_paths`

| Clé relative           | Description                              |
|------------------------|------------------------------------------|
| `metadata.json`        | Métadonnées de session (scénario, caméras, trackers, durée, …) |
| `tracker_positions.csv`| Positions 6-DOF des trackers VIVE par frame |
| `gripper_left_data.csv`| Données capteur pince gauche             |
| `gripper_right_data.csv`| Données capteur pince droite            |
| `videos/head.mp4`      | Vidéo caméra tête                        |
| `videos/left.mp4`      | Vidéo caméra gauche                      |
| `videos/right.mp4`     | Vidéo caméra droite                      |
| `videos/head.jsonl`    | Annotations frame-level caméra tête      |
| `videos/left.jsonl`    | Annotations frame-level caméra gauche    |
| `videos/right.jsonl`   | Annotations frame-level caméra droite    |

### Le fichier `_manifest.json`

Le manifeste est un fichier JSON déposé sur le NAS à la fin de chaque upload. Il contient :

```json
{
  "session_id":  "session_20260308_161838",
  "uploaded_at": "2026-03-08T16:18:58Z",
  "zone":        "bronze/landing",
  "files": {
    "metadata.json": {
      "remote":     "/DB-EXORIA/...",
      "speed_mbps": 45.2,
      "size_bytes": 1024,
      "sha256":     "a3f...",
      "elapsed_s":  0.021,
      "ok":         true
    }
  },
  "inspection": { "...métadonnées de session..." },
  "checksums":  { "metadata.json": "a3f...", "..." : "..." }
}
```

---

## Events Kafka associés

Ces events sont émis sur le topic `monitoring` en parallèle de la publication RabbitMQ.

### `ingestion_queue / job_published`
Publié quand le job a été inséré dans la queue avec succès.

```json
{
  "source":          "inspect_session",
  "step":            "ingestion_queue",
  "status":          "job_published",
  "session_id":      "session_20260308_161838",
  "queue":           "ingestion_queue",
  "zone":            "bronze/landing",
  "files_count":     10,
  "manifest_remote": "/DB-EXORIA/lakehouse/bronze/landing/2026/03/08/session_20260308_161838/_manifest.json"
}
```

| Champ             | Description |
|-------------------|-------------|
| `queue`           | Nom de la queue RabbitMQ utilisée |
| `files_count`     | Nombre de fichiers référencés dans `nas_paths` |
| `manifest_remote` | Chemin NAS du manifeste |

### `ingestion_queue / publish_failed`
Publié si la connexion RabbitMQ échoue au moment de la publication. Le pipeline retourne quand même `True` — l'upload NAS a réussi.

```json
{
  "source":     "inspect_session",
  "step":       "ingestion_queue",
  "status":     "publish_failed",
  "session_id": "session_20260308_161838",
  "queue":      "ingestion_queue",
  "error":      "ConnectionClosed: ..."
}
```

---

## Position dans le pipeline

```
pipeline/started
  ├─ inspection/started → ... → inspection/completed
  ├─ pipeline/inspection_passed
  ├─ upload/started → ... → upload/completed
  ├─ ingestion_queue/job_published   ← ici
  └─ pipeline/completed
```
