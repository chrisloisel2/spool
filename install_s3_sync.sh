#!/usr/bin/env bash
# install_s3_sync.sh — Installe s3_sync.py comme service systemd
#
# Usage (sur le serveur, en root) :
#   bash install_s3_sync.sh [options]
#
# Options :
#   --inbox DIR            Dossier à surveiller       (défaut: /srv/exoria/inbox)
#   --bucket NAME          Bucket S3                  (défaut: physical-data-storage)
#   --prefix PATH          Préfixe S3                 (défaut: bronze)
#   --interval N           Scan toutes les N secondes (défaut: 5)
#   --stable N             Stabilité avant upload     (défaut: 10)
#   --workers N            Threads upload             (défaut: 4)
#   --delete-after-upload  Supprimer après upload
#   --aws-key KEY          AWS_ACCESS_KEY_ID
#   --aws-secret SECRET    AWS_SECRET_ACCESS_KEY
#   --aws-region REGION    AWS_DEFAULT_REGION         (défaut: eu-west-3)

set -euo pipefail

SERVICE_NAME="exoria-s3-sync"
SCRIPT_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/s3_sync.py"
INSTALL_DIR="/opt/exoria"
INSTALL_SCRIPT="$INSTALL_DIR/s3_sync.py"
LOG_DIR="/var/log/exoria"
PYTHON="$(command -v python3)"

# ── Defaults ──────────────────────────────────────────────────────────────────
INBOX="/srv/exoria/inbox"
BUCKET="physical-data-storage"
PREFIX="bronze"
INTERVAL=5
STABLE=10
WORKERS=4
DELETE_FLAG=""
AWS_KEY=""
AWS_SECRET=""
AWS_REGION="eu-west-3"
KAFKA_BROKER="192.168.88.4:9092"

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --inbox)               INBOX="$2";       shift 2 ;;
    --bucket)              BUCKET="$2";      shift 2 ;;
    --prefix)              PREFIX="$2";      shift 2 ;;
    --interval)            INTERVAL="$2";    shift 2 ;;
    --stable)              STABLE="$2";      shift 2 ;;
    --workers)             WORKERS="$2";     shift 2 ;;
    --delete-after-upload) DELETE_FLAG="--delete-after-upload"; shift ;;
    --aws-key)             AWS_KEY="$2";     shift 2 ;;
    --aws-secret)          AWS_SECRET="$2";  shift 2 ;;
    --aws-region)          AWS_REGION="$2";  shift 2 ;;
    --broker)              KAFKA_BROKER="$2"; shift 2 ;;
    *) echo "Option inconnue: $1"; exit 1 ;;
  esac
done

# ── Couleurs ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RESET='\033[0m'; BOLD='\033[1m'
ok()   { echo -e "  ${GREEN}✔${RESET}  $*"; }
info() { echo -e "  ${CYAN}…${RESET}  $*"; }
warn() { echo -e "  ${YELLOW}!${RESET}  $*"; }

echo -e "\n${BOLD}Installation de $SERVICE_NAME${RESET}"
echo "  Inbox    : $INBOX"
echo "  Bucket   : s3://$BUCKET/$PREFIX/"
echo "  Interval : ${INTERVAL}s  stable=${STABLE}s  workers=$WORKERS"
[[ -n "$DELETE_FLAG" ]] && echo "  Mode     : delete-after-upload activé"
echo ""

# ── Checks ────────────────────────────────────────────────────────────────────
[[ "$(id -u)" -eq 0 ]] || { echo "Erreur : lancer en root (sudo)"; exit 1; }
[[ -f "$SCRIPT_SRC" ]] || { echo "Erreur : $SCRIPT_SRC introuvable"; exit 1; }

# ── Dépendances Python ────────────────────────────────────────────────────────
info "Vérification boto3…"
if ! "$PYTHON" -c "import boto3" 2>/dev/null; then
  info "Installation de boto3…"
  pip3 install --quiet boto3
  ok "boto3 installé"
else
  ok "boto3 présent"
fi

# ── Répertoires ───────────────────────────────────────────────────────────────
mkdir -p "$INSTALL_DIR" "$LOG_DIR"
ok "Répertoires prêts"

# ── Credentials AWS ───────────────────────────────────────────────────────────
# Si fournis en argument, on les écrit dans /opt/exoria/.aws/
if [[ -n "$AWS_KEY" && -n "$AWS_SECRET" ]]; then
  info "Écriture des credentials AWS…"
  mkdir -p "$INSTALL_DIR/.aws"
  cat > "$INSTALL_DIR/.aws/credentials" <<CREDS
[default]
aws_access_key_id     = $AWS_KEY
aws_secret_access_key = $AWS_SECRET
CREDS
  cat > "$INSTALL_DIR/.aws/config" <<CFG
[default]
region = $AWS_REGION
output = json
CFG
  chmod 600 "$INSTALL_DIR/.aws/credentials" "$INSTALL_DIR/.aws/config"
  ok "Credentials écrits dans $INSTALL_DIR/.aws/"
else
  warn "Pas de credentials fournis — le service utilisera ~/.aws/ ou les variables d'env"
  warn "Pour les configurer plus tard : aws configure --profile default"
fi

# ── Copie du script ───────────────────────────────────────────────────────────
cp "$SCRIPT_SRC" "$INSTALL_SCRIPT"
chmod 755 "$INSTALL_SCRIPT"
ok "Script copié → $INSTALL_SCRIPT"

# ── Fichier systemd ───────────────────────────────────────────────────────────
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
info "Création du service systemd → $UNIT_FILE"

# Construction de la commande ExecStart
EXEC_CMD="$PYTHON $INSTALL_SCRIPT"
EXEC_CMD+=" --inbox $INBOX"
EXEC_CMD+=" --bucket $BUCKET"
EXEC_CMD+=" --prefix $PREFIX"
EXEC_CMD+=" --interval $INTERVAL"
EXEC_CMD+=" --stable $STABLE"
EXEC_CMD+=" --workers $WORKERS"
EXEC_CMD+=" --broker $KAFKA_BROKER"
[[ -n "$DELETE_FLAG" ]] && EXEC_CMD+=" $DELETE_FLAG"

# Variables d'env AWS pour le service
AWS_ENV_BLOCK=""
if [[ -n "$AWS_KEY" ]]; then
  AWS_ENV_BLOCK="Environment=AWS_SHARED_CREDENTIALS_FILE=$INSTALL_DIR/.aws/credentials
Environment=AWS_CONFIG_FILE=$INSTALL_DIR/.aws/config"
fi

cat > "$UNIT_FILE" <<EOF
[Unit]
Description=Exoria S3 Sync — inbox vers s3://$BUCKET/$PREFIX/
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$EXEC_CMD
Restart=always
RestartSec=15
StandardOutput=append:$LOG_DIR/${SERVICE_NAME}.log
StandardError=append:$LOG_DIR/${SERVICE_NAME}.log
Environment=PYTHONUNBUFFERED=1
$AWS_ENV_BLOCK

[Install]
WantedBy=multi-user.target
EOF

ok "Unit file écrit"

# ── Activation ────────────────────────────────────────────────────────────────
systemctl daemon-reload
systemctl enable  "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
sleep 2

if systemctl is-active --quiet "$SERVICE_NAME"; then
  ok "Service démarré et actif"
else
  warn "Le service ne semble pas actif — vérifier :"
  echo "    journalctl -u $SERVICE_NAME -n 30"
  echo "    tail -f $LOG_DIR/${SERVICE_NAME}.log"
  exit 1
fi

# ── Résumé ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}Résumé${RESET}"
echo "  Statut      : $(systemctl is-active $SERVICE_NAME)"
echo "  Destination : s3://$BUCKET/$PREFIX/<session_id>/"
echo "  Logs        : tail -f $LOG_DIR/${SERVICE_NAME}.log"
echo "  Arrêter     : systemctl stop $SERVICE_NAME"
echo "  Désinstaller: systemctl disable --now $SERVICE_NAME && rm $UNIT_FILE $INSTALL_SCRIPT"
echo ""
