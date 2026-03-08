#!/usr/bin/env bash
# install_service.sh
# Installe inspect_session.py comme service systemd sur Debian 12
# Usage (sur le serveur Debian) : sudo bash install_service.sh

set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────────
SERVICE_NAME="inspect-session"
SCRIPT_SRC="$(cd "$(dirname "$0")" && pwd)/inspect_session.py"
INSTALL_DIR="/opt/exoria/inspect"
SCRIPT_DEST="${INSTALL_DIR}/inspect_session.py"
VENV_DIR="${INSTALL_DIR}/venv"
RUN_USER="spool"          # utilisateur qui exécutera le service (doit exister)
SESSIONS_ROOT="/srv/exoria/sessions"

# Variables d'environnement injectées dans le service — modifie si besoin
RABBITMQ_HOST="192.168.88.246"
RABBITMQ_PORT="5672"
RABBITMQ_USER="admin"
RABBITMQ_PASS='Admin123456!'
RABBITMQ_VHOST="/"
RABBITMQ_QUEUE="ingestion_queue"
SCENARIOS_QUEUE="scenarios_queue"
KAFKA_BROKER="192.168.88.4:9092"
NAS_HOST="192.168.88.248"
NAS_USER="EXORIA"
NAS_PASS='NasExori@2026!!#'
# ─────────────────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[+]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
error()   { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }
ok()      { echo -e "${GREEN}[✓]${NC} $*"; }

# ── Prérequis ─────────────────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "Ce script doit être exécuté en root (sudo bash install_service.sh)"

info "Vérification des prérequis..."

command -v python3 &>/dev/null || error "python3 non trouvé — apt install python3"
command -v systemctl &>/dev/null || error "systemd introuvable"

PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
info "Python détecté : $PY_VERSION"

# ── Utilisateur système ───────────────────────────────────────────────────────
if ! id "$RUN_USER" &>/dev/null; then
    info "Création de l'utilisateur système '$RUN_USER'..."
    useradd --system --no-create-home --shell /usr/sbin/nologin "$RUN_USER"
    ok "Utilisateur '$RUN_USER' créé."
else
    ok "Utilisateur '$RUN_USER' existe déjà."
fi

# ── Répertoires ───────────────────────────────────────────────────────────────
info "Création des répertoires..."
mkdir -p "$INSTALL_DIR" "$SESSIONS_ROOT" /var/log/exoria
chown "$RUN_USER:$RUN_USER" "$INSTALL_DIR" "$SESSIONS_ROOT" /var/log/exoria
ok "Répertoires créés."

# ── Copie du script ───────────────────────────────────────────────────────────
info "Copie de inspect_session.py → $SCRIPT_DEST"
cp "$SCRIPT_SRC" "$SCRIPT_DEST"
chown "$RUN_USER:$RUN_USER" "$SCRIPT_DEST"
chmod 750 "$SCRIPT_DEST"
ok "Script copié."

# ── Virtualenv + dépendances ──────────────────────────────────────────────────
info "Création du virtualenv dans $VENV_DIR..."
python3 -m venv "$VENV_DIR"

info "Installation des dépendances Python..."
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
"${VENV_DIR}/bin/pip" install --quiet \
    paramiko \
    pika \
    kafka-python

chown -R "$RUN_USER:$RUN_USER" "$VENV_DIR"
ok "Virtualenv prêt."

# ── Fichier d'environnement (credentials séparés du unit) ─────────────────────
ENV_FILE="/etc/exoria/inspect-session.env"
info "Écriture du fichier d'environnement $ENV_FILE..."
mkdir -p /etc/exoria
chmod 750 /etc/exoria
cat > "$ENV_FILE" <<EOF
RABBITMQ_HOST=${RABBITMQ_HOST}
RABBITMQ_PORT=${RABBITMQ_PORT}
RABBITMQ_USER=${RABBITMQ_USER}
RABBITMQ_PASS=${RABBITMQ_PASS}
RABBITMQ_VHOST=${RABBITMQ_VHOST}
RABBITMQ_QUEUE=${RABBITMQ_QUEUE}
SCENARIOS_QUEUE=${SCENARIOS_QUEUE}
KAFKA_BROKER=${KAFKA_BROKER}
NAS_HOST=${NAS_HOST}
NAS_USER=${NAS_USER}
NAS_PASS=${NAS_PASS}
SESSIONS_ROOT=${SESSIONS_ROOT}
EOF
chown "root:$RUN_USER" "$ENV_FILE"
chmod 640 "$ENV_FILE"
ok "Fichier d'environnement écrit."

# ── Unit systemd ──────────────────────────────────────────────────────────────
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
info "Écriture du unit systemd $UNIT_FILE..."

cat > "$UNIT_FILE" <<EOF
[Unit]
Description=Exoria — Inspection et ingestion des sessions robotiques
Documentation=https://github.com/exoria/spool
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_USER}
WorkingDirectory=${INSTALL_DIR}

# Variables d'environnement (credentials)
EnvironmentFile=${ENV_FILE}

# Commande principale — consumer RabbitMQ en boucle infinie
ExecStart=${VENV_DIR}/bin/python3 ${SCRIPT_DEST}

# Redémarrage automatique en cas de crash
Restart=on-failure
RestartSec=10
StartLimitIntervalSec=120
StartLimitBurst=5

# Stdout/stderr → journald (lisibles via : journalctl -u ${SERVICE_NAME} -f)
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Sécurité minimale
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${SESSIONS_ROOT} /var/log/exoria /tmp
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

ok "Unit systemd écrit."

# ── Activation ────────────────────────────────────────────────────────────────
info "Rechargement de systemd..."
systemctl daemon-reload

info "Activation du service au démarrage..."
systemctl enable "${SERVICE_NAME}.service"

info "Démarrage immédiat du service..."
systemctl start "${SERVICE_NAME}.service"

sleep 2

STATUS=$(systemctl is-active "${SERVICE_NAME}.service" || true)
if [[ "$STATUS" == "active" ]]; then
    ok "Service '${SERVICE_NAME}' actif et en cours d'exécution."
else
    warn "Le service n'est pas encore actif (status=$STATUS)."
    warn "Vérifie les logs : journalctl -u ${SERVICE_NAME} -n 50"
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "  Installation terminée"
echo "════════════════════════════════════════════════════════"
echo "  Script    : $SCRIPT_DEST"
echo "  Venv      : $VENV_DIR"
echo "  Env file  : $ENV_FILE"
echo "  Unit      : $UNIT_FILE"
echo ""
echo "  Commandes utiles :"
echo "    systemctl status  ${SERVICE_NAME}"
echo "    systemctl restart ${SERVICE_NAME}"
echo "    systemctl stop    ${SERVICE_NAME}"
echo "    journalctl -u ${SERVICE_NAME} -f"
echo "════════════════════════════════════════════════════════"
