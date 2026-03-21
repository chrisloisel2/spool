#!/usr/bin/env bash
# install_spool_service.sh
# Installe spool.py comme service systemd 24/7 sur Debian 12
# Usage (sur la machine cible) : sudo bash install_spool_service.sh

set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────────
SERVICE_NAME="spool"
SCRIPT_SRC="$(cd "$(dirname "$0")" && pwd)/spool.py"
QUALITY_SRC="$(cd "$(dirname "$0")" && pwd)/quality_processus"
INSTALL_DIR="/opt/exoria/spool"
QUALITY_DEST="${INSTALL_DIR}/quality_processus"
SCRIPT_DEST="${INSTALL_DIR}/spool.py"
VENV_DIR="${INSTALL_DIR}/venv"
RUN_USER="spool"

# Répertoires runtime
INBOX_DIR="/srv/exoria/inbox"
SPOOL_DIR="/srv/exoria/spool"
QUARANTINE_DIR="/srv/exoria/quarantine"
LOG_DIR="/srv/exoria/logs"
DB_DIR="/srv/exoria"

# ─────────────────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[+]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }
ok()    { echo -e "${GREEN}[✓]${NC} $*"; }

# ── Prérequis ─────────────────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "Ce script doit être exécuté en root (sudo bash install_spool_service.sh)"

command -v python3 &>/dev/null || error "python3 non trouvé"
command -v systemctl &>/dev/null || error "systemd introuvable"
[[ -f "$SCRIPT_SRC" ]] || error "spool.py introuvable : $SCRIPT_SRC"
[[ -d "$QUALITY_SRC" ]] || error "quality_processus/ introuvable : $QUALITY_SRC"

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
mkdir -p "$INSTALL_DIR" "$INBOX_DIR" "$SPOOL_DIR" "$QUARANTINE_DIR" "$LOG_DIR" "$DB_DIR" /var/log/exoria
chown "$RUN_USER:$RUN_USER" "$INSTALL_DIR" "$INBOX_DIR" "$SPOOL_DIR" "$QUARANTINE_DIR" "$LOG_DIR" "$DB_DIR" /var/log/exoria
ok "Répertoires créés."

# ── Copie des scripts ─────────────────────────────────────────────────────────
info "Copie de spool.py → $SCRIPT_DEST"
cp "$SCRIPT_SRC" "$SCRIPT_DEST"
chown "$RUN_USER:$RUN_USER" "$SCRIPT_DEST"
chmod 750 "$SCRIPT_DEST"

info "Copie de quality_processus/ → $QUALITY_DEST"
rm -rf "$QUALITY_DEST"
cp -r "$QUALITY_SRC" "$QUALITY_DEST"
chown -R "$RUN_USER:$RUN_USER" "$QUALITY_DEST"
chmod -R 755 "$QUALITY_DEST"
ok "Scripts copiés."

# ── Virtualenv + dépendances ──────────────────────────────────────────────────
info "Création du virtualenv dans $VENV_DIR..."
python3 -m venv "$VENV_DIR"

info "Installation des dépendances Python..."
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
"${VENV_DIR}/bin/pip" install --quiet \
    paramiko \
    kafka-python \
    opencv-python-headless \
    numpy

chown -R "$RUN_USER:$RUN_USER" "$VENV_DIR"
ok "Virtualenv prêt."

# ── Fichier d'environnement ───────────────────────────────────────────────────
ENV_FILE="/etc/exoria/spool.env"
info "Écriture du fichier d'environnement $ENV_FILE..."
mkdir -p /etc/exoria
chmod 750 /etc/exoria

# Les credentials sont lus depuis ce fichier — NE PAS versionner ce fichier
cat > "$ENV_FILE" <<'EOF'
# Credentials NAS (SFTP)
NAS_HOST=192.168.88.82
NAS_PORT=22
NAS_USER=sftpuser
NAS_PASS=Exori@2026!

# Kafka
KAFKA_BROKER=192.168.88.4
KAFKA_BROKER_PORT=9092
KAFKA_TOPIC=monitoring
EOF

chown "root:$RUN_USER" "$ENV_FILE"
chmod 640 "$ENV_FILE"
ok "Fichier d'environnement écrit ($ENV_FILE)."

# ── Unit systemd ──────────────────────────────────────────────────────────────
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
info "Écriture du unit systemd $UNIT_FILE..."

cat > "$UNIT_FILE" <<EOF
[Unit]
Description=Exoria Spool — ingestion et transfert des sessions robotiques vers NAS
Documentation=https://github.com/exoria/spool
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_USER}
WorkingDirectory=${INSTALL_DIR}

# Credentials (jamais dans le code source)
EnvironmentFile=${ENV_FILE}

# Lance spool.py en continu
ExecStart=${VENV_DIR}/bin/python3 -u ${SCRIPT_DEST}

# Redémarrage automatique en cas de crash — toujours
Restart=always
RestartSec=5
StartLimitIntervalSec=0

# Logs → journald (journalctl -u ${SERVICE_NAME} -f)
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Sécurité minimale
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${INBOX_DIR} ${SPOOL_DIR} ${QUARANTINE_DIR} ${LOG_DIR} /var/log/exoria ${DB_DIR} /tmp
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

sleep 3

STATUS=$(systemctl is-active "${SERVICE_NAME}.service" || true)
if [[ "$STATUS" == "active" ]]; then
    ok "Service '${SERVICE_NAME}' actif et en cours d'exécution."
else
    warn "Le service n'est pas encore actif (status=$STATUS)."
    warn "Vérifie : journalctl -u ${SERVICE_NAME} -n 50"
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "  Installation terminée"
echo "════════════════════════════════════════════════════════"
echo "  Script    : $SCRIPT_DEST"
echo "  Quality   : $QUALITY_DEST"
echo "  Venv      : $VENV_DIR"
echo "  Env file  : $ENV_FILE"
echo "  Unit      : $UNIT_FILE"
echo "  Logs      : /srv/exoria/logs/spool.log  +  journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "  Commandes utiles :"
echo "    systemctl status  ${SERVICE_NAME}"
echo "    systemctl restart ${SERVICE_NAME}"
echo "    systemctl stop    ${SERVICE_NAME}"
echo "    journalctl -u ${SERVICE_NAME} -f"
echo "════════════════════════════════════════════════════════"
