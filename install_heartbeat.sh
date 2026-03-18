#!/usr/bin/env bash
# install_heartbeat.sh — Installe server_heartbeat.py comme service systemd
#
# Usage (sur le serveur, en root) :
#   bash install_heartbeat.sh [--interval 10] [--broker 192.168.88.4:9092] [--server-id spool-01]

set -euo pipefail

SERVICE_NAME="exoria-heartbeat"
SCRIPT_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/server_heartbeat.py"
INSTALL_DIR="/opt/exoria"
INSTALL_SCRIPT="$INSTALL_DIR/server_heartbeat.py"
LOG_DIR="/var/log/exoria"
PYTHON="$(command -v python3)"

# ── Args ──────────────────────────────────────────────────────────────────────
INTERVAL=10
BROKER="192.168.88.4:9092"
SERVER_ID="$(hostname)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval)  INTERVAL="$2";  shift 2 ;;
    --broker)    BROKER="$2";    shift 2 ;;
    --server-id) SERVER_ID="$2"; shift 2 ;;
    *) echo "Option inconnue: $1"; exit 1 ;;
  esac
done

# ── Couleurs ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RESET='\033[0m'; BOLD='\033[1m'
ok()   { echo -e "  ${GREEN}✔${RESET}  $*"; }
info() { echo -e "  ${CYAN}…${RESET}  $*"; }
warn() { echo -e "  ${YELLOW}!${RESET}  $*"; }

echo -e "\n${BOLD}Installation de $SERVICE_NAME${RESET}"
echo "  Script   : $INSTALL_SCRIPT"
echo "  Interval : ${INTERVAL}s"
echo "  Broker   : $BROKER"
echo "  Server   : $SERVER_ID"
echo ""

# ── Vérifications ─────────────────────────────────────────────────────────────
[[ "$(id -u)" -eq 0 ]] || { echo "Erreur : ce script doit être lancé en root (sudo)"; exit 1; }
[[ -f "$SCRIPT_SRC" ]] || { echo "Erreur : $SCRIPT_SRC introuvable"; exit 1; }

# ── Dépendances Python ────────────────────────────────────────────────────────
info "Vérification des dépendances Python…"
if ! "$PYTHON" -c "import kafka" 2>/dev/null; then
  info "Installation de kafka-python…"
  pip3 install --quiet kafka-python
  ok "kafka-python installé"
else
  ok "kafka-python déjà présent"
fi

# ── Répertoires ───────────────────────────────────────────────────────────────
info "Création des répertoires…"
mkdir -p "$INSTALL_DIR" "$LOG_DIR"
ok "Répertoires prêts ($INSTALL_DIR, $LOG_DIR)"

# ── Copie du script ───────────────────────────────────────────────────────────
info "Copie du script…"
cp "$SCRIPT_SRC" "$INSTALL_SCRIPT"
chmod 755 "$INSTALL_SCRIPT"
ok "Script copié → $INSTALL_SCRIPT"

# ── Fichier systemd ───────────────────────────────────────────────────────────
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
info "Création du service systemd → $UNIT_FILE"

cat > "$UNIT_FILE" <<EOF
[Unit]
Description=Exoria Server Heartbeat (Kafka monitoring)
After=network.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$PYTHON $INSTALL_SCRIPT --interval $INTERVAL --broker $BROKER --server-id $SERVER_ID
Restart=always
RestartSec=10
StandardOutput=append:$LOG_DIR/${SERVICE_NAME}.log
StandardError=append:$LOG_DIR/${SERVICE_NAME}.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

ok "Unit file écrit"

# ── Activation et démarrage ───────────────────────────────────────────────────
info "Rechargement systemd…"
systemctl daemon-reload
ok "systemd rechargé"

info "Activation au démarrage…"
systemctl enable "$SERVICE_NAME"
ok "Service activé (autostart)"

info "Démarrage du service…"
systemctl restart "$SERVICE_NAME"
sleep 2

if systemctl is-active --quiet "$SERVICE_NAME"; then
  ok "Service démarré et actif"
else
  warn "Le service ne semble pas actif — vérifier les logs :"
  echo "    journalctl -u $SERVICE_NAME -n 30"
  echo "    tail -f $LOG_DIR/${SERVICE_NAME}.log"
  exit 1
fi

# ── Résumé ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}Résumé${RESET}"
echo "  Statut    : $(systemctl is-active $SERVICE_NAME)"
echo "  Logs      : tail -f $LOG_DIR/${SERVICE_NAME}.log"
echo "  Arrêter   : systemctl stop $SERVICE_NAME"
echo "  Désinstaller : systemctl disable --now $SERVICE_NAME && rm $UNIT_FILE $INSTALL_SCRIPT"
echo ""
