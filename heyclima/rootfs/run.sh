#!/bin/sh
# Startet den Proxy. Die Optionen server_url, mode und control schreibt Home Assistant nach /data/options.json.
set -eu
OPTIONS=/data/options.json
option() { node -e 'try{process.stdout.write(String(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"))[process.argv[2]]||""))}catch{}' "$OPTIONS" "$1"; }
CONTROL=false # nur die Option control schaltet das Steuern ein, keine Umgebungsvariable von außen
if [ -f "$OPTIONS" ]; then
  SERVER_URL="$(option server_url)"
  [ -n "$SERVER_URL" ] && export SERVER_URL
  MODE="$(option mode)"
  [ -n "$MODE" ] && export MODE
  # control: nur genau true schaltet das Steuern ein (Modus homeassistant); false, fehlend oder alles andere: nur Anzeige
  [ "$(option control)" = "true" ] && CONTROL=true
fi
export CONTROL
export SERVER_URL="${SERVER_URL:-https://app.heyclima.com}"
export MODE="${MODE:-heyclima}"
export WEB_DIR="${WEB_DIR:-/app/web}"
export PORT="${PORT:-8099}"
exec node /app/proxy.mjs
