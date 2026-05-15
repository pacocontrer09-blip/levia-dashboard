#!/bin/bash
# LEVIA™ Ops Dashboard — script de arranque
# Uso: ./start.sh

cd "$(dirname "$0")"

# Cargar variables de entorno
set -a; [ -f .env ] && source .env; set +a

# Verificar venv
if [ ! -d ".venv" ]; then
  echo "→ Creando entorno virtual..."
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
fi

echo ""
echo "  LEVIA™ Ops Dashboard"
echo "  ─────────────────────────────"
echo "  Local: http://localhost:8000"
echo ""

# ── Tunnel Cloudflare ──────────────────────────────────────────────────────
if command -v cloudflared &> /dev/null; then
  # Matar tunnel previo si existe
  pkill cloudflared 2>/dev/null
  sleep 1

  cloudflared tunnel --url http://localhost:8000 --no-autoupdate > /tmp/cloudflared.log 2>&1 &
  echo "  Tunnel iniciando..."
  sleep 6

  TUNNEL_URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' /tmp/cloudflared.log | head -1)

  if [ -n "$TUNNEL_URL" ]; then
    echo "  Remoto: $TUNNEL_URL"

    # ── Auto-actualizar webhooks en Shopify ──────────────────────────────
    if [ -n "$SHOPIFY_TOKEN" ] && [ -n "$SHOPIFY_STORE" ]; then
      echo "  Actualizando webhooks en Shopify..."

      SHOPIFY_API_VERSION="${SHOPIFY_API_VERSION:-2024-10}"
      SHOP_API="https://$SHOPIFY_STORE/admin/api/$SHOPIFY_API_VERSION"
      AUTH_HEADER="X-Shopify-Access-Token: $SHOPIFY_TOKEN"

      # Obtener webhooks existentes
      EXISTING=$(curl -s -H "$AUTH_HEADER" "$SHOP_API/webhooks.json")

      # Función para upsert un webhook
      upsert_webhook() {
        local TOPIC="$1"
        local ENDPOINT="$2"
        local NEW_URL="$TUNNEL_URL$ENDPOINT"

        # Buscar si ya existe un webhook para este topic
        local WH_ID=$(echo "$EXISTING" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for wh in data.get('webhooks', []):
    if wh['topic'] == '$TOPIC':
        print(wh['id'])
        break
" 2>/dev/null)

        if [ -n "$WH_ID" ]; then
          # Actualizar URL del existente
          RESULT=$(curl -s -X PUT \
            -H "$AUTH_HEADER" \
            -H "Content-Type: application/json" \
            -d "{\"webhook\":{\"address\":\"$NEW_URL\"}}" \
            "$SHOP_API/webhooks/$WH_ID.json")
          echo "  ✓ $TOPIC → actualizado"
        else
          # Crear nuevo
          RESULT=$(curl -s -X POST \
            -H "$AUTH_HEADER" \
            -H "Content-Type: application/json" \
            -d "{\"webhook\":{\"topic\":\"$TOPIC\",\"address\":\"$NEW_URL\",\"format\":\"json\"}}" \
            "$SHOP_API/webhooks.json")
          echo "  ✓ $TOPIC → creado"
        fi
      }

      upsert_webhook "orders/create"  "/webhooks/shopify/orders/create"
      upsert_webhook "orders/paid"    "/webhooks/shopify/orders/paid"
      upsert_webhook "customers/create" "/webhooks/shopify/customers/create"
      upsert_webhook "checkouts/create" "/webhooks/shopify/checkouts/create"

      echo "  Webhooks sincronizados."
    else
      echo "  ⚠  SHOPIFY_TOKEN no configurado — actualiza webhooks manualmente:"
      echo "  $TUNNEL_URL/webhooks/shopify/orders/create"
    fi
  else
    echo "  ⚠  No se pudo obtener URL del tunnel"
  fi
else
  echo "  (cloudflared no instalado — solo acceso local)"
fi

echo ""
echo "  Iniciando servidor... Ctrl+C para detener."
echo ""

# Arrancar servidor (foreground)
# Usar --reload solo en modo dev: LEVIA_ENV=dev ./start.sh
if [ "${LEVIA_ENV:-prod}" = "dev" ]; then
  .venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --reload
else
  .venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
fi
