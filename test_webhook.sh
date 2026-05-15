#!/bin/bash
# Simula un webhook de orden de Shopify → dispara toast en el dashboard
curl -s -X POST http://localhost:8000/webhooks/shopify/orders/create \
  -H "Content-Type: application/json" \
  -d '{"name":"#TEST-001","total_price":"1299","currency":"MXN"}' \
  && echo "✓ Webhook enviado — revisa el toast en el browser"
