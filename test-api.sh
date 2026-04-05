#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# Payment Workflow Service — Sample API Test Calls
# Run with: chmod +x test-api.sh && ./test-api.sh
# ─────────────────────────────────────────────────────────────────

BASE_URL="http://localhost:8080/api/payments"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Payment Workflow Service — API Tests"
echo "═══════════════════════════════════════════════════════════"

# ── Test 1: Successful WIRE payment ─────────────────────────────
echo ""
echo "▶ Test 1: Successful WIRE payment (USD, cross-border)"
curl -s -X POST "$BASE_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-1001",
    "destinationAccountId": "ACC-2002",
    "amount": 1500.00,
    "currency": "USD",
    "paymentType": "WIRE",
    "reference": "INV-2024-001",
    "description": "Invoice payment to supplier"
  }' | python3 -m json.tool 2>/dev/null || echo "(Install python3 for pretty-print, or remove '| python3 ...')"

echo ""
echo "─────────────────────────────────────────────────────────────"

# ── Test 2: Internal SGD transfer ────────────────────────────────
echo ""
echo "▶ Test 2: Internal SGD transfer"
curl -s -X POST "$BASE_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-3001",
    "destinationAccountId": "ACC-3002",
    "amount": 250.75,
    "currency": "SGD",
    "paymentType": "INTERNAL",
    "description": "Internal account transfer"
  }' | python3 -m json.tool 2>/dev/null

echo ""
echo "─────────────────────────────────────────────────────────────"

# ── Test 3: Fraud blocked payment ────────────────────────────────
echo ""
echo "▶ Test 3: Fraud blocked (sanctioned source account)"
curl -s -X POST "$BASE_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-BLOCKED-001",
    "destinationAccountId": "ACC-2002",
    "amount": 100.00,
    "currency": "USD",
    "paymentType": "WIRE"
  }' | python3 -m json.tool 2>/dev/null

echo ""
echo "─────────────────────────────────────────────────────────────"

# ── Test 4: Validation failure ────────────────────────────────────
echo ""
echo "▶ Test 4: Validation failure (unsupported currency XYZ)"
curl -s -X POST "$BASE_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-1001",
    "destinationAccountId": "ACC-2002",
    "amount": 100.00,
    "currency": "XYZ",
    "paymentType": "WIRE"
  }' | python3 -m json.tool 2>/dev/null

echo ""
echo "─────────────────────────────────────────────────────────────"

# ── Test 5: Async payment ─────────────────────────────────────────
echo ""
echo "▶ Test 5: Async payment submission"
ASYNC_RESPONSE=$(curl -s -X POST "$BASE_URL/async" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "ACC-5001",
    "destinationAccountId": "ACC-5002",
    "amount": 3000.00,
    "currency": "EUR",
    "paymentType": "SEPA"
  }')
echo "$ASYNC_RESPONSE" | python3 -m json.tool 2>/dev/null

# Extract paymentId from response and query status
PAYMENT_ID=$(echo "$ASYNC_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['paymentId'])" 2>/dev/null)
if [ -n "$PAYMENT_ID" ]; then
  echo ""
  echo "  → Querying status for: $PAYMENT_ID"
  sleep 1
  curl -s "$BASE_URL/$PAYMENT_ID/status" | python3 -m json.tool 2>/dev/null
fi

echo ""
echo "─────────────────────────────────────────────────────────────"

# ── Health check ──────────────────────────────────────────────────
echo ""
echo "▶ Health Check"
curl -s "$BASE_URL/health" | python3 -m json.tool 2>/dev/null

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Done! View workflows at: http://localhost:8233"
echo "  Swagger UI at: http://localhost:8080/swagger-ui.html"
echo "═══════════════════════════════════════════════════════════"
echo ""
