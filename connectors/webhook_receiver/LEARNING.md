# Webhook Receiver — Learning Notes

## What Is a Webhook?

A webhook is the inverse of polling. Instead of us asking "any new data?", the
**external system pushes data to us the moment something happens**. Think of it like
the difference between checking your mailbox every 10 minutes vs receiving a text
message the moment something arrives.

## The Webhook Security Pattern: HMAC Signatures

The most important security concept here is the **HMAC-SHA256 signature**.

When an external system sends you a webhook, anyone on the internet can POST to your
endpoint. How do you know the request came from your trusted partner and not an attacker?

1. You and the sender share a **secret key** (never transmitted in the request itself)
2. The sender computes `HMAC-SHA256(secret, request_body)` and puts it in a header
3. You recompute the same HMAC and compare with `hmac.compare_digest()` (constant-time!)
4. If they match → request is authentic

```python
# This is the pattern used by GitHub, Stripe, Twilio, etc.
expected = "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
is_valid  = hmac.compare_digest(expected, received_signature)
```

**Why `compare_digest` instead of `==`?**  
String `==` returns early on the first mismatch, leaking timing information. An
attacker can measure how long your comparison takes to infer correct characters one
by one (timing attack). `compare_digest` always takes the same time.

## Idempotency Keys

Networks are unreliable. What happens if:
- The sender sends a webhook
- Your server processes it and produces to Kafka
- The sender never gets your 202 response (network timeout)
- The sender **retries** the webhook

Without deduplication, you'd process the same risk alert twice — possibly alerting on a
transaction that was already handled!

The `X-Idempotency-Key` header lets the sender include a unique ID per logical event.
You store seen IDs (in Redis, a database, or Kafka consumer group semantics) and
skip duplicates. This connector passes the key through to Kafka for downstream
deduplication.

## Pydantic Validation

Notice that `RiskAlertPayload` uses Pydantic validators to enforce business rules:
- `severity` must be one of the known enum values
- `risk_score` must be between 0.0 and 1.0

This means **invalid data is rejected at the HTTP boundary** before it ever touches
Kafka. This is the "validate early, fail fast" principle.

## What the 202 Accepted Status Means

We return `HTTP 202 Accepted` (not `200 OK`) because:
- `200 OK` means "I processed your request completely"
- `202 Accepted` means "I received your request and will process it asynchronously"

Since producing to Kafka is async (we enqueue and move on), `202` is semantically correct.

## In Production
Real implementations would add:
- Redis/DB-backed idempotency key store with TTL
- Rate limiting per source IP
- mTLS for service-to-service authentication
- Webhook replay mechanism for operational incidents
- Dead letter endpoint for rejected payloads

## Try It Out

```bash
# Simulate 10 risk alerts
curl -X POST http://localhost:8002/webhook/simulate-alerts?count=10

# Send a real risk alert
curl -X POST http://localhost:8002/webhook/risk-alert \
  -H "Content-Type: application/json" \
  -d '{"alert_type":"FRAUD_SUSPECTED","severity":"HIGH","account_id":"ACC-001","description":"Unusual pattern","risk_score":0.92}'

# Then check Kafka UI → raw.webhooks topic
```
