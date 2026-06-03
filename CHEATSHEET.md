# FastAPI / API Integration — Interview Cheat Sheet
**Muhamed → Learnosity AI Labs (Senior/Full-Stack). Focus: FastAPI, API concepts, API integration.**
Scale anchor: *40M learners, 19B questions/year* — bring tradeoffs back to "at that scale…".

---

## 1. `async def` vs `def` in FastAPI
- FastAPI runs on **ASGI (Starlette/uvicorn)**, single-threaded event loop per worker.
- `async def` path op → runs **on the event loop**. Use for **I/O-bound** work (DB, HTTP, cache) with `await` and async libs (`httpx`, `asyncpg`).
- `def` (sync) path op → FastAPI runs it in a **threadpool** so it doesn't block the loop. Use when you only have **blocking/sync libs**.
- **Cardinal sin:** calling a blocking call (`requests`, `time.sleep`, heavy CPU) inside `async def` → blocks the whole loop, kills throughput.
- **CPU-bound** work → offload to a process pool / task queue (Celery, RQ, arq), not the event loop.
- Soundbite: *"async gives you concurrency, not parallelism — it shines for high-I/O fan-out, which is most API work."*

## 2. Pydantic v2
- v2 core is **Rust (`pydantic-core`)** → ~5–50× faster validation; matters at billions of payloads.
- Key API: `model_validate()`, `model_dump()`, `model_dump_json()`; `from_attributes=True` (was `orm_mode`) to build from ORM objects.
- Validators: `@field_validator`, `@model_validator`; `Field(...)` for constraints/aliases; `computed_field`.
- FastAPI uses Pydantic models for **request body validation + response serialization + auto OpenAPI docs** — one source of truth.
- `response_model=` enforces output shape and **strips fields not in the schema** (avoid leaking internal/sensitive fields).

## 3. Dependency Injection (`Depends`)
- `Depends(fn)` → FastAPI resolves and injects; great for auth, DB sessions, pagination params, feature flags.
- **`yield` dependencies** = setup/teardown (open DB session → `yield` → close), even on exceptions.
- **Sub-dependencies** compose; results are **cached within a single request** (same dependency resolved once).
- Testability win: `app.dependency_overrides[get_db] = fake_db` → swap real deps in tests with zero mocking hacks.

## 4. Pagination at scale
- **Offset/limit** (`OFFSET 1000000`) is O(n) — DB still scans skipped rows → death at billions of rows.
- Prefer **cursor / keyset pagination**: `WHERE id > :last_seen ORDER BY id LIMIT n` — uses the index, constant time, stable under inserts.
- Return an opaque `next_cursor` token; never expose raw offsets. Good for infinite scroll / API consumers.

## 5. Rate limiting
- Algorithms: **token bucket** (allows bursts) / leaky bucket / sliding window.
- Enforce at the **edge/API gateway** first (cheap rejection), app-level for fine-grained per-key limits.
- **Distributed** limiter needs shared state → **Redis** (atomic `INCR`+TTL or Lua sliding window); in-process counters break across workers/pods.
- Return `429` + `Retry-After` and `X-RateLimit-*` headers.

## 6. Idempotency
- **GET/PUT/DELETE** idempotent by definition; **POST** is not.
- For safe retries on POST (payments, "create assessment"): client sends an **`Idempotency-Key`** header; server stores key→result, replays the same response on retry within a TTL. Prevents double-creates from network retries.

## 7. Auth & API security
- FastAPI security utils: `OAuth2PasswordBearer`, `Security()`, scopes.
- **JWT**: stateless, signed (verify signature + `exp`/`aud`/`iss`); short-lived access + refresh token. Don't put secrets in the payload (it's only base64, not encrypted).
- **Service-to-service**: API keys, mTLS, or signed requests.
- **Learnosity-specific (good to name-drop):** their APIs authenticate with a **signed security packet — HMAC-SHA256 of the request using a consumer key/secret**, with a timestamp to prevent replay. Same pattern as webhook signature verification.
- Always: validate input (Pydantic), least-privilege scopes, CORS allowlist, secrets in env/secret manager, mind the **OWASP API Top 10** (BOLA/broken object-level auth is #1).

## 8. API integration (calling other services)
- Use **`httpx.AsyncClient`** (async, HTTP/2, connection pooling) — reuse one client, don't create per request.
- **Resilience:** sane **timeouts** (always set them), **retries with exponential backoff + jitter** on idempotent calls only, **circuit breaker** to stop hammering a down dependency, bulkheads to isolate.
- **Webhooks (inbound):** verify HMAC signature, respond 2xx fast, process async (queue), expect **at-least-once** delivery → make handlers **idempotent**.
- **Style choice:** REST (ubiquitous, cacheable) vs **gRPC** (low-latency internal, schema/proto, streaming) vs **GraphQL** (client-shaped queries, avoids over/under-fetching, but caching + N+1 are harder).

## 9. Testing & TDD
- `from fastapi.testclient import TestClient` (sync) or **`httpx.AsyncClient` + ASGITransport** (async tests).
- Override deps (`dependency_overrides`) to inject test DBs/fakes; spin real Postgres via **testcontainers** for integration tests.
- Pyramid: many fast **unit** tests → fewer **integration** → few **e2e**. **Contract testing** (Pact/schema) for cross-service APIs.
- TDD loop: red → green → refactor; tests as the spec. Mention you test **behaviour, not implementation**.

## 10. Errors, versioning, observability
- **Errors:** custom exception handlers, `HTTPException`, consistent error envelope (RFC 7807 *problem+json*); 422 is FastAPI's validation default.
- **Versioning:** URL (`/v1/`) is explicit/cache-friendly; header/media-type is cleaner but harder to debug. Never break consumers — additive changes, deprecate with notice.
- **Observability:** structured JSON logs, **correlation/request IDs** propagated across services, distributed tracing (OpenTelemetry), RED metrics (Rate/Errors/Duration). Essential at 19B-questions scale.

## 11. Scaling FastAPI in prod
- Run **uvicorn workers under gunicorn** (or `--workers`), 1 loop per worker; scale horizontally behind a load balancer (k8s pods).
- **Connection pooling** to DB (don't open per request); cache hot reads (Redis); watch **N+1 queries**; paginate everything.
- Long work → **`BackgroundTasks`** for fire-and-forget light jobs; **real queue (Celery/arq + Redis/SQS)** for anything heavy or that must survive a restart.

---

### Smart questions to ask Sean
- "How is AI Labs structured vs the core assessment platform — greenfield stack or building on existing APIs?"
- "What does the API integration surface look like — are you exposing LLM features through the existing Items/Data API or net-new services?"
- "Where do you feel the most scaling pain today at 19B questions/year — read path, write path, or analytics?"
- "What does the testing/CI culture look like for the new team — TDD, contract tests, deploy cadence?"

### If asked "why Learnosity / why this role"
Mission (equitable education at massive scale) + greenfield AI Labs (startup energy, real impact) + the exact stack you love (Python/FastAPI APIs + AI/LLMs). You want to build well-architected, well-tested APIs that put AI in front of 40M learners.
