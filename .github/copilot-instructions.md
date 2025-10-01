# Copilot Instructions: slask-tracking

Concise guide for AI agents contributing to this repository. Focus on existing patterns; avoid inventing new architectures.

## 1. Architecture & Data Flow
- Entry point: `main.go` starts HTTP server (port 8080) and RabbitMQ consumer. Requires `RABBIT_URL`.
- Events arrive two ways: HTTP `/track/*` endpoints (`tracking-server.go`) and RabbitMQ topic `global:tracking` (`pkg/events/rabbit-tracking.go`).
- Core in-memory state: `PersistentMemoryTrackingHandler` in `pkg/view/tracking-handler.go` implementing `TrackingHandler` interface.
- Optional persistence/analytics: `ClickhouseTrackingHandler` (`pkg/view/clickhouse_handler.go`) attached as a follower via `AttachFollower` to mirror events into ClickHouse.
- Periodic background save of in-memory state to `data/tracking.json` (every minute if changes) using JSON encoding.
- Prometheus metrics exposed at `/metrics` (counters: processed events, sessions).

## 2. Event Model
Defined in `pkg/view/types.go`:
- Event type constants (uint16) are authoritative (e.g. 0=session start, 1=search, 2=click, 5=impression, 11+=cart events).
- Structs embed `*BaseEvent` plus optional `*BaseItem` or payload fields.
- Session & item level personalization built from decay-scored events (see Decay* types in `decay.go`).
- Handlers fan out: memory handler updates aggregates + personalization groups; followers (ClickHouse) receive identical calls.

## 3. Scoring & Decay
- Popularity and personalization rely on `DecayList` / `DecayPopularity` (files: `decay.go`, `decay-list_test.go`). Each event adds a `DecayEvent` with (timestamp,value). Periodic decay & cleanup executed during `save()`.
- Different event kinds assign different base values (e.g. click≈200, cart_add≈190*quantity, impression≈position, search filters boost fields).

## 4. Concurrency Pattern
- HTTP handlers spawn goroutines (`go trk.HandleEvent(...)`) to avoid blocking requests.
- Follower dispatch uses `dispatchFollowers` copying slice under read lock, then async goroutines with panic recovery.
- Avoid long blocking operations inside handler methods—follow existing lightweight mutation pattern and rely on background save.

## 5. Persistence & Sessions
- Sessions stored in-memory: map[int64]*SessionData. Updated via `updateSession()` which also enriches SessionContent from request headers.
- File persistence: `tracking-handler.go` `save()` writes entire struct; keep additions JSON-tagged for backward compatibility.
- When adding fields to persistent structs, initialize nil maps/slices in loader to avoid nil deref (pattern shown for `ViewedTogether`, `AlsoBought`).

## 6. ClickHouse Integration
- Config populated from env (`CLICKHOUSE_ADDR(ES)`, `CLICKHOUSE_DATABASE`, table names, timeouts). See `loadClickhouseConfigFromEnv()`.
- `ClickhouseTrackingHandler` transforms each event into row(s) and batch inserts (`insertEventRows`). Match column types: session_id Int64, event_type UInt16, item_id UInt32.
- When creating materialized views use consistent state types (e.g. `AggregateFunction(uniq, Int64)` for session_id) to avoid conversion errors.

## 7. Adding New Event Types
1. Add new constant in `types.go` (pick unused uint16).
2. Add name mapping in `clickhouse_handler.go` (`clickhouseEventTypeNames`).
3. Extend switch in `rabbit-tracking.go` to unmarshal & route.
4. Implement new handler method on `TrackingHandler` if fundamentally new; else reuse existing (e.g. treat like `Event` or `CartEvent`).
5. Update session scoring logic in `SessionData.HandleEvent` with appropriate decay value.

## 8. Extending Followers / Analytics
- Implement `TrackingHandler` subset you need (no-op for unused methods) and attach via `AttachFollower`.
- Keep follower lightweight; do not mutate shared memory of primary handler.

## 9. Build & Run
- Local: `go build -o slask-tracker` then run with required env `RABBIT_URL`. Optional ClickHouse envs enable logging.
- Docker: multi-stage build defined in `Dockerfile` producing static binary (`CGO_ENABLED=0`). Container listens on 8080.

## 10. Testing & Validation
- Existing tests focus on decay logic (`decay-list_test.go`) and handler behavior (`tracking-handler_test.go`). Mirror test patterns when adding time-dependent scoring—inject timestamps or control clock if necessary.

## 11. Conventions & Style
- Prefer pointer embedding (`*BaseEvent`) for shared fields.
- Use lower-case JSON field names aligned with existing schema.
- Avoid premature abstractions; follow minimal incremental additions.
- Use `log.Printf` for diagnostics; metrics for high-volume counts.

## 12. Common Pitfalls
- Mixing finalized values with AggregateFunction states in ClickHouse MVs; choose one pattern and query accordingly.
- Forgetting to set timestamp on events ingested from RabbitMQ (`SetTimestamp()` already called—maintain pattern when adding new message types).
- Nil maps after adding new persisted fields—initialize in load path.

## 13. Example: Adding a "wishlist_add" Event
- Add `WISHLIST_ADD = uint16(16)` in `types.go`.
- Map name in `clickhouseEventTypeNames`.
- Update `rabbit-tracking.go` case 16 unmarshal to a struct (reuse `CartEvent` or define new `WishlistEvent`).
- In `SessionData.HandleEvent`, add scoring: `Value: 300` maybe.
- Adjust ClickHouse MV / views if you need funnel metrics that include wishlist stage.

Keep instructions synced with real code changes. Keep it short and concrete.
