# Java ↔ Go interoperability checklist

Use this checklist before declaring a Go phase complete. All scenarios assume wire **1.7** and shared Kafka topics.

## Prerequisites

- [ ] Same `.proto` definitions (field numbers and types identical)
- [ ] Request/reply topic names aligned with Java service config
- [ ] Broker `message.max.bytes` / fetch limits ≥ 10 MiB if testing large payloads
- [ ] Partitions ≥ consumer-count on both topics

## P0 — Unary

| # | Scenario | Direction | Pass |
|---|----------|-----------|------|
| 1 | `Greeter/GetGreeting` happy path | Java server → Go client | |
| 2 | `Greeter/GetGreeting` happy path | Go server → Java client | |
| 3 | Unknown method on server | Go/Java server ignores + warns | |
| 4 | Server error | `kafka-rpc-error` header, Go/Java client surfaces error | |
| 5 | Client timeout | No reply within 30s → timeout on caller | |
| 6 | Golden headers | Go record → Java `getHeader`; Java record → Go parser | |

## P1 — Oneway

| # | Scenario | Direction | Pass |
|---|----------|-----------|------|
| 7 | `returns Empty` method | Java server, Go `Send()` | |
| 8 | Same | Go server, Java stub | |
| 9 | No orphan pending | Go client does not wait for reply | |

## P2 — Ordered streaming

| # | Scenario | Direction | Pass |
|---|----------|-----------|------|
| 10 | Multi-chunk stream | Java server → Go client, order preserved | |
| 11 | Multi-chunk stream | Go server → Java client | |
| 12 | `kafka-rpc-stream-end` on last chunk | Both directions | |
| 13 | Stream error mid-flight | `kafka-rpc-error` on chunk | |
| 14 | Record key = correlationId | Verified on broker/log | |

## P3 — Healthcheck / idle

| # | Scenario | Direction | Pass |
|---|----------|-----------|------|
| 15 | Healthcheck every 5s | Stream stays alive > 1 min | |
| 16 | Server idle timeout | No healthcheck → server cancels | |
| 17 | Healthcheck after stream end | `kafka-rpc-stream-not-found` | |
| 18 | Method = `{Method}/healthcheck` | Wire name exact match | |

## P4 — Scalable / limits

| # | Scenario | Direction | Pass |
|---|----------|-----------|------|
| 19 | `Scalable*` method | `key=null`, chunks received | |
| 20 | `maxConcurrentStreams` | 1025th stream rejected | |
| 21 | `streamMaxInFlight` | `Send` blocks at 64 in-flight | |

## P5 — Codegen

| # | Scenario | Pass |
|---|----------|------|
| 22 | Generated stub calls without hand-written method strings | |
| 23 | Oneway/unary/streaming from same `.proto` as Java | |

## Topology / ops

| # | Scenario | Pass |
|---|----------|------|
| 24 | One Go channel per reply topic (no ORPHAN_REPLY storm) | |
| 25 | `group.id` suffix `-inst-{uuid}` on new Go channel | |
| 26 | Reply consumer ready before first `Request` | |
| 27 | Server stable group, no uuid suffix | |

## Negative cases

| # | Scenario | Expected |
|---|----------|----------|
| N1 | Missing `kafka-rpc-method` | Request dropped / ignored |
| N2 | Missing `kafka-rpc-reply-topic` on unary | Server does not reply (or logs violation) |
| N3 | Missing `kafka-rpc-stream-server-idle-timeout-ms` on stream start | Server drops request |
| N4 | Duplicate active `correlationId` stream | Reject or cancel old — no silent overwrite |
| N5 | Reprocessed request (at-least-once) | Idempotent handler safe |

## Sign-off

| Phase | Date | Notes |
|-------|------|-------|
| P0 | 2026-09-02 | Go unit + kfake integration tests |
| P1 | 2026-09-02 | Oneway Send + server null handler |
| P2 | 2026-09-02 | Ordered stream interop tests |
| P3 | 2026-09-02 | Healthcheck not-found test |
| P4 | 2026-09-02 | Scalable stream test |
| P5 | 2026-09-02 | protoc-gen-kafka-rpc-go + Greeter example |

Cross-language Java↔Go Greeter (items 1–2) — run with [bio4j-kafka-rpc-go/example/interop](https://github.com/tormoz70/bio4j-kafka-rpc-go/tree/main/example/interop) + Java example app.
