# Kafka record headers

All header **names** and **values** use UTF-8 encoding unless noted. Do not invent alternate header names — implementations must match these exactly for interoperability.

## Header reference

| Header | Set by | Purpose |
|--------|--------|---------|
| `kafka-rpc-correlation-id` | Client; echoed by server | Links request ↔ reply / stream chunks |
| `kafka-rpc-method` | Client (request); server (reply/chunks) | `ServiceName/MethodName` for dispatch |
| `kafka-rpc-reply-topic` | **Client only** | Topic where server sends replies |
| `kafka-rpc-error` | Server | Error message for unary or stream |
| `kafka-rpc-stream` | Client | `"true"` marks server-streaming request |
| `kafka-rpc-stream-end` | Server (last chunk) | Marks end of server stream |
| `kafka-rpc-stream-id` | Client | Stream identifier (healthcheck) |
| `kafka-rpc-stream-server-idle-timeout-ms` | Client | **Required** on stream start; server idle cancel |
| `kafka-rpc-stream-ordered` | Client | `"true"` = ordered (default); `"false"` = scalable |
| `kafka-rpc-stream-not-found` | Server | Healthcheck reply when stream is inactive |

## Per-message expectations

### Unary request (client → request topic)

Required:

- `kafka-rpc-correlation-id`
- `kafka-rpc-method`
- `kafka-rpc-reply-topic`

Record key: typically `correlationId`.

### Unary reply (server → reply topic)

Required:

- `kafka-rpc-correlation-id`
- `kafka-rpc-method`

On error: add `kafka-rpc-error` (value = error message string).

### Oneway request

Same as unary request, but client **does not wait** for a reply. `kafka-rpc-reply-topic` is optional.

Codegen rule: `returns google.protobuf.Empty` in `.proto` → oneway.

### Server-streaming request (client → request topic)

Required:

- `kafka-rpc-correlation-id`
- `kafka-rpc-method`
- `kafka-rpc-reply-topic`
- `kafka-rpc-stream` = `"true"`
- `kafka-rpc-stream-server-idle-timeout-ms` (milliseconds as decimal string)

Optional:

- `kafka-rpc-stream-ordered` (`"true"` or `"false"`; default ordered if omitted)
- `kafka-rpc-stream-id` (for healthcheck)

### Stream chunk (server → reply topic)

Required:

- `kafka-rpc-correlation-id`
- `kafka-rpc-method`

Last chunk additionally:

- `kafka-rpc-stream-end` = `"true"`

On error/cancel: `kafka-rpc-error` may be set.

### Stream healthcheck (client → request topic)

- `kafka-rpc-method` = `{originalMethod}/healthcheck` (suffix `/healthcheck`)
- `kafka-rpc-stream-id` = stream id from stream start
- `kafka-rpc-correlation-id`, `kafka-rpc-reply-topic` as usual

Server reply when stream is gone:

- `kafka-rpc-stream-not-found` = `"true"`

## Invariants

1. Server **must not** configure reply topic — only read `kafka-rpc-reply-topic` from the request.
2. Unknown `kafka-rpc-method` → **ignore** the record (warn), no handler fallback.
3. Header names are case-sensitive and fixed (see table above).

## Implementation anchors

- Java: `KafkaRpcConstants.java`
- Go: `kafkarpc/constants.go`
