# Wire protocol overview

Kafka RPC is a **gRPC-style RPC layer over Apache Kafka**. Services are defined in Protocol Buffers; payloads are serialized protobuf messages. Routing and correlation use **Kafka record headers**, not a custom envelope around the payload.

## Request / reply model

```
Client (Channel)                         Server
      |                                     |
      |  request topic (protobuf value)     |
      |  headers: method, correlation-id,   |
      |           reply-topic, ...          |
      |------------------------------------>|
      |                                     | dispatch by kafka-rpc-method
      |  reply topic (protobuf value)       |
      |  headers: correlation-id, ...       |
      |<------------------------------------|
```

- **Request topic**: configured per service (e.g. `greeter.request`). Server consumes it.
- **Reply topic**: set by the **client** in the `kafka-rpc-reply-topic` header. Server **never** configures a reply topic.
- **Correlation**: `kafka-rpc-correlation-id` links request and response(s).

## Record format

| Field | Type | Notes |
|-------|------|-------|
| Key | `string` or `null` | Usually `correlationId` for unary/ordered streams; `null` for scalable stream chunks |
| Value | `bytes` | Raw protobuf payload for the RPC method (no wrapper) |
| Headers | UTF-8 strings | See [headers.md](headers.md) |

## Delivery semantics

- **At-least-once** delivery. Handlers must be **idempotent** to reprocessing.
- No exactly-once business semantics.
- Offset commit strategy is implementation-specific (Java: manual commits on server poll thread when `enable.auto.commit=false`).

## Supported RPC types

| Type | Supported |
|------|-----------|
| Unary (request → single reply) | Yes |
| Oneway (`returns google.protobuf.Empty`) | Yes |
| Server-streaming (ordered) | Yes |
| Server-streaming (scalable, method name prefix `Scalable`) | Yes |
| Client-streaming | **No** |
| Bidirectional streaming | **No** |

See [rpc-types.md](rpc-types.md) and [streaming.md](streaming.md).

## Method wire name

Format: `ServiceName/MethodName`

Examples:

- `Greeter/GetGreeting`
- `Greeter/GetGreeting/healthcheck` (stream healthcheck)

Service config key (Spring/Java): proto service name in **lowercase** (e.g. `greeter`).

## Dispatch rules

- `kafka-rpc-method` header is **required** for dispatch.
- **Unknown method** → ignore record + warn log. No fallback to a single handler.
- Oneway is detected at **codegen** time: `returns google.protobuf.Empty` → client does not wait for a reply.

## Defaults (wire 1.7)

| Setting | Default |
|---------|---------|
| Unary timeout | 30_000 ms |
| Max message size | 10 MiB |
| Stream healthcheck interval | 5_000 ms |
| Stream healthcheck timeout | 15_000 ms |
| Stream server idle timeout | 20_000 ms (client must send header on stream start) |
| Stream buffer size | 1024 |
| Max concurrent streams (server) | 1024 |
| Stream max in-flight chunks | 64 |
| Consumer ready timeout (channel) | 30_000 ms |

Canonical header names and constants: Java [`KafkaRpcConstants`](https://github.com/tormoz70/bio4j-kafka-rpc/blob/master/kafka-rpc-runtime/src/main/java/ru/sbrf/uamc/kafkarpc/KafkaRpcConstants.java), Go [`kafkarpc/constants.go`](https://github.com/tormoz70/bio4j-kafka-rpc-go/blob/main/kafkarpc/constants.go).

## Related documents

- [headers.md](headers.md)
- [topology.md](topology.md)
- Java timeline: [pooled-kafka-rpc-request-timeline](https://github.com/tormoz70/bio4j-kafka-rpc/blob/master/docs/pooled-kafka-rpc-request-timeline.md)
