# RPC types

## Unary

One request, one reply.

**Client**

1. Generate unique `correlationId`.
2. Register pending waiter keyed by `correlationId`.
3. Publish to request topic with headers (see [headers.md](headers.md)).
4. Wait for reply on reply topic with matching `correlationId`.
5. Deserialize protobuf response from record value.

**Server**

1. Consume request topic.
2. Dispatch handler by `kafka-rpc-method`.
3. Serialize response protobuf to value.
4. Publish to `kafka-rpc-reply-topic` from request header.

**Timeout**: default 30_000 ms (client-side).

**Errors**: server sets `kafka-rpc-error` header; value may still be empty or absent.

**Contract violation**: unary handler returns no response while `kafka-rpc-reply-topic` is present → log error; do not mask.

## Oneway

Fire-and-forget RPC.

**Detection**: codegen only — method `returns google.protobuf.Empty`.

**Client**

- Sends request (method + correlation-id; reply-topic optional).
- Does **not** wait for a reply.

**Server**

- May process request; may return `null`/no reply from handler.
- Sending a reply when client did not expect one is wasteful but not a wire error.

Do not confuse oneway with unary that returns an empty protobuf message body.

## Server-streaming

One request, many reply chunks, then stream end.

**Detection**: `server_streaming = true` in `.proto` (codegen).

**Client** sends stream start (see [streaming.md](streaming.md)).

**Server** uses `StreamSink`-style API:

- `send(chunk)` for each message
- `end()` for final chunk with `kafka-rpc-stream-end`
- `cancel()` / idle timeout on server

Two modes:

| Mode | Method name | Record key | Order |
|------|-------------|------------|-------|
| Ordered (default) | Normal name | `correlationId` | Preserved per partition |
| Scalable | Prefix `Scalable` | `null` | Not guaranteed |

## Not supported

- Client-streaming
- Bidirectional streaming

Adding these requires a new wire version (MAJOR bump) and RFC.

## Codegen mapping (Java reference)

| Proto | Client stub | Server base |
|-------|-------------|-------------|
| Unary | `TResp method(TReq)` + async | `TResp method(TReq)` |
| Oneway (`Empty`) | `void method(TReq)` | `void method(TReq)` |
| Server-streaming | `StreamingCall method(TReq, Processor)` | `void method(TReq, StreamSink)` |

Go codegen (`protoc-gen-kafka-rpc-go`) should mirror this mapping in phase P5.
