# Topology and consumer groups

## Topics

| Topic | Who configures | Purpose |
|-------|----------------|---------|
| Request | Server (per service) | Inbound RPC requests |
| Reply | **Client** (per channel/pool) | Outbound replies to that client |

Server reads only the request topic. Reply destination comes **only** from `kafka-rpc-reply-topic` header.

## Partitions

Rule: **number of partitions ≥ consumer-count** for both request and reply topics used by a channel/server.

Record key routing:

- Unary / ordered stream: `key = correlationId` → partition = `hash(correlationId) % numPartitions`
- Scalable stream chunks: `key = null` → any partition

## Client channel (reply consumer)

One logical channel = one producer + N consumer threads in **one** consumer group (per channel instance).

### Reply consumer startup

Reply consumer starts at **channel build**, not at first `request()`:

1. Subscribe to reply topic
2. `seekToEnd` (only messages after channel is ready)
3. Poll loop runs in background
4. `request()` only registers pending + sends to request topic

See Java: [pooled-kafka-rpc-request-timeline](https://github.com/tormoz70/bio4j-kafka-rpc/blob/master/docs/pooled-kafka-rpc-request-timeline.md).

### Consumer group mutation

Default client behavior: append `-inst-{uuid}` to `group.id` on each channel start:

```
{configured-group-id}-inst-{UUID}
```

Reasons:

- Fast channel replacement without waiting for old member session timeout
- Reply consumer does not need historical offsets (`seekToEnd`)
- Isolation from stale consumers

Server (`KafkaRpcServer`) uses a **stable** group id (no uuid suffix) — offsets and horizontal scaling matter on the request topic.

## Multiple channels on one reply topic

If several channels (different `group.id`) subscribe to the **same** reply topic:

- Each group reads **all** partitions
- Only the channel with `pending[correlationId]` handles the reply
- Others log **ORPHAN_REPLY** — functional but wasteful

**Recommendation**: one pooled channel per reply topic, or dedicated reply topic per logical client.

```
reply-topic (10 partitions)
    ├─ group channel-1-inst-uuid1  → consumer threads
    ├─ group channel-2-inst-uuid2  → ...
    └─ ...
```

## Server scaling

Multiple server instances share the request topic consumer group. Each partition is handled by at most one consumer in the group at a time.

## Go implementations

Go channel/server must follow the same topology invariants for Java ↔ Go interop on a shared Kafka cluster.

## Java-specific configuration

Spring YAML, `consumer-count`, pool eviction: [application-yml-configuration.md](https://github.com/tormoz70/bio4j-kafka-rpc/blob/master/docs/application-yml-configuration.md) and [client-channel-topology-and-consumer-groups.md](https://github.com/tormoz70/bio4j-kafka-rpc/blob/master/docs/client-channel-topology-and-consumer-groups.md).
