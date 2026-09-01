# Kafka RPC — cross-language documentation

Language-agnostic **wire protocol** and interoperability docs for [Kafka RPC](https://github.com/tormoz70/bio4j-kafka-rpc).

| Repository | Role |
|------------|------|
| [bio4j-kafka-rpc](https://github.com/tormoz70/bio4j-kafka-rpc) | Java 21+ runtime, protoc plugin, Spring Boot starter |
| [bio4j-kafka-rpc-go](https://github.com/tormoz70/bio4j-kafka-rpc-go) | Go runtime (wire-compatible peer) |
| [bio4j-kafka-rpc-example](https://github.com/tormoz70/bio4j-kafka-rpc-example) | End-to-end Java example application |
| **bio4j-kafka-rpc-docs** (this repo) | Wire contract, topology, Go port plan, interop checklists |

Java-specific configuration (Spring YAML, Gradle) stays in [bio4j-kafka-rpc/docs](https://github.com/tormoz70/bio4j-kafka-rpc/tree/master/docs).

## Wire version

Current wire protocol version: **1.7** (see [`VERSION`](VERSION)). It tracks the Java library release that defines the canonical implementation.

Breaking changes to headers, method wire format, or RPC semantics require:

1. MAJOR version bump in `VERSION`
2. Updates under [`wire/`](wire/)
3. Coordinated releases in Java and Go runtimes

## Contents

### Wire protocol

| Document | Description |
|----------|-------------|
| [wire/overview.md](wire/overview.md) | Request/reply model, delivery guarantees, scope |
| [wire/headers.md](wire/headers.md) | Kafka record headers (names, direction, encoding) |
| [wire/rpc-types.md](wire/rpc-types.md) | Unary, oneway, server-streaming |
| [wire/streaming.md](wire/streaming.md) | Ordered vs scalable streams, healthcheck, idle timeout |
| [wire/topology.md](wire/topology.md) | Topics, partitions, consumer groups, reply routing |

### Plans and interop

| Document | Description |
|----------|-------------|
| [plans/go-port.md](plans/go-port.md) | Go implementation roadmap (P0–P5) |
| [plans/interop-checklist.md](plans/interop-checklist.md) | Java ↔ Go acceptance scenarios |

## License

Apache License 2.0 — see [LICENSE](LICENSE).
