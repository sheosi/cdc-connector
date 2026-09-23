# cdc-conector

Postgres -> Kafka CDC pipeline with schema sharing, per-transaction batching, and exactly-once semantics.

## Features

- Decode Postgres logical replication WAL.
- Share table schemas via compacted Kafka topic (`cdc.relations`)
- Route events to per-table topics (`cdc.events.{namespace}.{table}`).
- Per-Postgres-transaction Kafka transactions.
- LSN checkpoint topic for recovery.
- Postgres and Feldera sinks.
- Zero-copy SIMD-optimized tuple parsing (~135ns per insert).
- Exactly once messages (more below).

## Architecture

```
Postgres WAL
|
cdc-wal-reader (lib) + cdc-producer (bin)
|                         ↑
|                     cdc.relations (compacted)
|                     cdc.lsn (compacted)
|                     cdc.events.{namespace}.{table}
|
Kafka
|
|
cdc-sink (lib) + postgres-connector (bin) / cdc-sink (lib) + feldera-connector (bin)
```

## Quick start

```bash
docker compose up -d
cargo run --bin cdc-producer
cargo run --bin postgres-connector
```

## Configuration

| Var | Default | Description |
|---|---|---|
| `POSTGRES_URL` | `postgres://postgres:postgres@localhost:5432/postgres` | Source DB |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka brokers |
| `TOPIC_PREFIX` | `cdc` | Topic prefix |
| `PUBLICATION` | `cdc_pub` | Postgres publication name


## Exactly-once

* Producer: Kafka transactions + idempotent producer.
* Consumer: read_committed + idempotent sink writes.
* LSN checkpoint commited after successful Kafka transaction.

## MSRV (Minimum Supported Rust Version)

The MSRV is 1.93.1 due to our use of the feldera apis that require that version.


## Benchmarks.

| Operation | Latency |
|---|---|
| Insert tuple parse | ~135 ns |

## License


Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
