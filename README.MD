# cdc-conector

Postgres -> Kafka CDC pipeline with schema sharing, per-transaction batching, and exactly-once semantics.

**NOTE: This is still in heavy development**

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
```

* Note: This compose inits everything: Postgres source db, Postgres sink db, redpanda, grafana, cdc-producer, postgres-sink. *

## Configuration

### cdc-producer

Also, checkout `config/feldera-connector.toml`

| Var | Default | Description |
|-----|---------|-------------|
| `CDC_PROD_WILL_CONNECT_TO_FELDERA` | `false` | Whether the `feldera-connector` is going to be used |
| `CDC_PROD_POSTGRES_HOST` | None | source DB host|
| `CDC_PROD_POSTGRES_USER` | None | Source DB user |
| `CDC_PROD_POSTGRES_PASSWORD` | None | Source DB password |
| `CDC_PROD_POSTGRES_SLOT_NAME` | None | Source DB slot name |
| `CDC_PROD_POSTGRES_DBNAME` | None | Source DB name to connect to |
| `CDC_PROD_POSTGRES_PUBLICATION` | None | Source DB publication name |
| `CDC_PROD_POSTGRES_PORT` | `5432` | Source DB port |
| `CDC_PROD_KAFKA_BROKERS` | None | Kafka brokers host and port |
| `CDC_PROD_KAFKA_TOPIC` | `cdc` | Parent topic where to put all the events, relations and lsn |
| `CDC_PROD_KAFKA_KEY` | None | The key to use in kafka |

### postgres-connector

Also, checkout `config/postgres-connector.toml`

| Var | Default | Description |
|-----|---------|-------------|
| `PG_CONN_POSTGRES_HOST` | `127.0.0.1` | Sink DB host |
| `PG_CONN_POSTGRES_USER` | `cdc` | Sink DB user |
| `PG_CONN_POSTGRES_PASSWORD` | `cdc` | Sink DB password |
| `PG_CONN_POSTGRES_PORT` | `5401` | Sink DB port |
| `PG_CONN_KAFKA_BROKERS` | None | Kafka brokers host and port |
| `PG_CONN_KAFKA_TOPIC` | `cdc` | Parent topic to read all the events, relations and lsn from |
| `PG_CONN_KAFKA_GROUP_ID` | `default` | The Kafka group id this pertains |

## feldera-connector

Also, checkout `config/feldera-connector.toml`

| Var | Default | Description |
|-----|---------|-------------|
| FELD_CONN_FELDERA_URL | `` | Feldera URL to connect to |
| FELD_CONN_FELDERA_PIPELINE | `` | Feldera pipeline |
| FELD_CONN_KAFKA_BROKERS | None | Kafka brokers to host and port |
| FELD_CONN_KAFKA_TOPIC | `cdc` | Parent topic to read all the events, relations and lsn from |
| FELD_CONN_KAFKA_GROUP_ID | `default` | The Kafka group id this pertains |

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
| Relation tuple parse | ~400 ns |

Events are really fast to parse, we make use of every possible trick: Simd, Zero-copy and Arenas.

Relations on the other hand we want them to persist in memory, so we can't make use of zero-copy nor arenas (given that not only they have to persist, with time some of them will be removed, something very complicated in an arena).


## Known limitations

This is a Poc project, rather than a full one. So it's scope it's reduced:

* Postgres data types: Right now only int4 and text are supported.
* Postgres WAL messages: insert, delete, update, Transaction begin/end are the ones supported. logical decoding, origin, types, truncate, stream start/stop/commit/abort, begin prepare, preare, prepare, rollback prepare and stream prepare messages aren't supported.

## License


Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
