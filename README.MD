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
- Rust-based integration tests.

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
| `CDC_PROD_POSTGRES_SLOTNAME` | None | Source DB slot name |
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
* LSN checkpoint committed after successful Kafka transaction.

## MSRV (Minimum Supported Rust Version)

The MSRV is 1.93.1 due to our use of the feldera apis that require that version.


## Benchmarks.

| Operation | Latency |
|---|---|
| Insert tuple parse | ~135 ns |
| Relation tuple parse | ~400 ns |

Events are really fast to parse, we make use of every possible trick: Simd, Zero-copy and Arenas.

Relations on the other hand we want them to persist in memory, so we can't make use of zero-copy nor arenas (given that not only they have to persist, with time some of them will be removed, something very complicated in an arena).

## Running tests

Tests make use of the `testcontainer` crate, which create independent containers using Docker, and thus they need a running Docker daemon.

Run on this project root:

```shell
cargo test --workspace
```

To see the logs:

```shell
cargo test --workspace -- --nocapture
```

## Known limitations

This is a PoC project, rather than a full one. So it's scope it's reduced:

* Postgres data types: Right now only int4, bigint, bool, text and timestamp are supported.
* Postgres WAL messages: insert, delete, update, Transaction begin/end are the ones supported. logical decoding, origin, types, truncate, stream start/stop/commit/abort, begin prepare, preare, prepare, rollback prepare and stream prepare messages aren't supported.
* One db per cdc-producer

### Future work

#### Delta updates

For a high-perfomance system this could be transformed into a system that reduces communication by sending deltas. While an extremely interesting idea to research this would entail tackling several problems:

* *Cached vs uncached queries*: with the number of queries being variable, we'd have to adjust which ones will be kept cached and which ones are just made dinamycally.
* *Smart cache cleaning*: for same reason as above, a system that checks both when was the last time a query was used and how probable we are to use it again would be needed as well.
* *Delta generation*: for minimum bandwith the best option would be to have some internal db replica of the data being sent. The great ammount of data would make it impossible to have everything in memory, needing something like Redis (possibly in-process).
* *Full row recovery*: A sink might have some problem where the whole row is needed, some way to obtain that row should be possible too.

## License


Licensed under either of:

- Apache License, Version 2.0 ([LICENSE.APACHE](LICENSE.APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE.MIT](LICENSE.MIT) or http://opensource.org/licenses/MIT)

at your option.
