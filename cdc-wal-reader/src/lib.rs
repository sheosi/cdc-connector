use std::collections::HashMap;

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::{Relation, arena::ChangeEvent};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;
use serde::Deserialize;
use thiserror::Error;
use tokio_postgres::NoTls;
use tracing::{debug, error, warn};

use crate::decoder::{DecoderError, relation::RelationData};

// This has to be public for the benches to make use of it
pub mod decoder;

async fn send_to_producer<'a, P>(
    event_res: Result<(ChangeEvent<'a>, &RelationData), DecoderError>,
    producer: &P,
    currently_in_transaction: &mut bool,
) where
    P: Producer,
{
    use cdc_avro::arena::Op;
    match event_res {
        Ok((event, relation)) => {
            let kind = match &event.op {
                Op::Insert { row: _ } => "insert",
                Op::Update { old: _, row: _ } => "update",
                Op::Delete { old: _ } => "delete",
            };

            metrics::counter!("cdc_events_produced_total", "op"=> kind, "table" =>relation.inner.name.to_string())
                .increment(1);

            if let Err(e) = producer.send(&relation.inner, event).await {
                error!(error = e, "Kafka error");

                metrics::counter!("cdc_produce_errors_total", "stage" => "send").increment(1);

                if let Err(e) = producer.abort_transaction().await {
                    error!(error = ?e, "Failed to abort transaction");
                }

                *currently_in_transaction = false;
            }
        }
        Err(e) => {
            error!(error = e.to_string(), "Failed to decode input ");
        }
    }
}

#[derive(Deserialize)]
pub struct PostgresConfig {
    host: String,
    user: String,
    password: String,
    slot_name: String,
    dbname: String,

    #[serde(default = "default_port")]
    port: u16,

    #[serde(default = "default_publication")]
    publication: String,
}

fn default_port() -> u16 {
    5432
}

fn default_publication() -> String {
    "cdc_pub".to_string()
}

pub async fn start_wal_input<P: Producer>(
    own_config: PostgresConfig,
    last_lsn: u64,
    replica_identity_full: bool,
    mut producer: P,
) -> Result<(), StartWalInputError> {
    let pg_config = ReplicationConfig::new(
        own_config.host,
        own_config.user,
        own_config.password,  // host, user, password
        own_config.dbname,    // dbname
        own_config.slot_name, // slot name
        own_config.publication,
    )
    .with_start_lsn(Lsn(last_lsn))
    .with_port(own_config.port);

    configure_replica_identity(&pg_config, replica_identity_full).await?;
    let mut client = ReplicationClient::connect(pg_config).await?;

    let arena = Bump::with_capacity(1024);
    let mut relation_map = HashMap::<u32, RelationData, RandomState>::default();
    let mut currently_in_transaction = false;

    while let Some(ev) = client.recv().await? {
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                if data.len() == 0 {
                    debug!("Got empty data")
                }

                match data[0] {
                    b'R' => match RelationData::parse(data) {
                        Ok(relation) => {
                            println!("{:?}", &relation);

                            if let Err(e) = producer.on_relation(&relation.inner).await {
                                error!(error = e, "Failed to send relation");
                            }

                            relation_map.insert(relation.inner.relation_oid, relation);
                        }
                        Err(e) => {
                            error!(error = e.to_string(), "Failed to parse Relation")
                        }
                    },
                    b'I' => {
                        // If not in a transaction because it was aborted, skip treating this
                        if currently_in_transaction {
                            let insert = decoder::insert::parse(&data, &relation_map, &arena);

                            println!("Got insert: {:?}", insert);

                            send_to_producer(insert, &producer, &mut currently_in_transaction)
                                .await;
                        }
                    }
                    b'D' => {
                        println!("Remove bytes={:?}", &data);

                        // If not in a transaction because it was aborted, skip treating this
                        if currently_in_transaction {
                            send_to_producer(
                                decoder::delete::parse(&data, &relation_map, &arena),
                                &producer,
                                &mut currently_in_transaction,
                            )
                            .await;
                        }
                    }
                    b'U' => {
                        println!("Delete bytes={:?}", &data);

                        // If not in a transaction because it was aborted, skip treating this
                        if currently_in_transaction {
                            send_to_producer(
                                decoder::update::parse(&data, &relation_map, &arena),
                                &producer,
                                &mut currently_in_transaction,
                            )
                            .await;
                        }
                    }
                    _ => {
                        println!("XLogData wal_end={} bytes={:?}", wal_end, data);
                    }
                }
            }
            ReplicationEvent::Begin {
                final_lsn: _,
                xid: _,
                commit_time_micros: _,
            } => {
                if currently_in_transaction {
                    warn!(
                        "Already in a transaction but asked for a new one, let's abort the old one"
                    );
                    if let Err(e) = producer.abort_transaction().await {
                        error!(error = ?e, "Failed to abort transaction");
                    }
                } else {
                    currently_in_transaction = true;
                }

                if let Err(e) = producer.start_transaction().await {
                    error!(error = ?e, "Failed to start transaction");
                }
            }
            ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros: _,
            } => {
                // Don't commit if we already aborted
                if currently_in_transaction {
                    currently_in_transaction = false;
                    if let Err(e) = producer.commit_transaction(end_lsn.0).await {
                        error!(error = ?e, "Failed to commit transaction");

                        metrics::counter!("cdc_produce_errors_total", "stage" => "commit")
                            .increment(1);
                    } else {
                        client.update_applied_lsn(lsn);

                        metrics::gauge!("cdc_lsn_committed").set(lsn.0 as f64);
                    }
                }
            }
            ReplicationEvent::KeepAlive { .. } => {
                // heartbeat; crate handles reply
            }
            ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
            } => {
                println!(
                    "Got message: {} {} {} {:?}",
                    transactional, lsn, prefix, content
                );
            }
            ev => println!("other: {:?}", ev),
        }
    }

    // The connection was closed
    if currently_in_transaction {
        if let Err(e) = producer.abort_transaction().await {
            error!(error = ?e, "Failed to abort final transaction");
        }
    }

    Ok(())
}

async fn configure_replica_identity(
    config: &ReplicationConfig,
    replica_identity_full: bool,
) -> Result<(), StartWalInputError> {
    let pg_config = tokio_postgres::Config::new()
        .host(&config.host)
        .port(config.port)
        .user(&config.user)
        .password(&config.password)
        .dbname(&config.database)
        .to_owned();

    let (clt, connection) = pg_config.connect(NoTls).await?;
    tokio::spawn(connection);

    let pub_names: Vec<String> = config.publication.names().to_vec();

    let identity = if replica_identity_full {
        "FULL"
    } else {
        "DEFAULT"
    };

    let rows = clt
        .query(
            "SELECT schemaname, tablename
               FROM pg_publication_tables
               WHERE pubname = ANY($1)",
            &[&pub_names],
        )
        .await?;

    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let fullname = format!("{}.{}", schema, table);

        clt.execute(
            &format!("ALTER TABLE {} REPLICA IDENTITY {}", fullname, identity),
            &[],
        )
        .await?;
    }

    Ok(())
}

pub trait Producer: Send {
    fn start_transaction(&self) -> impl std::future::Future<Output = Result<(), String>>;
    fn send<'a>(
        &self,
        relation: &Relation,
        event: ChangeEvent<'a>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    fn on_relation(
        &mut self,
        relation: &Relation,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    fn commit_transaction(&self, lsn: u64)
    -> impl std::future::Future<Output = Result<(), String>>;

    fn abort_transaction(&self) -> impl std::future::Future<Output = Result<(), String>>;
}

#[derive(Debug, Error)]
pub enum StartWalInputError {
    #[error("While setting identity {0}")]
    PostgresError(#[from] tokio_postgres::Error),

    #[error("From WAL operations {0}")]
    WalError(#[from] pgwire_replication::PgWireError),
}
