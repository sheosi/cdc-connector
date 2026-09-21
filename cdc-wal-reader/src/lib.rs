use std::collections::HashMap;

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::{ChangeEvent, Relation};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;
use serde::Deserialize;
use tokio_postgres::NoTls;

use crate::decoder::{DecoderError, relation::RelationData};

// This has to be public for the benches to make use of it
pub mod decoder;

async fn send_to_producer<'a, P>(
    client: &ReplicationClient,
    event_res: Result<(ChangeEvent<'a>, &'a RelationData<'a>), DecoderError>,
    producer: &P,
    lsn: u64,
) where
    P: Producer,
{
    match event_res {
        Ok((event, relation)) => {
            if let Err(e) = producer.send(&relation.inner, event, lsn).await {
                eprintln!("Producer had an error {}", e);
            } else {
                client.update_applied_lsn(Lsn(lsn));
            }
        }
        Err(e) => {
            eprintln!("Failed to decode input {}", e);
        }
    }
}

fn might_add<'a>(
    event: Result<ChangeEvent<'a>, DecoderError>,
    buffer: &mut bumpalo::collections::Vec<'a, ChangeEvent<'a>>,
) {
    match event {
        Ok(e) => buffer.push(e),
        Err(e) => eprintln!("Failed to parse event {}", e),
    }
}

#[derive(Deserialize)]
pub struct PostgresConfig {
    host: String,
    user: String,
    password: String,
    slot_name: String,
}

pub async fn start_wal_input<P: Producer>(
    own_config: PostgresConfig,
    last_lsn: u64,
    replica_identity_full: bool,
    mut producer: P,
) -> Result<(), pgwire_replication::PgWireError> {
    let pg_config = ReplicationConfig::new(
        own_config.host,
        own_config.user,
        own_config.password,  // host, user, password
        "cdc",                // dbname
        own_config.slot_name, // slot name
        "cdc_pub",            // publication
    )
    .with_start_lsn(Lsn(last_lsn))
    .with_port(5400);

    configure_replica_identity(&pg_config, replica_identity_full)
        .await
        .unwrap();
    let mut client = ReplicationClient::connect(pg_config).await?;

    let relations_arena = Bump::with_capacity(4096);
    let data_arena = Bump::with_capacity(2048);
    let events_arena = Bump::with_capacity(1024);
    let mut relation_map = HashMap::<u32, RelationData, RandomState>::default();

    let mut txn_data = bumpalo::collections::Vec::with_capacity_in(5, &data_arena);
    let mut txn_events = bumpalo::collections::Vec::with_capacity_in(5, &events_arena);

    while let Some(ev) = client.recv().await? {
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                txn_data.push(data);
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

            ReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => {}

            ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            } => {
                for data in txn_data.into_iter() {
                    match data[0] {
                        b'R' => {
                            if let Ok(relation) =
                                decoder::relation::RelationData::parse(&data, &relations_arena)
                            {
                                println!("{:?}", &relation);

                                if let Err(e) = producer.on_relation(&relation.inner).await {
                                    eprintln!("Failed to send relation: {}", e);
                                }

                                //relation_map.insert(relation.inner.relation_oid, relation);
                            }
                        }
                        b'I' => {
                            might_add(
                                decoder::insert::parse(&data, &relation_map, &events_arena)
                                    .map(|(a, _)| a),
                                &mut txn_events,
                            );
                        }
                        b'D' => {
                            println!("Remove bytes={:?}", &data);

                            send_to_producer(
                                &client,
                                decoder::delete::parse(&data, &relation_map, &events_arena),
                                &producer,
                                0,
                            )
                            .await;
                        }
                        b'U' => {
                            println!("Delete bytes={:?}", &data);
                            send_to_producer(
                                &client,
                                decoder::update::parse(&data, &relation_map, &events_arena),
                                &producer,
                                0,
                            )
                            .await;
                        }
                        _ => {}
                    }
                }

                txn_events.clear();
                txn_data = bumpalo::collections::Vec::with_capacity_in(5, &data_arena);
            }

            ev => println!("other: {:?}", ev),
        }
    }

    Ok(())
}

async fn configure_replica_identity(
    config: &ReplicationConfig,
    replica_identity_full: bool,
) -> Result<(), ()> {
    let pg_config = tokio_postgres::Config::new()
        .host(&config.host)
        .port(config.port)
        .user(&config.user)
        .password(&config.password)
        .dbname(&config.database)
        .to_owned();

    let (clt, connection) = pg_config.connect(NoTls).await.unwrap();
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
        .await
        .unwrap();

    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let fullname = format!("{}.{}", schema, table);

        clt.execute(
            &format!("ALTER TABLE {} REPLICA IDENTITY {}", fullname, identity),
            &[],
        )
        .await
        .unwrap();
    }

    Ok(())
}

pub trait Producer: Send {
    fn send<'a>(
        &self,
        relation: &Relation<'a>,
        event: ChangeEvent<'a>,
        lsn: u64,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    fn on_relation<'a>(
        &mut self,
        relation: &Relation<'a>,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}
