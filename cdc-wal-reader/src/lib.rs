use std::collections::HashMap;

use cdc_avro::ChangeEvent;
use pgwire_replication::{ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;
use tokio_postgres::NoTls;

use crate::decoder::DecoderError;

// This has to be public for the benches to make use of it
pub mod decoder;

async fn send_to_producer<'a, 'b, P>(
    event_res: Result<ChangeEvent<'a, 'b>, DecoderError>,
    producer: &P,
    lsn: i32,
) where
    P: Producer,
{
    match event_res {
        Ok(event) => {
            if let Err(e) = producer.send(event, lsn).await {
                eprintln!("Producer had an error {}", e);
            }
        }
        Err(e) => {
            eprintln!("Failed to decode input {}", e);
        }
    }
}

pub async fn start_wal_input<P: Producer>(
    config: ReplicationConfig,
    replica_identity_full: bool,
    producer: P,
) -> Result<(), pgwire_replication::PgWireError> {
    configure_replica_identity(&config, replica_identity_full)
        .await
        .unwrap();
    let mut client = ReplicationClient::connect(config).await?;
    let mut relation_map = HashMap::new();

    while let Some(ev) = client.recv().await? {
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => match data[0] {
                b'R' => {
                    if let Ok(relation) = decoder::relation::Relation::parse(data) {
                        println!("{:?}", &relation);
                        relation_map.insert(relation.relation_oid, relation);
                    }
                }
                b'B' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    if let Ok(begin) = decoder::transactions::Begin::parse(data) {}
                }
                b'C' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    if let Ok(begin) = decoder::transactions::Begin::parse(data) {}
                }
                b'I' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    send_to_producer(decoder::insert::parse(&data, &relation_map), &producer, 0)
                        .await;
                }
                b'D' => {
                    println!("Remove bytes={:?}", &data);
                    send_to_producer(decoder::delete::parse(&data, &relation_map), &producer, 0)
                        .await;
                }
                b'U' => {
                    println!("Delete bytes={:?}", &data);
                    send_to_producer(decoder::update::parse(&data, &relation_map), &producer, 0)
                        .await;
                }
                _ => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, data);
                }
            },
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
        "FULl"
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
    fn send(
        &self,
        event: ChangeEvent,
        lsn: i32,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}
