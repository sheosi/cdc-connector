use std::collections::HashMap;

use cdc_avro::ChangeEvent;
use pgwire_replication::{ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;

use crate::decoder::DecoderError;

mod decoder;

async fn send_to_producer<P>(event: Result<ChangeEvent, DecoderError>, producer: &P)
where
    P: Producer,
{
    match event {
        Ok(delete) => {
            if let Err(e) = producer.send(delete).await {
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
    producer: P,
) -> Result<(), pgwire_replication::PgWireError> {
    let mut client = ReplicationClient::connect(config).await?;
    let mut relation_map = HashMap::new();

    while let Some(ev) = client.recv().await? {
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => match data[0] {
                b'R' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    if let Ok(relation) = decoder::relation::Relation::parse(data) {
                        println!("{:?}", &relation);
                        relation_map.insert(relation.relation_oid, relation);
                    }
                }
                b'I' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    send_to_producer(decoder::insert::parse(data, &relation_map), &producer).await;
                }
                b'D' => {
                    println!("Remove bytes={:?}", &data);
                    send_to_producer(decoder::delete::parse(data, &relation_map), &producer).await;
                }
                b'U' => {
                    println!("Delete bytes={:?}", &data);
                    send_to_producer(decoder::update::parse(data, &relation_map), &producer).await;
                }
                _ => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, data);
                }
            },
            ReplicationEvent::KeepAlive { .. } => {
                // heartbeat; crate handles reply
            }
            ev => println!("other: {:?}", ev),
        }
    }

    Ok(())
}

pub trait Producer: Send {
    fn send(&self, event: ChangeEvent) -> impl std::future::Future<Output = Result<(), String>>;
}
