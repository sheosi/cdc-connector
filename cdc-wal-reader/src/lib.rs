use std::collections::HashMap;

use cdc_avro::ChangeEvent;
use pgwire_replication::{ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;
use thiserror::Error;

mod decoder;

pub async fn start_wal_input<P: Producer>(
    config: ReplicationConfig,
    producer: P,
) -> Result<(), ProducerError> {
    let mut client = ReplicationClient::connect(config).await.unwrap();
    let mut relation_map = HashMap::new();

    while let Some(ev) = client.recv().await.unwrap() {
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => match data[0] {
                b'R' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    if let Some(relation) = decoder::relation::Relation::parse(data) {
                        println!("{:?}", &relation);
                        relation_map.insert(relation.relation_oid, relation);
                    }
                }
                b'I' => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, &data);
                    let insert = decoder::insert::parse(data, &relation_map);
                    println!("{:?}", insert);
                    producer.send(insert).await;
                }
                b'D' => {
                    println!("Remove bytes={:?}", &data);
                    let delete = decoder::delete::parse(data, &relation_map);
                    producer.send(delete).await;
                }
                b'U' => {
                    println!("Delete bytes={:?}", &data);
                    let update = decoder::update::parse(data, &relation_map);
                    producer.send(update).await;
                }
                _ => {
                    println!("XLogData wal_end={} bytes={:?}", wal_end, data);
                }
            },
            ReplicationEvent::KeepAlive { .. } => {
                // heartbeat; crat handles reply
            }
            ev => println!("other: {:?}", ev),
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum ProducerError {}

pub trait Producer: Send {
    fn send(
        &self,
        event: ChangeEvent,
    ) -> impl std::future::Future<Output = Result<(), ProducerError>>;
}
