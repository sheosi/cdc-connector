use std::collections::HashMap;

use pgwire_replication::{ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;

mod decoder;

type Result<T> = anyhow::Result<T>;

pub async fn start_wal_input<P: Producer>(config: ReplicationConfig, producer: P) -> Result<()> {
    let mut client = ReplicationClient::connect(config).await?;
    let mut relation_map = HashMap::new();

    while let Some(ev) = client.recv().await? {
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
                }
                b'D' => {
                    println!("Remove bytes={:?}", wal_end, &data);
                    let delete = decoder::insert::parse(data, &relation_map);
                }
                b'U' => {
                    println!("Delete bytes={:?", wal_end, &data);
                    let update = decoder::update::parse(data, &relation_map);
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

pub enum ProducerError {}

pub trait Producer: Send {
    fn send(
        &self,
        record: ProducerRecord,
    ) -> impl std::future::Future<Output = Result<(), ProducerError>>;
}

pub struct ProducerRecord {
    pub topic: String,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, Vec<u8>>,
}
