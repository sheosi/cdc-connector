use std::collections::HashMap;

use pgwire_replication::{ReplicationClient, ReplicationEvent};

pub use pgwire_replication::ReplicationConfig;

mod decoder;

type Result<T> = anyhow::Result<T>;

pub async fn start_wal_input(config: ReplicationConfig) -> Result<()> {
    let mut client = ReplicationClient::connect(config).await?;
    let mut relation_map = HashMap::new();

    // TODO? What now?

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
