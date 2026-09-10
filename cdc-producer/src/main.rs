use std::collections::HashMap;

use anyhow::Result;

use cdc_wal_reader::{Producer, ProducerError, ProducerRecord, ReplicationConfig};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

pub struct KafkaProducer {
    inner: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self, ()> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();

        Ok(Self { inner: producer })
    }
}

impl Producer for KafkaProducer {
    async fn send(&self, record: ProducerRecord) -> Result<(), ProducerError> {
        let future_record = FutureRecord::to(&record.topic)
            .key(&record.key)
            .payload(&record.payload);

        self.inner
            .send(
                future_record,
                Timeout::After(std::time::Duration::from_secs(5)),
            )
            .await
            .unwrap();

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ReplicationConfig::new(
        "localhost",
        "cdc",
        "cdc",      // host, user, password
        "cdc",      // dbname
        "cdc_slot", // slot name
        "cdc_pub",  // publication
    )
    .with_port(5400);

    // TODO: Give proper brokers
    cdc_wal_reader::start_wal_input(config, KafkaProducer::new(""))
        .await
        .unwrap();
    Ok(())
}
