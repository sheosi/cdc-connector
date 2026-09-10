use anyhow::Result;

use cdc_wal_reader::{Producer, ProducerError, ProducerRecord, ReplicationConfig};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Deserialize;

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

#[derive(Deserialize)]
struct ProducerConfig {
    host: String,
    user: String,
    password: String,
    slot_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let own_config: ProducerConfig = config::Config::builder()
        .add_source(config::File::with_name("cdc-producer"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    let config = ReplicationConfig::new(
        own_config.host,
        own_config.user,
        own_config.password,  // host, user, password
        "cdc",                // dbname
        own_config.slot_name, // slot name
        "cdc_pub",            // publication
    )
    .with_port(5400);

    // TODO: Give proper brokers
    cdc_wal_reader::start_wal_input(config, KafkaProducer::new("").unwrap())
        .await
        .unwrap();
    Ok(())
}
