use anyhow::Result;

use bumpalo::Bump;
use cdc_avro::{ChangeEvent, Relation};
use cdc_wal_reader::{Producer as CdcProducer, ReplicationConfig};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::Deserialize;

pub struct KafkaProducer {
    topic: String,
    lsn_topic: String,
    inner: FutureProducer,
    key: String,
    arena: Bump,
}

impl KafkaProducer {
    fn new(config: &KafkaConfig) -> Result<Self, rdkafka::error::KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.clone())
            .set("message.timeout.ms", "5000")
            .set("transactional.id", "cdc-producer-1")
            .set("enable.idempotence", "true")
            .create()?;

        Ok(Self {
            inner: producer,
            topic: config.topic.clone(),
            lsn_topic: format!("{}_lsn", config.topic),
            key: config.key.clone(),
            arena: Bump::with_capacity(2048),
        })
    }
}

impl CdcProducer for KafkaProducer {
    async fn send<'a>(
        &self,
        relation: &Relation<'a>,
        event: ChangeEvent<'a>,
        lsn: i32,
    ) -> Result<(), String> {
        let payload = event.into_avro().map_err(|e| e.to_string())?;
        let lsn_payload = lsn.to_be_bytes();
        let topic = format!(
            "{}.events.{}.{}",
            self.topic, relation.namespace, relation.name
        );

        let future_record = FutureRecord::to(&topic).key(&self.key).payload(&payload);

        let lsn_future_record = FutureRecord::to(&self.lsn_topic)
            .key(&self.key)
            .payload(&lsn_payload);

        self.inner
            .init_transactions(std::time::Duration::from_secs(3))
            .map_err(|e| e.to_string())?;

        self.inner
            .send(
                future_record,
                Timeout::After(std::time::Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| e.to_string())?;

        self.inner
            .send(
                lsn_future_record,
                Timeout::After(std::time::Duration::from_secs(5)),
            )
            .await
            .map_err(|(e, _)| e.to_string())?;

        self.inner
            .commit_transaction(std::time::Duration::from_secs(3))
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn on_relation<'a>(
        &mut self,
        relation: &Relation<'a>,
    ) -> std::prelude::v1::Result<(), String> {
        let relation_bin = relation.to_avro().map_err(|e| e.to_string())?;

        let rel_topic = format!("{}.relations", self.topic);
        let as_bytes = relation.relation_oid.to_be_bytes();

        let future_record = FutureRecord::to(&rel_topic)
            .key(&as_bytes)
            .payload(&relation_bin);

        self.inner
            .send(future_record, std::time::Duration::from_secs(5))
            .await
            .map_err(|(e, _)| e.to_string())?;

        Ok(())
    }
}

#[derive(Deserialize)]
struct ProducerConfig {
    postgres: PostgresConfig,
    kafka: KafkaConfig,
    will_connect_to_feldera: bool,
}

#[derive(Deserialize)]
struct PostgresConfig {
    host: String,
    user: String,
    password: String,
    slot_name: String,
}

#[derive(Deserialize)]
struct KafkaConfig {
    brokers: String,

    #[serde(default = "default_topic")]
    topic: String,
    key: String,
}

fn default_topic() -> String {
    "cdc".to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    let own_config: ProducerConfig = config::Config::builder()
        .add_source(config::File::with_name("cdc-producer"))
        .build()
        .expect("Failed to find cdc-producer config")
        .try_deserialize()
        .expect("cdc-producer config is malformated");

    let config = ReplicationConfig::new(
        own_config.postgres.host,
        own_config.postgres.user,
        own_config.postgres.password,  // host, user, password
        "cdc",                         // dbname
        own_config.postgres.slot_name, // slot name
        "cdc_pub",                     // publication
    )
    .with_port(5400);

    cdc_wal_reader::start_wal_input(
        config,
        own_config.will_connect_to_feldera,
        KafkaProducer::new(&own_config.kafka).expect("Failed to init kafka"),
    )
    .await
    .expect("Wal input loop had an error");
    Ok(())
}
