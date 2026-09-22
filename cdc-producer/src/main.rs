use std::time::Duration;

use anyhow::Result;

use bumpalo::Bump;
use cdc_avro::{ChangeEvent, Relation};
use cdc_wal_reader::Producer as CdcProducer;
use futures_util::stream::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use serde::Deserialize;

pub struct KafkaProducer {
    topic: String,
    lsn_topic: String,
    inner: FutureProducer,
    key: String,
    arena: Bump,
}

impl KafkaProducer {
    async fn new(config: &KafkaConfig) -> Result<Self, rdkafka::error::KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.clone())
            .set("message.timeout.ms", "5000")
            .set("transactional.id", "cdc-producer-1")
            .set("enable.idempotence", "true")
            .create()?;

        producer.init_transactions(Duration::from_secs(3))?;

        Ok(Self {
            inner: producer,
            topic: config.topic.clone(),
            lsn_topic: format!("{}.lsn", config.topic),
            key: config.key.clone(),
            arena: Bump::with_capacity(2048),
        })
    }
}

impl CdcProducer for KafkaProducer {
    async fn start_transaction(&self) -> Result<(), String> {
        self.inner.begin_transaction().map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn send<'a>(
        &self,
        relation: &Relation<'a>,
        event: ChangeEvent<'a>,
    ) -> Result<(), String> {
        let payload = event.into_avro().map_err(|e| e.to_string())?;
        let topic = format!(
            "{}.events.{}.{}",
            self.topic, relation.namespace, relation.name
        );

        let future_record = FutureRecord::to(&topic).key(&self.key).payload(&payload);

        self.inner
            .send(future_record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(e, _)| e.to_string())?;

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
            .send(future_record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| e.to_string())?;

        Ok(())
    }

    async fn commit_transaction(&self, lsn: u64) -> std::prelude::v1::Result<(), String> {
        let lsn_payload = lsn.to_be_bytes();

        let lsn_future_record = FutureRecord::to(&self.lsn_topic)
            .key(&self.key)
            .payload(&lsn_payload);

        self.inner
            .send(lsn_future_record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(e, _)| e.to_string())?;

        self.inner
            .commit_transaction(Duration::from_secs(3))
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn abort_transaction(&self) -> Result<(), String> {
        self.inner
            .abort_transaction(Duration::from_secs(5))
            .map_err(|e| e.to_string())
    }
}

#[derive(Deserialize)]
struct ProducerConfig {
    postgres: cdc_wal_reader::PostgresConfig,
    kafka: KafkaConfig,
    will_connect_to_feldera: bool,
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

async fn read_lsn(kafka_config: &KafkaConfig) -> Result<u64, rdkafka::error::KafkaError> {
    let log_level = if cfg!(debug_assertions) {
        RDKafkaLogLevel::Debug
    } else {
        RDKafkaLogLevel::Info
    };

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "producer-lsn-read".to_string())
        .set("boostrap.servers", kafka_config.brokers.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("isolation.level", "read_committed")
        .set_log_level(log_level)
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[&format!("{}.lsn", &kafka_config.topic)])?;

    match tokio::time::timeout(Duration::from_millis(500), consumer.stream().next()).await {
        Ok(Some(Ok(msg))) => Ok(u64::from_be_bytes(
            msg.payload_view::<[u8]>()
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        )),
        Ok(Some(Err(e))) => return Err(e),
        Ok(None) | Err(_) => {
            println!("Lsn read timeout starting from 0");
            Ok(0)
        }
    }
}

async fn ensure_topic(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
    partitions: i32,
    replication: i32,
    compact: bool,
) -> Result<(), String> {
    let metadata = admin
        .inner()
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .map_err(|e| e.to_string())?;

    if metadata.topics().is_empty() || metadata.topics()[0].partitions().is_empty() {
        let mut new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(replication));
        if compact {
            new_topic = new_topic.set("cleanup.policy", "compact");
        }
        admin
            .create_topics(
                &[new_topic],
                &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
            )
            .await
            .map_err(|e| e.to_string())?;

        return Ok(());
    }

    let topic_meta = &metadata.topics()[0];
    if topic_meta.partitions().len() != partitions as usize {
        return Err(format!("{} has wrong partition count", topic));
    }

    let resource = ResourceSpecifier::Topic(topic);
    let configs = admin
        .describe_configs(&[resource], &AdminOptions::new())
        .await
        .map_err(|e| e.to_string())?;

    for config in configs {
        if let Ok(config) = config {
            for entry in config.entries {
                if entry.name == "cleanup.policy" {
                    let is_compact = matches!(entry.value.as_deref(), Some("compact"));

                    if !(compact && is_compact || !compact && !is_compact) {
                        eprintln!(
                            "Correctly setting compactability of topic '{}' has failed",
                            topic
                        );
                        panic!();
                    }
                }
            }
        }
    }
    Ok(())
}

async fn check_topics(kafka_config: &KafkaConfig) -> Result<(), ()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &kafka_config.brokers)
        .create()
        .unwrap();

    ensure_topic(&admin, &format!("{}.lsn", &kafka_config.topic), 1, 1, true)
        .await
        .unwrap();

    ensure_topic(
        &admin,
        &format!("{}.relations", &kafka_config.topic),
        1,
        1,
        true,
    )
    .await
    .unwrap();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let own_config: ProducerConfig = config::Config::builder()
        .add_source(config::File::with_name("cdc-producer"))
        .build()
        .expect("Failed to find cdc-producer config")
        .try_deserialize()
        .expect("cdc-producer config is malformated");

    check_topics(&own_config.kafka)
        .await
        .expect("Failed to setup topics");

    let lsn = read_lsn(&own_config.kafka)
        .await
        .expect("Failed to read lsn");

    cdc_wal_reader::start_wal_input(
        own_config.postgres,
        lsn,
        own_config.will_connect_to_feldera,
        KafkaProducer::new(&own_config.kafka)
            .await
            .expect("Failed to init kafka"),
    )
    .await
    .expect("Wal input loop had an error");
    Ok(())
}
