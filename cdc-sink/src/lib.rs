use std::{collections::HashMap, time::Duration};

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::{ChangeEvent, Relation};
use futures_util::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
};
use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct KafkaConfig {
    pub brokers: String,

    #[serde(default = "default_topic")]
    pub topic: String,
    pub group_id: String,
}

fn default_topic() -> String {
    "cdc".to_string()
}

impl KafkaConfig {
    pub async fn connect(self) -> KafkaClient {
        let log_level = if cfg!(debug_assertions) {
            RDKafkaLogLevel::Debug
        } else {
            RDKafkaLogLevel::Info
        };

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", self.group_id)
            .set("boostrap.servers", self.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("isolation.level", "read_committed")
            .set_log_level(log_level)
            .create()
            .expect("Consumer creation failed");

        KafkaClient {
            consumer,
            topic: self.topic,
        }
    }
}

pub trait KafkaSink {
    fn on_event<'a>(
        &mut self,
        event: ChangeEvent<'a>,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    fn on_relation<'a>(
        &mut self,
        relation: Relation<'a>,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}

pub struct KafkaClient {
    consumer: StreamConsumer,
    topic: String,
}

impl KafkaClient {
    pub async fn load_relations<'a>(
        &self,
        arena: &'a Bump,
    ) -> Result<HashMap<u32, Relation<'a>>, ()> {
        let rel_topic = format!("{}.relations", self.topic);
        self.consumer.subscribe(&[&rel_topic]).map_err(|_| ())?;

        let mut relations = HashMap::new();
        let mut stream = self.consumer.stream();

        loop {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    let payload = msg.payload_view::<[u8]>().unwrap().map_err(|_| ())?;
                    let rel = Relation::from_avro(payload).map_err(|_| ())?;
                    relations.insert(rel.relation_oid, rel);
                }
                Ok(Some(Err(_))) => return Err(()),
                Ok(None) | Err(_) => break,
            }
        }
        Ok(relations)
    }

    pub async fn consume_from_kafka<S: KafkaSink>(&self, mut sink: S) {
        let topic = format!("{}.event.*", self.topic.as_str());
        self.consumer
            .subscribe(&vec![topic.as_str()])
            .expect("Can't subscribe to specified topics");

        let mut stream = self.consumer.stream();
        while let Some(result) = stream.next().await {
            match result {
                Ok(borrowed_message) => {
                    if let Some(view) = borrowed_message.payload_view::<[u8]>() {
                        let Some(topic) = borrowed_message.topic().strip_prefix(&self.topic) else {
                            eprintln!(
                                "Got topic that doesn't start by the prefix, shouldn't happen: {}",
                                borrowed_message.topic()
                            );
                            continue;
                        };

                        if topic.starts_with(".event") {
                            match ChangeEvent::from_avro(view.expect("")) {
                                Ok(event) => {
                                    // This is written a little bit awkward but
                                    if let Err(e) = sink.on_event(event).await {
                                        eprintln!("{:?}", e);
                                    } else if let Err(e) = self.consumer.commit_message(
                                        &borrowed_message,
                                        rdkafka::consumer::CommitMode::Async,
                                    ) {
                                        eprintln!("{:?}", e);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse event's avro: {}", e)
                                }
                            }
                        } else if topic == ".relations" {
                            match Relation::from_avro(view.expect("")) {
                                Ok(relation) => {
                                    if let Err(e) = sink.on_relation(relation).await {
                                        eprintln!("{:?}", e);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse relation's avro: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Kafka error: {:?}", e),
            }
        }
    }
}

pub struct TableNames(HashMap<u32, String, RandomState>);

impl TableNames {
    pub fn new() -> Self {
        Self(HashMap::default())
    }

    pub fn from_rels(rels: &HashMap<u32, Relation<'_>>) -> Self {
        Self(
            rels.into_iter()
                .map(|(i, r)| (*i, r.name.clone()))
                .collect(),
        )
    }

    #[inline]
    pub fn insert(&mut self, rel_oid: u32, value: String) {
        self.0.insert(rel_oid, value);
    }

    #[inline]
    pub fn get(&self, rel_oid: u32) -> Option<&str> {
        self.0.get(&rel_oid).map(|s| s.as_str())
    }
}
