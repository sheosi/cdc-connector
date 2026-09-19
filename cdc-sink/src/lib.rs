use std::collections::HashMap;

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
    pub topic: String,
    pub group_id: String,
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
        // TODO: This
        Err(())
    }

    pub async fn consume_from_kafka<S: KafkaSink>(&self, mut sink: S) {
        self.consumer
            .subscribe(&vec![self.topic.as_str()])
            .expect("Can't subscribe to specified topics");

        let mut stream = self.consumer.stream();
        while let Some(result) = stream.next().await {
            match result {
                Ok(borrowed_message) => {
                    if let Some(view) = borrowed_message.payload_view::<[u8]>() {
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
                                eprintln!("Failed to retrieve avro: {}", e)
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

    #[inline]
    pub fn insert(&mut self, rel_oid: u32, value: String) {
        self.0.insert(rel_oid, value);
    }

    #[inline]
    pub fn get(&self, rel_oid: u32) -> Option<&str> {
        self.0.get(&rel_oid).map(|s| s.as_str())
    }
}
