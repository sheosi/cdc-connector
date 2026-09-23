use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    time::{Duration, Instant},
};

use ahash::RandomState;
use bumpalo::Bump;
use cdc_avro::{ChangeEvent, Relation};
use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
};
use serde::Deserialize;
use std::net::SocketAddr;

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

#[derive(Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_config")]
    port: u16,
}

fn default_metrics_config() -> u16 {
    9000
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            port: default_metrics_config(),
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
        let topic = format!("{}.events.*", self.topic.as_str());
        let rel_topic = format!("{}.relations", self.topic);

        self.consumer
            .subscribe(&vec![topic.as_str(), rel_topic.as_str()])
            .expect("Can't subscribe to specified topics");

        let mut stream = self.consumer.stream();
        while let Some(result) = stream.next().await {
            match result {
                Ok(borrowed_message) => {
                    if let Some(ts) = borrowed_message.timestamp().to_millis() {
                        let lag_secs = (Utc::now().timestamp_millis() - ts) as f64 / 1000.0;
                        metrics::gauge!("cdc_kafka_lag_seconds", "topic" => topic.clone(), "partition" => borrowed_message.partition().to_string())
                             .set(lag_secs.max(0.0));
                    }

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
                                    let start = Instant::now();

                                    let event_rel = event.rel.to_string();
                                    let op_str = event.op.op_str();

                                    // This is written a little bit awkward but
                                    if let Err(e) = sink.on_event(event).await {
                                        eprintln!("{:?}", e);
                                    } else if let Err(e) = self.consumer.commit_message(
                                        &borrowed_message,
                                        rdkafka::consumer::CommitMode::Async,
                                    ) {
                                        eprintln!("{:?}", e);
                                    }

                                    metrics::histogram!("cdc_event_process_duration_seconds", "op"=> op_str)
                                        .record(start.elapsed().as_secs_f64());

                                    metrics::counter!("cdc_events_consumed_total", "op" => op_str, "rel" => event_rel)
                                        .increment(1);
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

pub fn init_metrics(config: MetricsConfig) {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED.into(), config.port));

    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .with_http_listener(addr)
        .install_recorder()
        .expect("Failed to install recorder");
    metrics::describe_counter!(
        "cdc_events_consumed_total",
        "The total ammount of events consumed by this instance"
    );
    metrics::describe_histogram!(
        "cdc_event_process_duration_seconds",
        "The duration of the processing"
    );
    metrics::describe_gauge!(
        "cdc_kafka_lag_seconds",
        "The lag introduced by Kafka, in seconds"
    );
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
