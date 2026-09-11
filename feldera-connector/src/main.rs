use std::collections::HashMap;

use cdc_avro::ChangeEvent;
use cdc_sink::KafkaConfig;
use cdc_sink::KafkaSink;
use config::Config;
use feldera_rest_api::Client;
use serde::Deserialize;

use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("A")]
    A,
}

#[derive(Deserialize, Default)]
pub struct BridgeConfig {
    pub kafka: KafkaConfig,
    pub feldera: FelderaConfig,
}

#[derive(Deserialize, Default)]
pub struct FelderaConfig {
    pub url: String,
    pub pipeline: String,
}

pub struct FelderaConnector {
    inner: Client,
    pipeline: String,
}

#[derive(Serialize)]
enum FelderaEvent {
    Insert(HashMap<String, String>),
    Delete(HashMap<String, String>),
}

impl FelderaEvent {
    fn convert_op_batches(batch: Vec<ChangeEvent>) -> Vec<FelderaEvent> {
        let mut result = Vec::with_capacity(batch.len());

        // TODO: How do we translate to ops?

        for op in batch.into_iter() {
            match op.op {
                cdc_avro::Op::Insert { row } => result.push(FelderaEvent::Insert(row)),
                cdc_avro::Op::Update { key, row } => {
                    // TODO! Add delete data

                    //result.push(FelderaEvent::Delete());
                    result.push(FelderaEvent::Insert(row));
                }
                cdc_avro::Op::Delete { key } => { /*TODO: Add delete*//*result.push(FelderaEvent::Delete());*/
                }
            }
        }

        result
    }

    fn to_lines(batch: Vec<Self>) -> String {
        let mut result = Vec::new();

        for op in batch.into_iter() {
            serde_json::to_writer(&mut result, &op).unwrap();
            result.push(b'\n');
        }

        // This is fine, we know that serde_json (and what we add) is all UTF-8
        unsafe { String::from_utf8_unchecked(result) }
    }
}

impl FelderaConnector {
    pub fn new(base_url: &str, pipeline: String) -> Self {
        let inner = Client::new(base_url, feldera_rest_api::RetryPolicy::default());

        Self { inner, pipeline }
    }

    pub async fn insert_batch(&self, table: &str, records: Vec<ChangeEvent>) -> Result<(), Error> {
        let json_str = FelderaEvent::to_lines(FelderaEvent::convert_op_batches(records));

        self.inner
            .http_input()
            .pipeline_name(self.pipeline.clone())
            .table_name(table)
            .format("json")
            .update_format(feldera_types::format::json::JsonUpdateFormat::InsertDelete)
            .body(json_str)
            .send()
            .await
            .unwrap();

        Ok(())
    }
}
impl KafkaSink for FelderaConnector {
    async fn on_event(&mut self, event: ChangeEvent) -> Result<(), ()> {
        self.insert_batch(&event.table.clone(), vec![event])
            .await
            .unwrap();

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("feldera-connector"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    cdc_sink::consume_from_kafka(
        config.kafka,
        FelderaConnector::new(&config.feldera.url, config.feldera.pipeline),
    )
    .await;
}
