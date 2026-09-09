use std::collections::HashMap;

use cdc_avro::ChangeEvent;
use feldera_rest_api::Client;
use rdkafka::{
    ClientConfig, ClientContext,
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
    types::RDKafka,
};
use serde::Serialize;

pub enum Error {}

#[derive(serde::Serialize)]
pub struct JsonRow {}

#[derive(Default)]
pub struct BridgeConfig {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
    pub feldera_url: String,
    pub feldera_pipeline: String,
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

#[tokio::main]
async fn main() {}
