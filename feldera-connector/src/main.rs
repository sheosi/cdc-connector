use std::collections::HashMap;

use bumpalo::Bump;
use cdc_avro::{ChangeEvent, PgValue, Relation};
use cdc_sink::TableNames;
use cdc_sink::{KafkaConfig, KafkaSink};
use config::Config;
use feldera_rest_api::Client;
use serde::Deserialize;

use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Feldera api had an error {0}")]
    Feldera(#[from] feldera_rest_api::Error<feldera_types::error::ErrorResponse>),

    #[error("Serialization {0}")]
    Serialization(#[from] serde_json::Error),
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
    table_names: TableNames,
}

#[derive(Serialize)]
enum FelderaEvent<'a> {
    Insert(bumpalo::collections::Vec<'a, PgValue>),
    Delete(bumpalo::collections::Vec<'a, PgValue>),
}

impl<'a> FelderaEvent<'a> {
    fn convert_op_batches(batch: Vec<ChangeEvent<'a>>) -> Vec<FelderaEvent<'a>> {
        let mut result = Vec::with_capacity(batch.len());

        for op in batch.into_iter() {
            match op.op {
                cdc_avro::Op::Insert { row } => result.push(FelderaEvent::Insert(row)),
                cdc_avro::Op::Update { old, row } => {
                    result.push(FelderaEvent::Delete(old));
                    result.push(FelderaEvent::Insert(row));
                }
                cdc_avro::Op::Delete { old } => {
                    result.push(FelderaEvent::Delete(old));
                }
            }
        }

        result
    }

    fn to_lines(batch: Vec<Self>) -> Result<String, serde_json::Error> {
        let mut result = Vec::new();

        for op in batch.into_iter() {
            serde_json::to_writer(&mut result, &op)?;
            result.push(b'\n');
        }

        // This is fine, we know that serde_json (and what we add) is all UTF-8
        Ok(unsafe { String::from_utf8_unchecked(result) })
    }
}

impl FelderaConnector {
    pub fn new(base_url: &str, pipeline: String, relations: HashMap<u32, Relation<'_>>) -> Self {
        let inner = Client::new(base_url, feldera_rest_api::RetryPolicy::default());

        Self {
            inner,
            pipeline,
            table_names: TableNames::from_rels(&relations),
        }
    }

    pub async fn insert_batch<'a>(
        &self,
        table: &str,
        records: Vec<ChangeEvent<'a>>,
    ) -> Result<(), Error> {
        let json_str = FelderaEvent::to_lines(FelderaEvent::convert_op_batches(records))?;

        self.inner
            .http_input()
            .pipeline_name(self.pipeline.clone())
            .table_name(table)
            .format("json")
            .update_format(feldera_types::format::json::JsonUpdateFormat::InsertDelete)
            .body(json_str)
            .send()
            .await?;

        Ok(())
    }
}
impl KafkaSink for FelderaConnector {
    async fn on_event<'a>(&mut self, event: ChangeEvent<'a>) -> Result<(), String> {
        self.insert_batch(self.table_names.get(event.rel).unwrap(), vec![event])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn on_relation<'a>(&mut self, relation: Relation<'a>) -> Result<(), String> {
        self.table_names
            .insert(relation.relation_oid, relation.name);

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("feldera-connector"))
        .build()
        .expect("Failed to load feldera-connector config")
        .try_deserialize()
        .expect("Feldera-connector config was malformed");

    let arena = &Bump::with_capacity(2048);

    let kafka = config.kafka.connect().await;
    let relations = kafka
        .load_relations(arena)
        .await
        .expect("Failed to load relations");

    kafka
        .consume_from_kafka(FelderaConnector::new(
            &config.feldera.url,
            config.feldera.pipeline,
            relations,
        ))
        .await;
}
