use std::collections::HashMap;

use bumpalo::Bump;
use cdc_avro::{
    Field, PgValue, Relation,
    owned::{ChangeEvent, Op},
};
use cdc_sink::{KafkaConfig, KafkaSink, SinkError};
use cdc_sink::{MetricsConfig, TableNames};
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

    #[error("Unknow oid {0}")]
    UnknownRelation(u32),
}

#[derive(Deserialize, Default)]
pub struct BridgeConfig {
    pub kafka: KafkaConfig,
    pub feldera: FelderaConfig,

    #[serde(default)]
    pub metrics: MetricsConfig,
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
    field_names: HashMap<u32, Vec<String>>,
}

type FelderaRow<'a> = HashMap<&'a str, serde_json::Value>;

#[derive(Serialize)]
enum FelderaEvent<'a> {
    #[serde(rename = "insert")]
    Insert(FelderaRow<'a>),

    #[serde(rename = "delete")]
    Delete(FelderaRow<'a>),
}

impl<'a> FelderaEvent<'a> {
    fn convert_row(
        oid: u32,
        values: Vec<PgValue<'_>>,
        field_names: &'a HashMap<u32, Vec<String>>,
    ) -> Result<FelderaRow<'a>, Error> {
        if let Some(fields) = field_names.get(&oid) {
            let val = values
                .iter()
                .zip(fields)
                .map(|(v, f)| (f.as_str(), v.to_json_value()))
                .collect();

            Ok(val)
        } else {
            Err(Error::UnknownRelation(oid))
        }
    }

    fn convert_op_batches(
        batch: Vec<ChangeEvent<'a>>,
        field_names: &'a HashMap<u32, Vec<String>>,
    ) -> Result<Vec<FelderaEvent<'a>>, Error> {
        let mut result = Vec::with_capacity(batch.len());

        for op in batch.into_iter() {
            match op.op {
                Op::Insert { row } => result.push(FelderaEvent::Insert(Self::convert_row(
                    op.rel,
                    row,
                    field_names,
                )?)),
                Op::Update { old, row } => {
                    result.push(FelderaEvent::Delete(Self::convert_row(
                        op.rel,
                        old,
                        field_names,
                    )?));
                    result.push(FelderaEvent::Insert(Self::convert_row(
                        op.rel,
                        row,
                        field_names,
                    )?));
                }
                Op::Delete { old } => {
                    result.push(FelderaEvent::Delete(Self::convert_row(
                        op.rel,
                        old,
                        field_names,
                    )?));
                }
            }
        }

        Ok(result)
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
    pub fn new(base_url: &str, pipeline: String, relations: HashMap<u32, Relation>) -> Self {
        let inner = Client::new(base_url, feldera_rest_api::RetryPolicy::default());

        let mut field_names = HashMap::new();
        for (oid, rel) in relations.iter() {
            field_names.insert(*oid, extract_names(&rel.fields));
        }

        Self {
            inner,
            pipeline,
            table_names: TableNames::from_rels(&relations),
            field_names,
        }
    }

    pub async fn insert_batch<'a>(
        &self,
        table: &str,
        records: Vec<ChangeEvent<'a>>,
    ) -> Result<(), Error> {
        let json_str = FelderaEvent::to_lines(FelderaEvent::convert_op_batches(
            records,
            &self.field_names,
        )?)?;

        self.inner
            .http_input()
            .pipeline_name(self.pipeline.clone())
            .table_name(table)
            .format("json")
            .update_format(feldera_types::format::json::JsonUpdateFormat::InsertDelete)
            .force(false)
            .body(json_str)
            .send()
            .await?;

        Ok(())
    }
}
impl KafkaSink for FelderaConnector {
    async fn on_event<'a>(
        &mut self,
        mut event: ChangeEvent<'a>,
        _arena: &Bump,
    ) -> Result<(), SinkError> {
        if let Err(()) = event.op.merge_toasted() {
            return Err(SinkError::ToastedInKeyOnly);
        }

        self.insert_batch(
            self.table_names
                .get(event.rel)
                .ok_or_else(|| SinkError::UnknownRelation(event.rel))?,
            vec![event],
        )
        .await
        .map_err(|e| match e {
            Error::UnknownRelation(oid) => SinkError::UnknownRelation(oid),
            e => SinkError::Platform(e.to_string()),
        })?;

        Ok(())
    }

    async fn on_relation(&mut self, relation: Relation) -> Result<(), SinkError> {
        self.table_names
            .insert(relation.relation_oid, relation.name);
        self.field_names
            .insert(relation.relation_oid, extract_names(&relation.fields));

        Ok(())
    }
}

fn extract_names(fields: &[Field]) -> Vec<String> {
    fields.iter().map(|f| f.name.clone()).collect()
}

#[tokio::main]
async fn main() {
    cdc_sink::init_logs();

    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("config/feldera-connector").required(false))
        .add_source(config::Environment::with_prefix("FELD_CONN").separator("_"))
        .build()
        .expect("Failed to load feldera-connector config")
        .try_deserialize()
        .expect("Feldera-connector config was malformed");

    println!("{}", config.kafka.groupid);

    cdc_sink::init_metrics(config.metrics);

    let mut arena = Bump::with_capacity(2048);

    let kafka = config.kafka.connect().await;
    let relations = kafka
        .load_relations(&arena)
        .await
        .expect("Failed to load relations");

    arena.reset();

    kafka
        .consume_from_kafka(FelderaConnector::new(
            &config.feldera.url,
            config.feldera.pipeline,
            relations,
        ))
        .await;
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn feldera_event_json_shape() {
        let mut fields = HashMap::new();
        fields.insert(16386, vec!["id".to_string(), "name".to_string()]);

        let event = ChangeEvent {
            op: Op::Insert {
                row: vec![PgValue::Int4(1), PgValue::Text("Ada")],
            },
            rel: 16386,
        };

        let json =
            FelderaEvent::to_lines(FelderaEvent::convert_op_batches(vec![event], &fields).unwrap())
                .unwrap();
        assert!(json.contains(r#"{"insert":{"id":1,"name":"Ada"}}"#));
    }
}
