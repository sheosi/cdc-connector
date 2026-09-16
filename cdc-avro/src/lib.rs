use std::{collections::HashMap, sync::LazyLock};

use serde::{Deserialize, Serialize};
use serde_avro_fast::Schema;
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Op<'a, 'b> {
    Insert {
        #[serde(borrow)]
        row: HashMap<&'b str, PgValue<'a>>,
    },
    Update {
        key: OverrideData<'a, 'b>,
        row: HashMap<&'b str, PgValue<'a>>,
    },
    Delete {
        key: OverrideData<'a, 'b>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum OverrideData<'a, 'b> {
    #[serde(borrow)]
    Key(Vec<PgValue<'a>>),
    #[serde(borrow)]
    Row(HashMap<&'b str, PgValue<'a>>),
}

#[derive(Debug, Error)]
pub enum FromAvroError {
    #[error("No events where found in the transmission")]
    NoEvents,

    #[error("While deserializeing from Avro: {0}")]
    Avro(#[from] serde_avro_fast::de::DeError),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ChangeEvent<'a, 'b> {
    #[serde(borrow)]
    pub op: Op<'a, 'b>,
    pub table: &'b str,
}

impl<'a: 'b, 'b> ChangeEvent<'a, 'b> {
    pub fn from_avro(slice: &'a [u8]) -> Result<Self, FromAvroError> {
        Ok(serde_avro_fast::from_datum_slice::<ChangeEvent>(
            slice,
            &CHANGE_EVENT_SCHEMA,
        )?)
    }

    pub fn into_avro(&self) -> Result<Vec<u8>, serde_avro_fast::ser::SerError> {
        let schema = &CHANGE_EVENT_SCHEMA;

        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        serde_avro_fast::to_datum(&self, Vec::with_capacity(256), &mut config)
    }
}

const CHANGE_EVENT_SCHEMA_STR: &str = r#"{"name":"ChangeEvent","type":"record","fields":[{"name":"op","type":[{"name":"Insert","type":"record","fields":[{"name":"row","type":{"type":"map","values":[{"name":"Text","type":"record","fields":[{"name":"Text","type":"string"}]},{"name":"Int4","type":"record","fields":[{"name":"Int4","type":"long"}]}]}}]},{"name":"Update","type":"record","fields":[{"name":"key","type":[{"name":"Key","type":"record","fields":[{"name":"Key","type":{"type":"array","items":["Text","Int4"]}}]},{"name":"Row","type":"record","fields":[{"name":"Row","type":{"type":"map","values":["Text","Int4"]}}]}]},{"name":"row","type":{"type":"map","values":["Text","Int4"]}}]},{"name":"Delete","type":"record","fields":[{"name":"key","type":["Key","Row"]}]}]},{"name":"table","type":"string"}]}"#;

const CHANGE_EVENT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    CHANGE_EVENT_SCHEMA_STR
        .parse()
        .expect("Failed to parse Avro schema")
});

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PgValue<'a> {
    Text(&'a str),
    Int4(u32),
}

impl<'a> From<&'a str> for PgValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::Text(value)
    }
}

impl From<u32> for PgValue<'_> {
    fn from(value: u32) -> Self {
        Self::Int4(value)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn back_and_forth() {
        let event = ChangeEvent {
            op: Op::Insert {
                row: maplit::hashmap!("a"=>PgValue::Text("b")),
            },
            table: "users",
        };

        let bytes = event.into_avro().unwrap();

        //Reader::new(std::io::Cursor::new(bytes))
        //let back = ChangeEvent::from_avro(&bytes).unwrap();

        //assert_eq!(event, back);
    }
}

#[cfg(test)]
mod schema_dump {
    use super::CHANGE_EVENT_SCHEMA;
    use serde_avro_fast::Schema;

    #[test]
    fn dump_parsed_schema() {
        eprintln!("=== serde_avro_fast parsed schema ===");
        eprintln!("{}", CHANGE_EVENT_SCHEMA.canonical_form());
    }
}
