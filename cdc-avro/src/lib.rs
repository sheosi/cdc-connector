use bumpalo::collections::Vec;
use serde::Serialize;
use serde::{Serializer, ser::SerializeStruct};
use serde_avro_fast::Schema;
use std::sync::LazyLock;
use thiserror::Error;

#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum Op<'a> {
    Insert {
        #[serde(borrow)]
        row: Vec<'a, RowEntry<'a>>,
    },
    Update {
        key: OverrideData<'a>,
        row: Vec<'a, RowEntry<'a>>,
    },
    Delete {
        key: OverrideData<'a>,
    },
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct RowEntry<'a> {
    pub key: &'a str,
    #[serde(borrow)]
    pub value: PgValue<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OverrideData<'a> {
    Key(Vec<'a, PgValue<'a>>),
    Row(Vec<'a, RowEntry<'a>>),
}

impl<'a> Serialize for OverrideData<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            OverrideData::Key(vals) => {
                let mut record = serializer.serialize_struct("Key", 1)?;
                record.serialize_field("Key", vals)?;
                record.end()
            }
            OverrideData::Row(row) => {
                let mut record = serializer.serialize_struct("Row", 1)?;
                record.serialize_field("Row", row)?;
                record.end()
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum FromAvroError {
    #[error("No events where found in the transmission")]
    NoEvents,

    #[error("While ng from Avro: {0}")]
    Avro(#[from] serde_avro_fast::de::DeError),
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ChangeEvent<'a> {
    #[serde(borrow)]
    pub op: Op<'a>,
    pub table: &'a str,
}

impl<'a> ChangeEvent<'a> {
    pub fn from_avro(slice: &'a [u8]) -> Result<Self, FromAvroError> {
        /*Ok(serde_avro_fast::from_datum_slice::<ChangeEvent<'_>>(
            slice,
            &CHANGE_EVENT_SCHEMA,
        )?)*/
        Err(FromAvroError::NoEvents)
    }

    pub fn into_avro(&self) -> Result<std::vec::Vec<u8>, serde_avro_fast::ser::SerError> {
        let schema = &CHANGE_EVENT_SCHEMA;

        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        serde_avro_fast::to_datum(&self, std::vec::Vec::with_capacity(256), &mut config)
    }
}

const CHANGE_EVENT_SCHEMA_STR: &str = r#"{"type":"record","name":"ChangeEvent","fields":[{"name":"op","type":[{"type":"record","name":"Insert","fields":[{"name":"row","type":{"type":"array","items":{"type":"record","name":"RowEntry","fields":[{"name":"key","type":"string"},{"name":"value","type":[{"type":"record","name":"Text","fields":[{"name":"Text","type":"string"}]},{"type":"record","name":"Int4","fields":[{"name":"Int4","type":"long"}]}]}]}}}]},{"type":"record","name":"Update","fields":[{"name":"key","type":[{"type":"record","name":"Key","fields":[{"name":"Key","type":{"type":"array","items":["Text","Int4"]}}]},{"type":"record","name":"Row","fields":[{"name":"Row","type":{"type":"array","items":"RowEntry"}}]}]},{"name":"row","type":{"type":"array","items":"RowEntry"}}]},{"type":"record","name":"Delete","fields":[{"name":"key","type":["Key","Row"]}]}]},{"name":"table","type":"string"}]}"#;

const CHANGE_EVENT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    CHANGE_EVENT_SCHEMA_STR
        .parse()
        .expect("Failed to parse Avro schema")
});

#[derive(Debug, Clone, PartialEq)]
pub enum PgValue<'a> {
    Text(&'a str),
    Int4(u32),
}

impl<'a> Serialize for PgValue<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            PgValue::Text(s) => {
                let mut record = serializer.serialize_struct("Text", 1)?;
                record.serialize_field("Text", s)?;
                record.end()
            }
            PgValue::Int4(n) => {
                let mut record = serializer.serialize_struct("Int4", 1)?;
                record.serialize_field("Int4", n)?;
                record.end()
            }
        }
    }
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

    use bumpalo::{Bump, vec};

    #[test]
    fn back_and_forth() {
        let arena = Bump::with_capacity(1024);

        let event = ChangeEvent {
            op: Op::Insert {
                row: vec![in &arena; RowEntry {
                    key: "a",
                    value: PgValue::Text("b"),
                }],
            },
            table: "users",
        };

        let bytes = event.into_avro().unwrap();

        //Reader::new(std::io::Cursor::new(bytes))
        //let back = ChangeEvent::from_avro(&bytes).unwrap();

        //assert_eq!(event, back);
    }
}
