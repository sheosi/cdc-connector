use std::{collections::HashMap, sync::LazyLock};

use apache_avro::{Reader, from_value};

use apache_avro::{AvroSchema, Schema};
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Op {
    Insert {
        row: HashMap<String, PgValue>,
    },
    Update {
        key: String,
        row: HashMap<String, PgValue>,
    },
    Delete {
        key: String,
    },
}

#[derive(Debug, Error)]
pub enum FromAvroError {
    #[error("No events where found in the transmission")]
    NoEvents,

    #[error("While deserializeing from Avro: {0}")]
    Avro(#[from] apache_avro::Error),
}

#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    pub op: Op,
    pub table: String,
}

impl ChangeEvent {
    pub fn from_avro(bytes: &[u8]) -> Result<Self, FromAvroError> {
        let reader = Reader::new(std::io::Cursor::new(bytes))?;
        for result in reader {
            let new_event: ChangeEvent = from_value(&result?)?;
            return Ok(new_event);
        }

        Err(FromAvroError::NoEvents)
    }

    pub fn into_avro(&self) -> Result<Vec<u8>, apache_avro::Error> {
        let schema = &CHANGE_EVENT_SCHEMA;

        let mut writer = apache_avro::Writer::new(schema, Vec::with_capacity(100))?;

        writer.append_ser(self)?;
        writer.flush()?;

        writer.into_inner()
    }
}

const CHANGE_EVENT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| ChangeEvent::get_schema());

#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PgValue {
    Text(String),
    Int4(u32),
}

impl PgValue {}

impl From<String> for PgValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<u32> for PgValue {
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
                row: maplit::hashmap!("a".to_string()=>PgValue::Text( "b".to_string())),
            },
            table: "users".to_string(),
        };

        let bytes = event.into_avro().unwrap();

        let back = ChangeEvent::from_avro(&bytes).unwrap();

        assert_eq!(event, back);
    }
}
