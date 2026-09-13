use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::LazyLock,
};

use apache_avro::AvroSchemaComponent;
use apache_avro::schema::{Name, NamespaceRef, RecordField, RecordSchema, UnionSchema};
use apache_avro::{AvroSchema, Schema};
use apache_avro::{Reader, from_value};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
#[derive(AvroSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Op {
    Insert {
        row: HashMap<String, PgValue>,
    },
    Update {
        key: OverrideData,
        row: HashMap<String, PgValue>,
    },
    Delete {
        key: OverrideData,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum OverrideData {
    Key(Vec<PgValue>),
    Row(HashMap<String, PgValue>),
}

impl AvroSchemaComponent for OverrideData {
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: NamespaceRef,
    ) -> Schema {
        let vec_schema_ctxt =
            Vec::<PgValue>::get_schema_in_ctxt(named_schemas, enclosing_namespace);
        let hashmap_schema_ctxt =
            HashMap::<String, PgValue>::get_schema_in_ctxt(named_schemas, enclosing_namespace);

        let key =
            newtype_variant_schema(named_schemas, enclosing_namespace, "Key", vec_schema_ctxt);

        let row = newtype_variant_schema(
            named_schemas,
            enclosing_namespace,
            "Row",
            hashmap_schema_ctxt,
        );

        Schema::Union(UnionSchema::new(vec![key, row]).expect("OverrideData union"))
    }
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
        for result in reader.into_deser_iter::<ChangeEvent>() {
            return Ok(result?);
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PgValue {
    Text(String),
    Int4(u32),
}

fn union_of_records_attr() -> BTreeMap<String, JsonValue> {
    let mut m = BTreeMap::new();
    m.insert(
        "org.apache.avro.rust.union_of_records".to_string(),
        JsonValue::Bool(true),
    );
    m
}

fn newtype_variant_schema(
    named_schemas: &mut HashSet<Name>,
    enclosing_namespace: NamespaceRef<'_>,
    variant: &str,
    inner: Schema,
) -> Schema {
    let name = Name::new(variant).expect("valid variant name");

    if named_schemas.contains(&name) {
        return Schema::Ref { name };
    }

    named_schemas.insert(name.clone());
    Schema::Record(
        RecordSchema::builder()
            .name(name)
            .attributes(union_of_records_attr())
            .fields(vec![
                RecordField::builder().name(variant).schema(inner).build(),
            ])
            .build(),
    )
}

impl AvroSchemaComponent for PgValue {
    fn get_schema_in_ctxt(
        named_schemas: &mut HashSet<Name>,
        enclosing_namespace: NamespaceRef,
    ) -> Schema {
        let str_schema_ctxt = String::get_schema_in_ctxt(named_schemas, enclosing_namespace);
        let u32_schema_ctxt = u32::get_schema_in_ctxt(named_schemas, enclosing_namespace);

        let text =
            newtype_variant_schema(named_schemas, enclosing_namespace, "Text", str_schema_ctxt);
        let int4 =
            newtype_variant_schema(named_schemas, enclosing_namespace, "Int4", u32_schema_ctxt);
        Schema::Union(UnionSchema::new(vec![text, int4]).expect("PgValue union"))
    }

    fn get_record_fields_in_ctxt(
        _named_schemas: &mut HashSet<Name>,
        _enclosing_namespace: NamespaceRef,
    ) -> Option<Vec<RecordField>> {
        None
    }
}

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
