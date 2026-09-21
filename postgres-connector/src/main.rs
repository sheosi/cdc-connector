use bumpalo::Bump;
use cdc_avro::{ChangeEvent, Field, PgValue, Relation, ReplicaKind};
use cdc_sink::{KafkaConfig, KafkaSink, TableNames};
use config::Config;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tokio_postgres::{
    Connection, NoTls, Socket,
    tls::NoTlsStream,
    types::{IsNull, ToSql},
};

use crate::statements::{DeleteStatementCache, InsertStatementCache};

mod statements;

#[tokio::main]
async fn main() {
    let arena = Bump::with_capacity(2048);

    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("feldera-connector"))
        .build()
        .expect("Failed to find postgres-connect config")
        .try_deserialize()
        .expect("postgres-connect config is malformed");

    let kafka = config.kafka.connect().await;
    let relations = kafka
        .load_relations(&arena)
        .await
        .expect("Failed to load relations");

    kafka
        .consume_from_kafka(
            PostgresSink::new(config.postgres, relations)
                .await
                .expect("Failed to connect to postgres"),
        )
        .await;
}

#[derive(Debug, Error)]
enum BridgeError {
    #[error("Found unknown relation oid")]
    UnknowRelation,
}

#[derive(Deserialize)]
struct BridgeConfig {
    kafka: KafkaConfig,
    postgres: PostgresConfig,
}

#[derive(Deserialize)]
struct PostgresConfig {
    host: String,
    user: String,
    password: String,
}

impl PostgresConfig {
    fn to_postgres_string(&self) -> String {
        format!(
            "host={} user={} password={}",
            self.host, self.user, self.password
        )
    }
}

pub struct PostgresSink {
    client: tokio_postgres::Client,
    conn: Connection<Socket, NoTlsStream>,
    pk_cache: HashMap<String, Vec<String>>,
    insert_stmt_cache: InsertStatementCache,
    delete_stmt_cache: DeleteStatementCache,
    relation_cache: RelationCache,
}

impl PostgresSink {
    async fn new(
        config: PostgresConfig,
        relations: HashMap<u32, Relation<'_>>,
    ) -> Result<Self, tokio_postgres::Error> {
        let (clt, conn) = tokio_postgres::connect(&config.to_postgres_string(), NoTls).await?;

        Ok(Self {
            client: clt,
            conn,
            pk_cache: HashMap::new(),
            insert_stmt_cache: InsertStatementCache::new(),
            delete_stmt_cache: DeleteStatementCache::new(),
            relation_cache: RelationCache::from_rels(&relations),
        })
    }

    async fn perform_op<'a>(&mut self, event: ChangeEvent<'a>) -> Result<(), BridgeError> {
        match event.op {
            cdc_avro::Op::Insert { mut row } => {
                let insert_stmt = self
                    .insert_stmt_cache
                    .get(
                        &self.client,
                        event.rel,
                        &["id", "name", "email"],
                        &self.relation_cache.table_names,
                    )
                    .await
                    .expect("Failed to generate insert statement");

                let email = row.pop().unwrap();
                let name = row.pop().unwrap();
                let id = row.pop().unwrap();

                if let Err(e) = self
                    .client
                    .execute(
                        &insert_stmt.stmt,
                        &[&ToSqlWrapper(id), &ToSqlWrapper(name), &ToSqlWrapper(email)],
                    )
                    .await
                {
                    eprintln!("{:?}", e);
                }
            }
            cdc_avro::Op::Update { old, mut row } => {
                // TODO: how to process updates, should we upsert or not?
                let update_stmt = self
                    .client
                    .prepare("INSERT INTO users (id,name,email) VALUES ($1, $2,$3)")
                    .await
                    .unwrap();

                let email = ToSqlWrapper(row.pop().unwrap());
                let name = ToSqlWrapper(row.pop().unwrap());
                let id = ToSqlWrapper(row.pop().unwrap());

                if let Err(e) = self
                    .client
                    .execute(&update_stmt, &[&id, &name, &email])
                    .await
                {
                    eprintln!("{:?}", e);
                }
            }

            cdc_avro::Op::Delete { old } => {
                let delete_stmt = self
                    .delete_stmt_cache
                    .get(&self.client, event.rel, &self.relation_cache.table_names)
                    .await
                    .expect("Failed to generate insert statement");

                let key_identity = self.relation_cache.identities.get(&event.rel);

                let keys: Vec<ToSqlWrapper> = match key_identity {
                    Some(RelationIdentity::Full) => {
                        old.into_iter().map(|v| ToSqlWrapper(v)).collect()
                    }
                    Some(RelationIdentity::Keys(ids)) => old
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, v)| {
                            if ids.contains(&i) {
                                Some(ToSqlWrapper(v))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    None => return Err(BridgeError::UnknowRelation),
                };

                let keys_ref: Vec<&(dyn ToSql + Sync)> =
                    keys.iter().map(|k| k as &(dyn ToSql + Sync)).collect();

                if let Err(e) = self
                    .client
                    .execute(&delete_stmt.stmt, keys_ref.as_slice())
                    .await
                {
                    eprintln!("{:?}", e);
                }
            }
        }

        Ok(())
    }
}

impl KafkaSink for PostgresSink {
    async fn on_event<'a>(&mut self, event: ChangeEvent<'a>) -> Result<(), String> {
        self.perform_op(event).await.map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn on_relation<'a>(&mut self, relation: Relation<'a>) -> Result<(), String> {
        self.relation_cache.update(relation);

        Ok(())
    }
}

#[derive(Debug)]
struct ToSqlWrapper<'a>(PgValue<'a>);

impl<'a> ToSql for ToSqlWrapper<'a> {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self.0 {
            PgValue::Text(s) => s.to_sql(ty, out),
            PgValue::Int4(n) => n.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self.0 {
            PgValue::Text(s) => s.to_sql_checked(ty, out),
            PgValue::Int4(n) => n.to_sql_checked(ty, out),
        }
    }
}

pub struct RelationCache {
    identities: HashMap<u32, RelationIdentity>,
    table_names: TableNames,
}

impl RelationCache {
    pub fn new() -> Self {
        Self {
            identities: HashMap::new(),
            table_names: TableNames::new(),
        }
    }

    pub fn from_rels<'a>(rels: &HashMap<u32, Relation<'a>>) -> Self {
        let identities = rels
            .iter()
            .map(|(i, v)| (*i, extract_key_identity(&v)))
            .collect();

        Self {
            identities,
            table_names: TableNames::from_rels(&rels),
        }
    }

    pub fn update(&mut self, relation: Relation<'_>) {
        self.identities
            .insert(relation.relation_oid, extract_key_identity(&relation));
        self.table_names
            .insert(relation.relation_oid, relation.name);
    }
}

fn extract_keys_pos(fields: &[Field]) -> HashSet<usize> {
    fields
        .iter()
        .enumerate()
        .fold(HashSet::new(), |mut s, (i, f)| {
            if f.is_key {
                s.insert(i);
            }

            s
        })
}

fn extract_key_identity(rel: &Relation<'_>) -> RelationIdentity {
    match rel.replica_id {
        ReplicaKind::Keys => RelationIdentity::Keys(extract_keys_pos(&rel.fields)),
        ReplicaKind::Row => RelationIdentity::Full,
    }
}

pub enum RelationIdentity {
    Full,
    Keys(HashSet<usize>),
}
