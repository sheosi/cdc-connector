use bumpalo::{Bump, collections::CollectIn};
use cdc_avro::{ChangeEvent, Field, PgValue, Relation, ReplicaKind};
use cdc_sink::{KafkaConfig, KafkaSink, MetricsConfig, TableNames};
use config::Config;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tokio_postgres::{
    Connection, NoTls, Socket,
    tls::NoTlsStream,
    types::{IsNull, ToSql},
};

use crate::statements::{DeleteStatementCache, InsertStatementCache, UpsertStatementCache};

mod statements;

#[tokio::main]
async fn main() {
    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("postgres-connector"))
        .build()
        .expect("Failed to find postgres-connect config")
        .try_deserialize()
        .expect("postgres-connect config is malformed");

    cdc_sink::init_metrics(config.metrics);

    let arena = Bump::with_capacity(2048);

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

    #[serde(default)]
    metrics: MetricsConfig,
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
    _conn: Connection<Socket, NoTlsStream>,
    insert_stmt_cache: InsertStatementCache,
    upsert_stmt_cache: UpsertStatementCache,
    delete_stmt_cache: DeleteStatementCache,
    relation_cache: RelationCache,
    arena: Bump,
}

impl PostgresSink {
    async fn new(
        config: PostgresConfig,
        relations: HashMap<u32, Relation<'_>>,
    ) -> Result<Self, tokio_postgres::Error> {
        let (clt, _conn) = tokio_postgres::connect(&config.to_postgres_string(), NoTls).await?;

        Ok(Self {
            client: clt,
            _conn,
            insert_stmt_cache: InsertStatementCache::new(),
            upsert_stmt_cache: UpsertStatementCache::new(),
            delete_stmt_cache: DeleteStatementCache::new(),
            relation_cache: RelationCache::from_rels(&relations),
            arena: Bump::with_capacity(2048),
        })
    }

    async fn perform_op<'a>(&'a mut self, event: ChangeEvent<'a>) -> Result<(), BridgeError> {
        match event.op {
            cdc_avro::Op::Insert { row } => {
                let insert_stmt = self
                    .insert_stmt_cache
                    .get(
                        &self.client,
                        event.rel,
                        &self.relation_cache.fields,
                        &self.relation_cache.table_names,
                    )
                    .await
                    .expect("Failed to generate insert statement");
                {
                    let keys: bumpalo::collections::Vec<'_, ToSqlWrapper> = row
                        .into_iter()
                        .map(|v| ToSqlWrapper(v))
                        .collect_in(&self.arena);

                    if let Err(e) = self
                        .client
                        .execute(&insert_stmt.stmt, &extract_keys_ref(&self.arena, &keys))
                        .await
                    {
                        eprintln!("{:?}", e);
                    }
                }

                self.arena.reset();
            }
            cdc_avro::Op::Update { old, row } => {
                let update_stmt = self
                    .upsert_stmt_cache
                    .get(&self.client, event.rel, &self.arena, &self.relation_cache)
                    .await
                    .unwrap();

                let keys = extract_keys(&self.arena, row);

                if let Err(e) = self
                    .client
                    .execute(&update_stmt.stmt, &extract_keys_ref(&self.arena, &keys))
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

                {
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

                    if let Err(e) = self
                        .client
                        .execute(
                            &delete_stmt.stmt,
                            &extract_keys_ref(&self.arena, &keys).as_slice(),
                        )
                        .await
                    {
                        eprintln!("{:?}", e);
                    }
                }

                self.arena.reset()
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
    fields: HashMap<u32, Vec<String>>,
    keys: HashMap<u32, Vec<String>>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self {
            identities: HashMap::new(),
            table_names: TableNames::new(),
            fields: HashMap::new(),
            keys: HashMap::new(),
        }
    }

    pub fn from_rels<'a>(rels: &HashMap<u32, Relation<'a>>) -> Self {
        let identities = rels
            .iter()
            .map(|(i, v)| (*i, extract_key_identity(&v)))
            .collect();

        let fields = rels
            .iter()
            .map(|(i, v)| (*i, v.fields.iter().map(|f| f.name.clone()).collect()))
            .collect();

        let keys = rels
            .iter()
            .map(|(i, v)| {
                (
                    *i,
                    v.fields
                        .iter()
                        .filter_map(|f| if f.is_key { Some(f.name.clone()) } else { None })
                        .collect(),
                )
            })
            .collect();

        Self {
            identities,
            table_names: TableNames::from_rels(&rels),
            fields,
            keys,
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

fn extract_keys<'a>(
    arena: &'a Bump,
    row: bumpalo::collections::Vec<'a, PgValue<'a>>,
) -> bumpalo::collections::Vec<'a, ToSqlWrapper<'a>> {
    row.into_iter().map(|v| ToSqlWrapper(v)).collect_in(arena)
}

fn extract_keys_ref<'a>(
    arena: &'a Bump,
    keys: &'a [ToSqlWrapper],
) -> bumpalo::collections::Vec<'a, &'a (dyn ToSql + Sync)> {
    keys.iter()
        .map(|k| k as &(dyn ToSql + Sync))
        .collect_in(arena)
}

pub enum RelationIdentity {
    Full,
    Keys(HashSet<usize>),
}
