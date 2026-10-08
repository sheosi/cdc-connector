use bumpalo::{Bump, collections::CollectIn};
use cdc_avro::{
    Field, PgValue, Relation, ReplicaKind,
    owned::{ChangeEvent, Op},
};
use cdc_sink::{KafkaConfig, KafkaSink, MetricsConfig, SinkError, TableNames};
use config::Config;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{
    Connection, NoTls,
    types::{IsNull, ToSql},
};
use tracing::debug;

use crate::statements::{DeleteStatementCache, UpsertStatementCache};

mod statements;

#[tokio::main]
async fn main() {
    cdc_sink::init_logs();

    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("config/postgres-connector").required(false))
        .add_source(config::Environment::with_prefix("PG_CONN").separator("_"))
        .build()
        .expect("Failed to find postgres-connect config")
        .try_deserialize()
        .expect("postgres-connect config is malformed");

    cdc_sink::init_metrics(config.metrics);

    let mut arena = Bump::with_capacity(2048);

    let kafka = config.kafka.connect().await;
    let relations = kafka
        .load_relations(&arena)
        .await
        .expect("Failed to load relations");

    arena.reset();

    kafka
        .consume_from_kafka(
            PostgresSink::new(config.postgres, relations)
                .await
                .expect("Failed to connect to postgres"),
        )
        .await;
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
    port: u16,
}

impl PostgresConfig {
    fn to_postgres_string(&self) -> String {
        format!(
            "host={} user={} password={} port={}",
            self.host, self.user, self.password, self.port
        )
    }
}

pub struct PostgresSink {
    client: tokio_postgres::Client,
    _conn: ConnTask,
    upsert_stmt_cache: UpsertStatementCache,
    delete_stmt_cache: DeleteStatementCache,
    relation_cache: RelationCache,
}

impl PostgresSink {
    async fn new(
        config: PostgresConfig,
        relations: HashMap<u32, Relation>,
    ) -> Result<Self, tokio_postgres::Error> {
        let (clt, conn) = tokio_postgres::connect(&config.to_postgres_string(), NoTls).await?;

        Ok(Self {
            client: clt,
            _conn: ConnTask::spawn(conn),
            upsert_stmt_cache: UpsertStatementCache::new(),
            delete_stmt_cache: DeleteStatementCache::new(),
            relation_cache: RelationCache::from_rels(&relations),
        })
    }
}

impl KafkaSink for PostgresSink {
    async fn on_event<'a>(
        &mut self,
        event: ChangeEvent<'a>,
        arena: &Bump,
    ) -> Result<(), SinkError> {
        debug!("Got event: {:?}", &event);
        match event.op {
            Op::Insert { row } => {
                let insert_stmt = self
                    .upsert_stmt_cache
                    .get(&self.client, event.rel, &self.relation_cache, arena)
                    .await?;

                // Confine this block to confine the reference to the arena
                // so that it can be reset later.
                {
                    let keys: bumpalo::collections::Vec<'_, ToSqlWrapper> =
                        row.into_iter().map(|v| ToSqlWrapper(v)).collect_in(arena);

                    self.client
                        .execute(&insert_stmt.stmt, &extract_keys_ref(&arena, &keys))
                        .await
                        .map_err(|e| SinkError::Platform(e.to_string()))?;
                }
            }
            Op::Update { old: _, row } => {
                let update_stmt = self
                    .upsert_stmt_cache
                    .get(&self.client, event.rel, &self.relation_cache, &arena)
                    .await?;

                // Confine this block to confine the reference to the arena
                // so that it can be reset later.
                {
                    let keys = extract_keys(&arena, row);

                    self.client
                        .execute(&update_stmt.stmt, &extract_keys_ref(&arena, &keys))
                        .await
                        .map_err(|e| SinkError::Platform(e.to_string()))?;
                }
            }

            Op::Delete { old } => {
                let delete_stmt = self
                    .delete_stmt_cache
                    .get(&self.client, event.rel, &self.relation_cache, &arena)
                    .await?;

                let key_identity = self.relation_cache.identities.get(&event.rel);

                // Confine this block to confine the reference to the arena
                // so that it can be reset later.
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
                        None => return Err(SinkError::UnknownRelation(event.rel)),
                    };

                    self.client
                        .execute(
                            &delete_stmt.stmt,
                            &extract_keys_ref(&arena, &keys).as_slice(),
                        )
                        .await
                        .map_err(|e| SinkError::Platform(e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    async fn on_relation(&mut self, relation: Relation) -> Result<(), SinkError> {
        tracing::info!("Got relation");
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

    pub fn from_rels<'a>(rels: &HashMap<u32, Relation>) -> Self {
        let identities = rels
            .iter()
            .map(|(i, v)| (*i, extract_key_identity(&v)))
            .collect();

        let fields = rels
            .iter()
            .map(|(i, v)| (*i, extract_fields_names(v)))
            .collect();

        let keys = rels
            .iter()
            .map(|(i, v)| (*i, extract_keys_rel(v)))
            .collect();

        Self {
            identities,
            table_names: TableNames::from_rels(&rels),
            fields,
            keys,
        }
    }

    pub fn update(&mut self, relation: Relation) {
        self.identities
            .insert(relation.relation_oid, extract_key_identity(&relation));
        self.fields
            .insert(relation.relation_oid, extract_fields_names(&relation));
        self.keys
            .insert(relation.relation_oid, extract_keys_rel(&relation));
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

fn extract_key_identity(rel: &Relation) -> RelationIdentity {
    match rel.replica_id {
        ReplicaKind::Keys => RelationIdentity::Keys(extract_keys_pos(&rel.fields)),
        ReplicaKind::Row => RelationIdentity::Full,
    }
}

fn extract_fields_names(rel: &Relation) -> Vec<String> {
    rel.fields.iter().map(|f| f.name.clone()).collect()
}

fn extract_keys_rel(rel: &Relation) -> Vec<String> {
    rel.fields
        .iter()
        .filter_map(|f| if f.is_key { Some(f.name.clone()) } else { None })
        .collect()
}

fn extract_keys<'a>(
    arena: &'a Bump,
    row: Vec<PgValue<'a>>,
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

struct ConnTask(tokio::task::JoinHandle<()>);

impl ConnTask {
    fn spawn<S, T>(conn: Connection<S, T>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });

        Self(handle)
    }
}

impl Drop for ConnTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
