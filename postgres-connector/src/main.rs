use cdc_avro::ChangeEvent;
use cdc_sink::{KafkaConfig, KafkaSink};
use config::Config;
use serde::Deserialize;
use std::collections::HashMap;
use tokio_postgres::{Connection, NoTls, Socket, tls::NoTlsStream};

use crate::statements::{DeleteStatementCache, InsertStatementCache};

mod statements;

#[tokio::main]
async fn main() {
    let config: BridgeConfig = Config::builder()
        .add_source(config::File::with_name("feldera-connector"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    cdc_sink::consume_from_kafka(config.kafka, PostgresSink::new(config.postgres).await).await;
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
}

impl PostgresSink {
    async fn new(config: PostgresConfig) -> Self {
        let (clt, conn) = tokio_postgres::connect(&config.to_postgres_string(), NoTls)
            .await
            .unwrap();

        Self {
            client: clt,
            conn,
            pk_cache: HashMap::new(),
            insert_stmt_cache: InsertStatementCache::new(),
            delete_stmt_cache: DeleteStatementCache::new(),
        }
    }

    async fn perform_op(&mut self, event: ChangeEvent) -> Result<(), ()> {
        match event.op {
            cdc_avro::Op::Insert { row } => {
                let insert_stmt = self
                    .insert_stmt_cache
                    .get(
                        &self.client,
                        &event.table,
                        row.keys()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .as_slice(),
                    )
                    .await;

                if let Err(e) = self
                    .client
                    .execute(
                        &insert_stmt.stmt,
                        &[&row["id"], &row["name"], &row["email"]],
                    )
                    .await
                {
                    eprintln!("{:?}", e);
                }
            }
            cdc_avro::Op::Update { key, row } => {
                let update_stmt = self
                    .client
                    .prepare("INSERT INTO users (id,name,email) VALUES ($1, $2,$3)")
                    .await
                    .unwrap();

                if let Err(e) = self
                    .client
                    .execute(&update_stmt, &[&row["id"], &row["name"], &row["user"]])
                    .await
                {
                    eprintln!("{:?}", e);
                }
            }
            cdc_avro::Op::Delete { key } => {
                let delete_stmt = self.delete_stmt_cache.get(&self.client, &event.table).await;

                if let Err(e) = self.client.execute(&delete_stmt.stmt, &[&key]).await {
                    eprintln!("{:?}", e);
                }
            }
        }

        Ok(())
    }
}

impl KafkaSink for PostgresSink {
    async fn on_event(&mut self, event: ChangeEvent) -> Result<(), ()> {
        self.perform_op(event).await.expect("");

        Ok(())
    }
}
