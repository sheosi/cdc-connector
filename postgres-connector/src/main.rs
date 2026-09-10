use cdc_avro::ChangeEvent;
use cdc_sink::KafkaSink;
use std::collections::HashMap;
use std::fmt::Write;
use tokio_postgres::{Client, Connection, NoTls, Socket, Statement, tls::NoTlsStream};

#[tokio::main]
async fn main() {
    cdc_sink::consume_from_kafka(PostgresSink::new().await).await;
}

pub struct Config {}

pub struct PostgresSink {
    client: tokio_postgres::Client,
    conn: Connection<Socket, NoTlsStream>,
    pk_cache: HashMap<String, Vec<String>>,
    insert_stmt_cache: InsertStatementCache,
    delete_stmt_cache: DeleteStatementCache,
}

struct InsertStatementCache(HashMap<String, InsertStatement>);

impl InsertStatementCache {
    fn new() -> Self {
        Self(HashMap::new())
    }

    async fn get(&mut self, client: &Client, table: &str, rows: &[&str]) -> InsertStatement {
        let key = format!("{}{:?}", table, rows);

        use std::collections::hash_map::Entry;

        match self.0.entry(key) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e
                .insert(InsertStatement::new(client, table, rows).await)
                .clone(),
        }
    }
}

struct DeleteStatementCache(HashMap<String, DeleteStatement>);

impl DeleteStatementCache {
    fn new() -> Self {
        Self(HashMap::new())
    }

    async fn get(&mut self, client: &Client, table: &str) -> DeleteStatement {
        use std::collections::hash_map::Entry;

        match self.0.entry(table.to_string()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(DeleteStatement::new(client, table).await).clone(),
        }
    }
}

#[derive(Clone)]
pub struct InsertStatement {
    stmt: Statement,
}

impl InsertStatement {
    pub async fn new(client: &tokio_postgres::Client, table: &str, rows: &[&str]) -> Self {
        let stmt = client.prepare(&Self::gen_str(table, rows)).await.unwrap();

        InsertStatement { stmt }
    }

    fn gen_str(table: &str, rows: &[&str]) -> String {
        let mut stmt_str = String::with_capacity(25 + table.len() + rows.len() * 10);
        stmt_str.push_str("INSERT INTO ");
        stmt_str.push_str(table);
        stmt_str.push_str(" (");
        for (i, r) in rows.iter().enumerate() {
            stmt_str.push_str(*r);

            if i < rows.len() - 1 {
                stmt_str.push(',');
            }
        }
        stmt_str.push_str(") VALUES (");

        for i in 0..rows.len() {
            if i < rows.len() - 1 {
                write!(&mut stmt_str, "${},", i).expect("");
            } else {
                write!(&mut stmt_str, "${}", i).expect("");
            }
        }

        stmt_str.push(')');

        stmt_str
    }
}

#[derive(Clone)]
pub struct DeleteStatement {
    stmt: Statement,
}

impl DeleteStatement {
    pub async fn new(client: &tokio_postgres::Client, table: &str) -> Self {
        let stmt = client
            .prepare(&DeleteStatement::gen_str(table))
            .await
            .unwrap();

        DeleteStatement { stmt }
    }

    fn gen_str(table: &str) -> String {
        let mut stmt_str = "DELETE FROM ".to_string();
        stmt_str.push_str(table);
        stmt_str.push_str(" WHERE id = $1");

        stmt_str
    }
}

impl PostgresSink {
    async fn new() -> Self {
        let (clt, conn) = tokio_postgres::connect("host=", NoTls).await.unwrap();

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

#[cfg(test)]
mod test {

    use crate::{DeleteStatement, InsertStatement};

    #[test]
    fn simple_insert_str() {
        let insert_str = InsertStatement::gen_str("users", &["id", "name", "email"]);
        let res_str = "INSERT INTO users (id,name,email) VALUES ($1,$2,$3)";

        assert_eq!(insert_str, res_str);
    }

    #[test]
    fn simple_delet_str() {
        let delete_str = DeleteStatement::gen_str("users");
        let res_str = "DELETE FROM users WHERE id = $1";

        assert_eq!(delete_str, res_str);
    }
}
