use cdc_avro::ChangeEvent;
use std::collections::HashMap;
use tokio_postgres::Statement;

#[tokio::main]
async fn main() {
    let sink = PostgresSink::new();
    while let Some(event) = cdc_sink::consume_from_kafka().await {
        if let Err(e) = sink.perform_op(event).await {
            println!("{:?}", e);
        }
    }
}

pub struct PostgresSink {
    client: tokio_postgres::Client,
    pk_cache: HashMap<String, Vec<String>>,
}

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
                write!(stmt_str, "${},", i).expect("");
            } else {
                write!(stmt_str, "${},", i).expect("");
            }
        }

        stmt_str.push_str(");");

        stmt_str
    }
}

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
    fn new() -> Self {
        Self {
            client: tokio_postgres::Client,
            pk_cache: HashMap::new(),
        }
    }

    async fn perform_op(&self, event: ChangeEvent) -> Result<(), ()> {
        match event.op {
            cdc_avro::Op::Insert { row } => {
                let insert_stmt =
                    InsertStatement::new(&self.client, "users", &["id", "name", "email"]).await;

                self.client
                    .execute(
                        &insert_stmt.stmt,
                        &[&row["id"], &row["name"], &row["email"]],
                    )
                    .await;
            }
            cdc_avro::Op::Update { key, row } => {
                let update_stmt = self
                    .client
                    .prepare("INSERT INTO users (id,name,email) VALUES ($1, $2,$3)")
                    .await
                    .unwrap();

                self.client
                    .execute(&update_stmt, &[&row["id"], &row["name"], &row["user"]])
                    .await;
            }
            cdc_avro::Op::Delete { key } => {
                let delete_stmt = DeleteStatement::new(&self.client, "users").await;

                self.client.execute(&delete_stmt.stmt, &[&key]).await;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tokio_postgres::types::IsNull::Yes;

    use crate::{DeleteStatement, InsertStatement};

    #[test]
    fn simple_insert_str() {
        let insert_str = InsertStatement::gen_str("users", &["id", "name", "email"]);
        let res_str = "INSERT INTO users (id,name,email,) VALUES ($1,$2,$3,)";

        assert_eq!(insert_str, res_str);
    }

    #[test]
    fn simple_delet_str() {
        let delete_str = DeleteStatement::gen_str("users");
        let res_str = "DELETE from users where id = $1";

        assert_eq!(delete_str, res_str);
    }
}
