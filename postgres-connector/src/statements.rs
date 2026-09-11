use std::collections::HashMap;
use std::fmt::Write;
use tokio_postgres::{Client, Statement};

pub struct InsertStatementCache(HashMap<String, InsertStatement>);

impl InsertStatementCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn get(&mut self, client: &Client, table: &str, rows: &[&str]) -> InsertStatement {
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

pub struct DeleteStatementCache(HashMap<String, DeleteStatement>);

impl DeleteStatementCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn get(&mut self, client: &Client, table: &str) -> DeleteStatement {
        use std::collections::hash_map::Entry;

        match self.0.entry(table.to_string()) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => e.insert(DeleteStatement::new(client, table).await).clone(),
        }
    }
}

#[derive(Clone)]
pub struct InsertStatement {
    pub stmt: Statement,
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
    pub stmt: Statement,
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
