use bumpalo::{Bump, collections::CollectIn};
use cdc_sink::TableNames;
use std::collections::HashMap;
use std::fmt::Write;
use tokio_postgres::{Client, Statement};

use crate::RelationCache;

pub struct InsertStatementCache(HashMap<u32, InsertStatement>);

impl InsertStatementCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn get(
        &mut self,
        client: &Client,
        rel: u32,
        rows_table: &HashMap<u32, Vec<String>>,
        table_names: &TableNames,
    ) -> Result<InsertStatement, tokio_postgres::Error> {
        use std::collections::hash_map::Entry;

        match self.0.entry(rel) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let rows: Vec<&str> = rows_table[&rel].iter().map(|s| s.as_str()).collect();
                Ok(e.insert(
                    InsertStatement::new(client, table_names.get(rel).unwrap(), &rows).await?,
                )
                .clone())
            }
        }
    }
}

pub struct DeleteStatementCache(HashMap<u32, DeleteStatement>);

impl DeleteStatementCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    // Here, we assume a relation is a db
    pub async fn get(
        &mut self,
        client: &Client,
        relation: u32,
        table_names: &TableNames,
    ) -> Result<DeleteStatement, tokio_postgres::Error> {
        use std::collections::hash_map::Entry;

        match self.0.entry(relation) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => Ok(e
                .insert(DeleteStatement::new(client, table_names.get(relation).unwrap()).await?)
                .clone()),
        }
    }
}

#[derive(Clone)]
pub struct InsertStatement {
    pub stmt: Statement,
}

impl InsertStatement {
    pub async fn new(
        client: &tokio_postgres::Client,
        table: &str,
        rows: &[&str],
    ) -> Result<Self, tokio_postgres::Error> {
        let stmt = client.prepare(&Self::gen_str(table, rows)).await?;

        Ok(InsertStatement { stmt })
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
                write!(&mut stmt_str, "${},", i + 1).expect("");
            } else {
                write!(&mut stmt_str, "${}", i + 1).expect("");
            }
        }

        stmt_str.push(')');

        stmt_str
    }
}

pub struct UpsertStatementCache(HashMap<u32, UpsertStatement>);

impl UpsertStatementCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn get(
        &mut self,
        client: &Client,
        rel: u32,
        arena: &Bump,
        relation_cache: &RelationCache,
    ) -> Result<UpsertStatement, tokio_postgres::Error> {
        use bumpalo::collections::Vec;
        use std::collections::hash_map::Entry;

        match self.0.entry(rel) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let rows: Vec<'_, &str> = relation_cache.fields[&rel]
                    .iter()
                    .map(|s| s.as_str())
                    .collect_in(arena);

                let keys: Vec<'_, &str> = relation_cache.keys[&rel]
                    .iter()
                    .map(|s| s.as_str())
                    .collect_in(arena);

                Ok(e.insert(
                    UpsertStatement::new(
                        client,
                        relation_cache.table_names.get(rel).unwrap(),
                        &rows,
                        &keys,
                    )
                    .await?,
                )
                .clone())
            }
        }
    }
}

#[derive(Clone)]
pub struct UpsertStatement {
    pub stmt: Statement,
}

impl UpsertStatement {
    pub async fn new(
        client: &tokio_postgres::Client,
        table: &str,
        rows: &[&str],
        keys: &[&str],
    ) -> Result<Self, tokio_postgres::Error> {
        let stmt = client.prepare(&Self::gen_str(table, rows, keys)).await?;

        Ok(UpsertStatement { stmt })
    }

    fn gen_str(table: &str, rows: &[&str], keys: &[&str]) -> String {
        let mut stmt_str = InsertStatement::gen_str(table, rows);
        stmt_str.push_str("ON CONFLICT (");

        for (i, r) in keys.iter().enumerate() {
            stmt_str.push_str(r); // TODO SET PK

            if i < keys.len() - 1 {
                stmt_str.push_str(", ");
            }
        }

        stmt_str.push_str(") DO UPDATE SET ");

        for (i, c) in rows.iter().enumerate() {
            stmt_str.push_str(c);
            stmt_str.push_str(" = EXCLUDED.");
            stmt_str.push_str(c);

            if i < rows.len() - 1 {
                stmt_str.push_str(",");
            }
        }

        stmt_str
    }
}

#[derive(Clone)]
pub struct DeleteStatement {
    pub stmt: Statement,
}

impl DeleteStatement {
    pub async fn new(
        client: &tokio_postgres::Client,
        table: &str,
    ) -> Result<Self, tokio_postgres::Error> {
        let stmt = client.prepare(&DeleteStatement::gen_str(table)).await?;

        Ok(DeleteStatement { stmt })
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

    use crate::statements::{DeleteStatement, InsertStatement};

    #[test]
    fn simple_insert_str() {
        let insert_str = InsertStatement::gen_str("users", &["id", "name", "email"]);
        let res_str = "INSERT INTO users (id,name,email) VALUES ($1,$2,$3)";

        assert_eq!(insert_str, res_str);
    }

    #[test]
    fn simple_delete_str() {
        let delete_str = DeleteStatement::gen_str("users");
        let res_str = "DELETE FROM users WHERE id = $1";

        assert_eq!(delete_str, res_str);
    }
}
