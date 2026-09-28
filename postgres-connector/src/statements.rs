use bumpalo::Bump;
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
            Entry::Vacant(e) => Ok(e
                .insert(
                    InsertStatement::new(client, table_names.get(rel).unwrap(), &rows_table[&rel])
                        .await?,
                )
                .clone()),
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
        relation_cache: &RelationCache,
        arena: &Bump,
    ) -> Result<DeleteStatement, tokio_postgres::Error> {
        use std::collections::hash_map::Entry;

        match self.0.entry(relation) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let keys = relation_cache.keys.get(&relation).unwrap();
                Ok(e.insert(
                    DeleteStatement::new(
                        client,
                        relation_cache.table_names.get(relation).unwrap(),
                        keys.as_slice(),
                    )
                    .await?,
                )
                .clone())
            }
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
        rows: &[String],
    ) -> Result<Self, tokio_postgres::Error> {
        let stmt = client.prepare(&Self::gen_str(table, rows)).await?;

        Ok(InsertStatement { stmt })
    }

    fn gen_str(table: &str, rows: &[String]) -> String {
        let mut stmt_str = String::with_capacity(25 + table.len() + rows.len() * 10);
        stmt_str.push_str("INSERT INTO ");
        stmt_str.push_str(table);
        stmt_str.push_str(" (");
        for (i, r) in rows.iter().enumerate() {
            stmt_str.push_str(&r);

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
        relation_cache: &RelationCache,
        arena: &Bump,
    ) -> Result<UpsertStatement, tokio_postgres::Error> {
        use std::collections::hash_map::Entry;

        match self.0.entry(rel) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => Ok(e
                .insert(
                    UpsertStatement::new(
                        client,
                        relation_cache.table_names.get(rel).unwrap(),
                        &relation_cache.fields[&rel],
                        &relation_cache.keys[&rel],
                    )
                    .await?,
                )
                .clone()),
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
        rows: &[String],
        keys: &[String],
    ) -> Result<Self, tokio_postgres::Error> {
        let stmt = client.prepare(&Self::gen_str(table, rows, keys)).await?;

        Ok(UpsertStatement { stmt })
    }

    fn gen_str(table: &str, rows: &[String], keys: &[String]) -> String {
        let mut stmt_str = InsertStatement::gen_str(table, rows);
        stmt_str.reserve(14 + rows.len() * 15);
        stmt_str.push_str(" ON CONFLICT (");

        for (i, r) in keys.iter().enumerate() {
            stmt_str.push_str(r); // TODO SET PK

            if i < keys.len() - 1 {
                stmt_str.push_str(", ");
            }
        }

        if rows.len() != keys.len() {
            stmt_str.push_str(") DO UPDATE SET ");

            for (i, c) in rows.iter().enumerate() {
                if keys.contains(c) {
                    continue;
                }

                stmt_str.push_str(c);
                stmt_str.push_str(" = EXCLUDED.");
                stmt_str.push_str(c);

                if i < rows.len() - 1 {
                    stmt_str.push_str(",");
                }
            }
        } else {
            stmt_str.push_str(") DO NOTHING")
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
        keys: &[String],
    ) -> Result<Self, tokio_postgres::Error> {
        let stmt = client
            .prepare(&DeleteStatement::gen_str(table, keys))
            .await?;

        Ok(DeleteStatement { stmt })
    }

    fn gen_str(table: &str, keys: &[String]) -> String {
        let mut stmt_str = "DELETE FROM ".to_string();
        stmt_str.push_str(table);
        stmt_str.push_str(" WHERE ");

        for (i, key) in keys.iter().enumerate() {
            if i < keys.len() - 1 {
                write!(&mut stmt_str, "{} = ${} AND ", key, i + 1).expect("");
            } else {
                write!(&mut stmt_str, "{} = ${}", key, i + 1).expect("");
            }
        }

        stmt_str
    }
}

#[cfg(test)]
mod test {

    use crate::statements::{DeleteStatement, InsertStatement, UpsertStatement};

    #[test]
    fn simple_insert_str() {
        let insert_str = InsertStatement::gen_str(
            "users",
            &["id".to_string(), "name".to_string(), "email".to_string()],
        );
        let res_str = "INSERT INTO users (id,name,email) VALUES ($1,$2,$3)";

        assert_eq!(insert_str, res_str);
    }

    #[test]
    fn simple_upsert_str() {
        let upsert_str = UpsertStatement::gen_str(
            "users",
            &["id".to_string(), "name".to_string(), "email".to_string()],
            &["id".to_string()],
        );
        let res_str = "INSERT INTO users (id,name,email) VALUES ($1,$2,$3) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name,email = EXCLUDED.email";

        assert_eq!(upsert_str, res_str);
    }

    #[test]
    fn upsert_no_values() {
        let upsert_str =
            UpsertStatement::gen_str("users", &["id".to_string()], &["id".to_string()]);
        let res_str = "INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING";

        assert_eq!(upsert_str, res_str);
    }

    #[test]
    fn simple_delete_str() {
        let delete_str = DeleteStatement::gen_str("users", &["id".to_string()]);
        let res_str = "DELETE FROM users WHERE id = $1";

        assert_eq!(delete_str, res_str);
    }

    #[test]
    fn multiple_delete_str() {
        let delete_str = DeleteStatement::gen_str("users", &["id".to_string(), "name".to_string()]);
        let res_str = "DELETE FROM users WHERE id = $1 AND name = $2";

        assert_eq!(delete_str, res_str);
    }
}
