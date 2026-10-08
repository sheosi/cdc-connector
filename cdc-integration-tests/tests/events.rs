use std::time::Duration;

use cdc_integration_tests::{TWO_SECONDS, init_infra};
use serial_test::serial;
use tokio::test;

#[test]
#[serial]
async fn simple_insert() {
    let infra = init_infra().await;

    infra
        .exec_on_source("INSERT INTO users (name, email) VALUES ('e2e', 'e2e@test.com')")
        .await;

    std::thread::sleep(Duration::from_secs(10));

    infra
        .wait_for_sink_row(
            "users",
            "name = 'e2e' AND email = 'e2e@test.com'",
            TWO_SECONDS,
        )
        .await;
}

#[test]
#[serial]
async fn multiple_inserts() {
    let infra = init_infra().await;

    infra
        .exec_on_source(
            r#"BEGIN;
        INSERT INTO users (name, email) VALUES ('a', 'a@x.com');
        INSERT INTO users (name, email) VALUES ('b', 'b@x.com');
        COMMIT;
        "#,
        )
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'b' and email = 'b@x.com'", TWO_SECONDS)
        .await;
}

#[test]
#[serial]
async fn insert_update() {
    let infra = init_infra().await;

    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    infra
        .exec_on_source(
            r#"UPDATE users
        SET email = 'b@x.com'
        WHERE name = 'a'"#,
        )
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'b@x.com'", TWO_SECONDS)
        .await;
}

#[test]
#[serial]
async fn insert_delete() {
    let infra = init_infra().await;

    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    infra
        .exec_on_source(r#"DELETE FROM users WHERE name = 'a'"#)
        .await;

    infra
        .not_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;
}
/*
#[test]
#[serial]
async fn rollback_not_applied() {
    let infra = init_infra().await;

    infra
        .exec_on_source(
            r#"BEGIN;
        INSERT INTO users (name, email) VALUES ('a', 'a@x.com');
        ROLLBACK;"#,
        )
        .await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    infra
        .not_sink_row("users", "name = 'a' AND email = 'a@x.com'")
        .await;
}*/

/*#[test]
#[serial]
async fn rel_msg_pub() {
    let infra = init_infra().await;

    // TODO check for published relation message
}*/

/*#[test]
#[serial]
async fn lsn_checkpoint_persists() {
    let infra = init_infra().await;

    // First make an event
    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    // TODO: Check whether the lsn checkpoint persistss
}*/

/*#[test]
#[serial]
async fn producer_restart_resumes_lsn() {
    let infra = init_infra().await;

    // First insert
    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    // TODO: Check whether the producer resumes from last LSN
}*/

/*#[test]
#[serial]
async fn sink_restart_resumes_offset() {
    let infra = init_infra().await;

    // TODO: Check whether the consumer resumes from last offset
}*/

/*#[test]
#[serial]
async fn check_metrics() {
    let infra = init_infra().await;

    // First insert
    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    // TODO: check :9000/metric cdc_events_produced_total > 0
}*/

/*#[test]
#[serial]
async fn check_ordering() {
    let infra = init_infra().await;

    infra.exec_on_source(r#"
        BEGIN;
        INSERT INTO users (name, email) VALUES ('a', 'a@x.com');
        INSERT INTO users (name, email) VALUES ('b', 'b@x.com');
        INSERT INTO users (name, email) VALUES ('c', 'c@x.com');
        COMMIT;"#).await;

    // TODO: check sink sees them in order
}*/
