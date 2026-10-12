use std::time::Duration;

use cdc_integration_tests::{InfraOptions, TWO_SECONDS, get_metrics_for, init_infra, read_lsn};
use serial_test::serial;
use tokio::test;

#[test]
#[serial]
async fn simple_insert() {
    let infra = init_infra().await;

    infra
        .exec_on_source("INSERT INTO users (name, email) VALUES ('e2e', 'e2e@test.com')")
        .await;

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

    infra
        .exec_on_source(r#"DELETE FROM users WHERE name = 'a'"#)
        .await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    infra
        .not_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;
}

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
        .not_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;
}

#[test]
#[serial]
async fn rel_msg_pub() {
    let infra = init_infra().await;

    std::thread::sleep(Duration::from_secs(10));

    if infra
        .postgres_connector
        .stdout_lines()
        .iter()
        .find(|l| l.contains("Got relation"))
        .is_none()
    {
        panic!("Relation not found");
    }
}

#[test]
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

    assert!(read_lsn().await.expect("Failed to get LSN") > 1);
}

#[test]
#[serial]
async fn producer_restart_resumes_lsn() {
    let mut infra = init_infra().await;

    // First insert
    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    assert!(read_lsn().await.expect("Failed to get LSN") > 1);

    // Clear cdc-producer's output
    let _ = infra.cdc_producer.stdout_lines();

    infra.restart_producer().await;

    let is_zero = infra
        .cdc_producer
        .stdout_lines()
        .into_iter()
        .any(|l| l.contains("starting_lsn=0"));

    assert!(!is_zero);
}

#[test]
#[serial]
async fn sink_restart_resumes_offset() {
    let mut infra = init_infra().await;

    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('a', 'a@x.com');"#)
        .await;

    infra
        .wait_for_sink_row("users", "name = 'a' AND email = 'a@x.com'", TWO_SECONDS)
        .await;

    // Clear postgres' connector out
    let _ = infra.postgres_connector.stdout_lines();

    infra.restart_connector().await;

    // Insert B
    infra
        .exec_on_source(r#"INSERT INTO users (name, email) VALUES ('b', 'b@x.com');"#)
        .await;

    // Wait for B
    infra
        .wait_for_sink_row("users", "name = 'b' AND email = 'b@x.com'", TWO_SECONDS)
        .await;

    let new_lines = infra.postgres_connector.stdout_lines().join("\n");

    assert!(
        new_lines.contains("b@x.com"),
        "Connector did not process B after restart"
    );

    assert!(
        !new_lines.contains("a@x.com"),
        "Connector reprocessed A from an old offset after restart"
    );
}

#[test]
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

    let cdc_prod_metrics = get_metrics_for(9001)
        .await
        .expect("Failed to get cdc-prod metrics");

    assert!(!cdc_prod_metrics.is_empty());

    let pg_conn_metrics = get_metrics_for(9002)
        .await
        .expect("Failed to pg-conn metrics");

    assert!(!pg_conn_metrics.is_empty());
}

#[test]
#[serial]
async fn check_ordering() {
    let infra = init_infra().await;

    infra
        .exec_on_source(
            r#"
        BEGIN;
        INSERT INTO users (name, email) VALUES ('a', 'a@x.com');
        INSERT INTO users (name, email) VALUES ('b', 'b@x.com');
        INSERT INTO users (name, email) VALUES ('c', 'c@x.com');
        COMMIT;"#,
        )
        .await;

    std::thread::sleep(Duration::from_secs(3));

    // In which lines those are found
    struct Lines {
        a: Option<usize>,
        b: Option<usize>,
        c: Option<usize>,
    }

    let lines = infra
        .postgres_connector
        .stdout_lines()
        .iter()
        .enumerate()
        .fold(
            Lines {
                a: None,
                b: None,
                c: None,
            },
            |mut lines, (i, line)| {
                if line.contains("a@x.com") {
                    lines.a = Some(i);
                }

                if line.contains("b@x.com") {
                    lines.b = Some(i);
                }

                if line.contains("c@x.com") {
                    lines.c = Some(i);
                }

                lines
            },
        );

    let a = lines.a.expect("a@x.com not found");
    let b = lines.b.expect("b@x.com not found");
    let c = lines.c.expect("c@x.com not found");

    if a > b {
        panic!("B came after A")
    }
    if b > c {
        panic!("C came after B")
    }
}
