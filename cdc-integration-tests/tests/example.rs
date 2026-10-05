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

    infra
        .wait_for_sink_row(
            "users",
            "name = 'e2e' AND email = 'e2e@test.com",
            TWO_SECONDS,
        )
        .await;
}

