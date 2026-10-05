use std::{
    process::{Child, Command},
    time::{Duration, Instant},
};
use testcontainers::{ContainerAsync, ImageExt, core::ContainerPort, runners::AsyncRunner};
use testcontainers_modules::{kafka::Kafka, postgres::Postgres};
use tokio::{join, process::Command as AsyncCommand};
use tokio_postgres::{Client, Config, Connection, NoTls, Socket, tls::NoTlsStream};

fn build_crate(name: &str) -> AsyncCommand {
    let mut cmd = AsyncCommand::new("cargo");
    cmd.args(["build", "--bin", name]).current_dir("..");
    cmd
}

pub struct Infra {
    _kafka: ContainerAsync<Kafka>,
    _src_postgres: ContainerAsync<Postgres>,
    src_client: tokio_postgres::Client,
    _src_conn: tokio_postgres::Connection<Socket, NoTlsStream>,
    _sink_postgres: ContainerAsync<Postgres>,
    sink_client: tokio_postgres::Client,
    _sink_conn: tokio_postgres::Connection<Socket, NoTlsStream>,
    _cdc_producer: CdcProducer,
    _postgres_connector: PostgresConnector,
}

impl Infra {
    pub async fn wait_for_sink_row(
        &self,
        table: &str,
        where_clause: &str,
        timeout_duration: Duration,
    ) -> bool {
        let sql = format!("SELECT 1 F   ROM {} WHERE {}", table, where_clause);
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout_duration {
                return false;
            }

            match self
                .sink_client
                .query_opt(&sql, &[])
                .await
                .expect("Failed sink query")
            {
                Some(_) => return true,
                None => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }

    pub async fn exec_on_source(&self, query: &str) {
        self.src_client
            .execute(query, &[])
            .await
            .expect("Failed to perform source statement");
    }
}

pub struct CdcProducer {
    child: Child,
}

impl CdcProducer {
    pub fn start() -> Self {
        let child = Command::new("../target/debug/cdc-producer")
            .env("CDC_PROD_WILL_CONNECT_TO_FELDERA", "false")
            .env("CDC_PROD_POSTGRES_HOST", "localhost")
            .env("CDC_PROD_POSTGRES_USER", "cdc")
            .env("CDC_PROD_POSTGRES_PASSWORD", "cdc")
            .env("CDC_PROD_POSTGRES_SLOTNAME", "cdc_slot")
            .env("CDC_PROD_POSTGRES_DBNAME", "cdc")
            .env("CDC_PROD_POSTGRES_PUBLICATION", "cdc_pub")
            .env("CDC_PROD_POSTGRES_PORT", "5400")
            .env("CDC_PROD_KAFKA_BROKERS", "localhost:9092")
            .env("CDC_PROD_KAFKA_TOPIC", "example-topic")
            .env("CDC_PROD_KAFKA_KEY", "default")
            .env("CDC_PROD_POSTGRESS_DBNAME", "cdc")
            .spawn()
            .expect("Failed to spawn cdc-producer");

        Self { child }
    }
}

impl Drop for CdcProducer {
    fn drop(&mut self) {
        self.child
            .kill()
            .expect("Failed to kill cdc-producer child")
    }
}

pub struct PostgresConnector {
    child: Child,
}

impl PostgresConnector {
    pub fn start() -> Self {
        let child = Command::new("../target/debug/postgres-connector")
            .env("PG_CONN_POSTGRES_HOST", "127.0.0.1")
            .env("PG_CONN_POSTGRES_USER", "cdc")
            .env("PG_CONN_POSTGRES_PASSWORD", "cdc")
            .env("PG_CONN_POSTGRES_PORT", "5401")
            .env("PG_CONN_KAFKA_BROKERS", "")
            .env("PG_CONN_KAFKA_TOPIC", "example-topic")
            .env("PG_CONN_KAFKA_GROUP_ID", "default")
            .spawn()
            .expect("Failed to spawn postgres-connector");

        Self { child }
    }
}

impl Drop for PostgresConnector {
    fn drop(&mut self) {
        self.child
            .kill()
            .expect("Failed to kill postgres-connector child")
    }
}

async fn create_conn(port: u16) -> (Client, Connection<Socket, NoTlsStream>) {
    Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("cdc")
        .password("cdc")
        .dbname("cdc")
        .connect(NoTls)
        .await
        .expect("Failed to create connection")
}

pub async fn init_infra() -> Infra {
    let kafka = testcontainers_modules::kafka::Kafka::default().start();

    let src_postgres = testcontainers_modules::postgres::Postgres::default()
        .with_db_name("cdc")
        .with_user("cdc")
        .with_password("cdc")
        .with_init_sql(
            include_str!("../../scripts/init-source.sql")
                .to_string()
                .into_bytes(),
        )
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ])
        .with_mapped_port(5400, ContainerPort::Tcp(5432))
        .start();

    let sink_postgres = testcontainers_modules::postgres::Postgres::default()
        .with_db_name("cdc")
        .with_user("cdc")
        .with_password("cdc")
        .with_init_sql(
            include_str!("../../scripts/init-sink.sql")
                .to_string()
                .into_bytes(),
        )
        .with_mapped_port(5401, ContainerPort::Tcp(5432))
        .start();

    let cdc_producer_build = build_crate("cdc-producer").status();
    let postgres_connector_build = build_crate("postgres-connector").status();
    //let feldera_connector_build = build_crate("feldera-connector").status();

    let (res_kafka, res_src_postgres, res_sink_postgres, res_cdc_producer, res_postgres_connector) = join!(
        kafka,
        src_postgres,
        sink_postgres,
        cdc_producer_build,
        postgres_connector_build
    );

    res_cdc_producer.expect("Failed to build cdc producer");
    res_postgres_connector.expect("Failed to build postgres connector");

    let cdc_producer = CdcProducer::start();
    let postgres_connector = PostgresConnector::start();

    // Wait for processes to be active
    std::thread::sleep(Duration::from_secs(1));

    let ((src_client, _src_conn), (sink_client, _sink_conn)) =
        join!(create_conn(5400), create_conn(5401));

    Infra {
        _kafka: res_kafka.expect("Failed to init Kafka"),
        _src_postgres: res_src_postgres.expect("Failed to init source Postgres"),
        _sink_postgres: res_sink_postgres.expect("Failed to init sink Postgres"),
        _cdc_producer: cdc_producer,
        _postgres_connector: postgres_connector,
        src_client,
        _src_conn,
        sink_client,
        _sink_conn,
    }
}

pub const TWO_SECONDS: Duration = Duration::from_secs(2);
