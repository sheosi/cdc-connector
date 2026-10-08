use std::{
    collections::HashSet,
    process::{Child, Command},
    sync::OnceLock,
    time::{Duration, Instant},
};
use testcontainers::{
    ContainerAsync, GenericImage, Healthcheck, ImageExt,
    core::{ContainerPort, client::docker_client_instance},
    runners::AsyncRunner,
};
use testcontainers_modules::postgres::Postgres;
use tokio::{join, process::Command as AsyncCommand, sync::Mutex};
use tokio_postgres::{Client, Config, NoTls};

fn build_crate(name: &str) -> AsyncCommand {
    let mut cmd = AsyncCommand::new("cargo");
    cmd.args(["build", "--bin", name]).current_dir("..");
    cmd
}

pub struct Infra {
    //_kafka: ContainerAsync<Kafka>,
    _red_panda: ContainerAsync<GenericImage>,
    _src_postgres: ContainerAsync<Postgres>,
    src_client: tokio_postgres::Client,
    _src_conn: ConnTask,
    _sink_postgres: ContainerAsync<Postgres>,
    sink_client: tokio_postgres::Client,
    _sink_conn: ConnTask,
    _cdc_producer: CdcProducer,
    _postgres_connector: PostgresConnector,
}

// Note: we register IDs, but dont unregister them, this is due to the fact that
// we have to use async Mutex (since it is held across await), and using async in
// drop is difficult, for the time being we'll just try to unregister everything
// even those that are dead
static RUNNING_IDS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

impl Infra {
    pub async fn wait_for_sink_row(
        &self,
        table: &str,
        where_clause: &str,
        timeout_duration: Duration,
    ) {
        let sql = format!("SELECT 1 FROM {} WHERE {}", table, where_clause);
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout_duration {
                panic!("Row didn't appear in sink")
            }

            match self
                .sink_client
                .query_opt(&sql, &[])
                .await
                .expect("Failed sink query")
            {
                Some(_) => return,
                None => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }

    pub async fn not_sink_row(&self, table: &str, where_clause: &str) {
        let sql = format!("SELECT 1 FROM {} WHERE {}", table, where_clause);

        if self
            .sink_client
            .query_opt(&sql, &[])
            .await
            .expect("Failed sink query")
            .is_some()
        {
            panic!("Row is still present");
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
            .env("PG_CONN_KAFKA_BROKERS", "localhost:9092")
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

struct ConnTask(tokio::task::JoinHandle<()>);

impl Drop for ConnTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn create_conn(port: u16) -> (Client, ConnTask) {
    let (clt, conn) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("cdc")
        .password("cdc")
        .dbname("cdc")
        .connect(NoTls)
        .await
        .expect("Failed to create connection");

    let handle = tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    (clt, ConnTask(handle))
}

async fn register_ids(ids: Vec<String>) {
    let mut running_ids = RUNNING_IDS
        .get_or_init(|| {
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("ctrl+c");
                for id in RUNNING_IDS.get().expect("").lock().await.iter() {
                    docker_client_instance()
                        .await
                        .unwrap()
                        .stop_container(id, None)
                        .await
                        .expect("Failed to stop container");
                }

                std::process::exit(130);
            });
            Mutex::new(HashSet::new())
        })
        .lock()
        .await;

    ids.into_iter().for_each(|id| {
        running_ids.insert(id);
    });
}

pub async fn init_infra() -> Infra {
    //let kafka = testcontainers_modules::kafka::Kafka::default().start();
    let redpanda =
        testcontainers::GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.9")
            .with_cmd([
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "1G",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--kafka-addr",
                "PLAIN://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092",
                "--advertise-kafka-addr",
                "PLAIN://redpanda:29092,OUTSIDE://localhost:9092",
                "--rpc-addr",
                "0.0.0.0:33145",
                "--advertise-rpc-addr",
                "redpanda:33145",
            ])
            .with_mapped_port(9092, ContainerPort::Tcp(9092))
            .with_mapped_port(29092, ContainerPort::Tcp(29092))
            .with_mapped_port(9644, ContainerPort::Tcp(9644))
            .with_health_check(
                Healthcheck::cmd_shell("rpk cluster health")
                    .with_interval(Some(Duration::from_secs(5)))
                    .with_timeout(Duration::from_secs(5))
                    .with_retries(10),
            )
            .start();

    let src_postgres = testcontainers_modules::postgres::Postgres::default()
        .with_db_name("cdc")
        .with_user("cdc")
        .with_password("cdc")
        .with_host_auth()
        .with_init_sql(
            include_str!("../../scripts/init-source.sql")
                .to_string()
                .into_bytes(),
        )
        .with_tag("18-alpine")
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
        .with_host_auth()
        .with_init_sql(
            include_str!("../../scripts/init-sink.sql")
                .to_string()
                .into_bytes(),
        )
        .with_tag("18-alpine")
        .with_mapped_port(5401, ContainerPort::Tcp(5432))
        .start();

    let cdc_producer_build = build_crate("cdc-producer").status();
    let postgres_connector_build = build_crate("postgres-connector").status();
    //let feldera_connector_build = build_crate("feldera-connector").status();

    let (
        res_redpanda,
        res_src_postgres,
        res_sink_postgres,
        res_cdc_producer,
        res_postgres_connector,
    ) = join!(
        redpanda,
        src_postgres,
        sink_postgres,
        cdc_producer_build,
        postgres_connector_build
    );

    res_cdc_producer.expect("Failed to build cdc producer");
    res_postgres_connector.expect("Failed to build postgres connector");

    // Wait for processes to be active
    std::thread::sleep(Duration::from_secs(1));

    let cdc_producer = CdcProducer::start();
    let postgres_connector = PostgresConnector::start();

    // Wait for processes to be active
    std::thread::sleep(Duration::from_secs(1));

    let ((src_client, _src_conn), (sink_client, _sink_conn)) =
        join!(create_conn(5400), create_conn(5401));

    let red_panda = res_redpanda.expect("Failed to init RedPanda");
    let src_postgres = res_src_postgres.expect("Failed to init source Postgres");
    let sink_postgres = res_sink_postgres.expect("Failed to init sink Postgres");

    register_ids(vec![
        red_panda.id().to_string(),
        src_postgres.id().to_string(),
        sink_postgres.id().to_string(),
    ])
    .await;

    Infra {
        //_kafka: res_kafka.expect("Failed to init Kafka"),
        _red_panda: red_panda,
        _src_postgres: src_postgres,
        _sink_postgres: sink_postgres,
        _cdc_producer: cdc_producer,
        _postgres_connector: postgres_connector,
        src_client,
        _src_conn,
        sink_client,
        _sink_conn,
    }
}

pub const TWO_SECONDS: Duration = Duration::from_secs(2);
