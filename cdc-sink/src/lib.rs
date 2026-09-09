use cdc_avro::ChangeEvent;
use futures_util::stream::TryStreamExt;
use rdkafka::{
    ClientConfig, Message,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
};

#[derive(Default)]
pub struct KafkaConfig {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
}

pub trait KafkaSink {
    fn on_event(&self, event: ChangeEvent) -> impl std::future::Future<Output = Result<(), ()>>;
}

pub async fn consume_from_kafka<S: KafkaSink>(sink: &S) {
    let own_config = KafkaConfig::default();

    let log_level = if cfg!(debug_assertions) {
        RDKafkaLogLevel::Debug
    } else {
        RDKafkaLogLevel::Info
    };

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", own_config.kafka_group_id)
        .set("boostrap.servers", own_config.kafka_brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set_log_level(log_level)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec![own_config.kafka_topic.as_str()])
        .expect("Can't subscribe to specified topics");

    let stream_procesor = consumer
        .stream()
        .try_for_each(|borrowed_message| async move {
            if let Some(view) = borrowed_message.payload_view::<[u8]>() {
                let event = ChangeEvent::from_avro(view.expect(""));
                if let Err(e) = sink.on_event(event).await {
                    eprintln!("{:?}", e);
                }
            }

            Ok(())
        });

    stream_procesor.await.expect("Failed");
}
