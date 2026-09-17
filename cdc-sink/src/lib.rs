use cdc_avro::ChangeEvent;
use futures_util::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
};
use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
}

pub trait KafkaSink {
    fn on_event<'a>(
        &mut self,
        event: ChangeEvent<'a>,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}

pub async fn consume_from_kafka<S: KafkaSink>(own_config: KafkaConfig, mut sink: S) {
    let log_level = if cfg!(debug_assertions) {
        RDKafkaLogLevel::Debug
    } else {
        RDKafkaLogLevel::Info
    };

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", own_config.group_id)
        .set("boostrap.servers", own_config.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set_log_level(log_level)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec![own_config.topic.as_str()])
        .expect("Can't subscribe to specified topics");

    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        match result {
            Ok(borrowed_message) => {
                if let Some(view) = borrowed_message.payload_view::<[u8]>() {
                    match ChangeEvent::from_avro(view.expect("")) {
                        Ok(event) => {
                            // This is written a little bit awkward but
                            if let Err(e) = sink.on_event(event).await {
                                eprintln!("{:?}", e);
                            } else if let Err(e) = consumer.commit_message(
                                &borrowed_message,
                                rdkafka::consumer::CommitMode::Async,
                            ) {
                                eprintln!("{:?}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to retrieve avro: {}", e)
                        }
                    }
                }
            }
            Err(e) => eprintln!("Kafka error: {:?}", e),
        }
    }
}
