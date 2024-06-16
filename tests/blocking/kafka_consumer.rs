use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::consumer::base_consumer::BaseConsumer;

type TestConsumer = BaseConsumer<DefaultConsumerContext>;

pub fn get_consumer(
    brokers: &str,
    group_id: &str,
    topics: &[&str],
    auto_commit: bool,
) -> TestConsumer {
    let context = DefaultConsumerContext;
    let auto_commit_value = if auto_commit { "true" } else { "false" };
    let consumer: TestConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", auto_commit_value)
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(context)
        .expect("Consumer creation failed");
    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");
    consumer
}
