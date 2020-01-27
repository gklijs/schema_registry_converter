use avro_rs::types::Value;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use schema_registry_converter::Decoder;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }
}

#[derive(Debug)]
pub struct DeserializedRecord<'a> {
    pub key: Value,
    pub value: Value,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

type TestConsumer = BaseConsumer<CustomContext>;

pub fn consume(
    brokers: &str,
    group_id: &str,
    registry: String,
    topics: &[&str],
    test: Box<dyn Fn(DeserializedRecord) -> ()>,
) {
    let mut decoder = Decoder::new(registry);
    let consumer = get_consumer(brokers, group_id, topics);

    for message in consumer.iter() {
        match message {
            Err(e) => {
                assert!(false, "Got error consuming message: {}", e);
            }
            Ok(m) => {
                let des_r = get_deserialized_record(&m, &mut decoder);
                test(des_r);
                return
            }
        };
    }
}

fn get_deserialized_record<'a>(
    m: &'a BorrowedMessage,
    decoder: &'a mut Decoder,
) -> DeserializedRecord<'a> {
    let key = match decoder.decode(m.key()) {
        Ok(v) => v,
        Err(e) => panic!("Error getting value: {}", e),
    };
    let value = match decoder.decode(m.payload()) {
        Ok(v) => v,
        Err(e) => panic!("Error getting value: {}", e),
    };
    DeserializedRecord {
        key,
        value,
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
    }
}

fn get_consumer(brokers: &str, group_id: &str, topics: &[&str]) -> TestConsumer {
    let context = CustomContext;
    let consumer: TestConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(context)
        .expect("Consumer creation failed");
    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");
    consumer
}
