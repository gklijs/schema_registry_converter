use apache_avro::types::Value;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

pub struct RecordProducer {
    producer: FutureProducer,
    avro_encoder: AvroEncoder,
}

impl<'a> RecordProducer {
    pub fn send_avro(
        &'a mut self,
        topic: &'a str,
        key_values: Vec<(&'static str, Value)>,
        value_values: Vec<(&'static str, Value)>,
        key_strategy: SubjectNameStrategy,
        value_strategy: SubjectNameStrategy,
    ) {
        let payload = match self.avro_encoder.encode(value_values, &value_strategy) {
            Ok(v) => v,
            Err(e) => panic!("Error getting payload: {}", e),
        };
        let key = match self.avro_encoder.encode(key_values, &key_strategy) {
            Ok(v) => v,
            Err(e) => panic!("Error getting payload: {}", e),
        };
        let fr = FutureRecord {
            topic,
            partition: None,
            payload: Some(&payload),
            key: Some(&key),
            timestamp: None,
            headers: None,
        };
        self.producer.send_result(fr).unwrap();
    }
}

pub fn get_producer(brokers: &str, schema_registry_url: String) -> RecordProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "60000")
        .set("queue.buffering.max.messages", "10")
        .create()
        .expect("Producer creation error");

    let sr_settings = SrSettings::new(schema_registry_url);
    let avro_encoder = AvroEncoder::new(sr_settings);
    RecordProducer {
        producer,
        avro_encoder,
    }
}
