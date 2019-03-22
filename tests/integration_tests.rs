extern crate schema_registry_converter;

#[test] #[cfg_attr(not(feature = "kafka_test"), ignore)]
fn test_add() {
    assert_eq!(2 + 3, 5);
}