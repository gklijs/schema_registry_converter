use crate::blocking::proto_tests::get_schema_registry_url;
use schema_registry_converter::blocking::schema_registry::{
    get_all_subjects, get_all_versions, perform_sr_call, SrSettings,
};
use schema_registry_converter::schema_registry_common::SrCall;

#[test]
fn test_get_all_subjects() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let result = get_all_subjects(&sr_settings).unwrap();
    assert!(
        result.contains(&String::from("testavro-value")),
        "List of subjects contains testavro-value"
    );
}

#[test]
fn test_get_versions_for_testavro_value() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let result = get_all_versions(&sr_settings, String::from("testavro-value")).unwrap();
    assert_eq!(vec![1], result, "List of version is just one");
}

#[test]
fn test_get_schema_for_testavro_value() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let sr_call = SrCall::GetBySubjectAndVersion("testavro-value", 1);
    let result = perform_sr_call(&sr_settings, sr_call).unwrap();
    assert_eq!(Some(1), result.version, "Returned schema has version 1");
}
