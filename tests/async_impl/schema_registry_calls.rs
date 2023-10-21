use schema_registry_converter::async_impl::schema_registry::{
    get_all_subjects, get_all_versions, perform_sr_call, SrSettings,
};
use schema_registry_converter::schema_registry_common::SrCall;

fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
}

#[tokio::test]
async fn test_get_all_subjects() {
    let sr_settings = SrSettings::new(get_schema_registry_url());
    let result = get_all_subjects(&sr_settings).await.unwrap();
    assert!(
        result.contains(&String::from("testavro-value")),
        "List of subjects contains testavro-value"
    );
}

#[tokio::test]
async fn test_get_versions_for_testavro_value() {
    let sr_settings = SrSettings::new(get_schema_registry_url());
    let result = get_all_versions(&sr_settings, String::from("testavro-value"))
        .await
        .unwrap();
    assert_eq!(vec![1], result, "List of version is just one");
}

#[tokio::test]
async fn test_get_schema_for_testavro_value() {
    let sr_settings = SrSettings::new(get_schema_registry_url());
    let sr_call = SrCall::GetBySubjectAndVersion("testavro-value", 1);
    let result = perform_sr_call(&sr_settings, sr_call).await.unwrap();
    assert_eq!(Some(1), result.version, "Returned schema has version 1");
}
