use schema_registry_converter::async_impl::schema_registry::{
    get_all_subjects, get_all_versions, get_referenced_schema, get_schema_by_subject,
    is_compatible_schema, perform_sr_call, SrSettings,
};
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{
    RegisteredReference, SrCall, SubjectNameStrategy, SuppliedReference, SuppliedSchema,
};

fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
}

#[tokio::test]
async fn test_get_all_subjects() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let result = get_all_subjects(&sr_settings).await.unwrap();
    assert!(
        result.contains(&String::from("testavro-value")),
        "List of subjects contains testavro-value"
    );
}

#[tokio::test]
async fn test_get_versions_for_testavro_value() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let result = get_all_versions(&sr_settings, String::from("testavro-value"))
        .await
        .unwrap();
    assert_eq!(vec![1], result, "List of version is just one");
}

#[tokio::test]
async fn test_get_schema_for_testavro_value() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let sr_call = SrCall::GetBySubjectAndVersion("testavro-value", 1);
    let result = perform_sr_call(&sr_settings, sr_call).await.unwrap();
    assert_eq!(Some(1), result.version, "Returned schema has version 1");
}

#[tokio::test]
async fn test_get_unknown_schema() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let sr_call = SrCall::GetBySubjectAndVersion("unknown-schema", 1);
    let result = perform_sr_call(&sr_settings, sr_call).await;
    assert!(result.is_err(), "Call should have failed");
    assert_eq!(
        result.err().unwrap(),
        SRCError::new(
            "HTTP request to schema registry failed with status 404 Not Found",
            Some(String::from(
                "error_code: 40401, message: Subject 'unknown-schema' not found."
            )),
            false
        )
    );
}

async fn to_supplied_reference(
    sr_settings: &SrSettings,
    registered_reference: &RegisteredReference,
) -> SuppliedReference {
    let reference_schema = get_referenced_schema(sr_settings, registered_reference)
        .await
        .unwrap();
    SuppliedReference {
        name: registered_reference.name.clone(),
        subject: registered_reference.subject.clone(),
        schema: reference_schema.schema,
        references: futures::future::join_all(
            reference_schema
                .references
                .iter()
                .map(|r| to_supplied_reference(sr_settings, r)),
        )
        .await,
        properties: reference_schema.properties,
        tags: reference_schema.tags,
    }
}

#[tokio::test]
async fn test_check_schema_compatibility_pass() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let schema_subject = String::from("testavro-value");
    let compatibility_check_subject = schema_subject.clone();
    let schema = get_schema_by_subject(
        &sr_settings,
        &SubjectNameStrategy::RecordNameStrategy(schema_subject),
    )
    .await
    .unwrap();
    let compatible = is_compatible_schema(
        &sr_settings,
        compatibility_check_subject,
        SuppliedSchema {
            name: None,
            schema_type: schema.schema_type,
            schema: schema.schema,
            references: futures::future::join_all(
                schema
                    .references
                    .iter()
                    .map(|r| to_supplied_reference(&sr_settings, r)),
            )
            .await,
            properties: schema.properties,
            tags: schema.tags,
        },
    )
    .await
    .unwrap();
    assert!(compatible, "Identical schema should be compatible");
}

#[tokio::test]
async fn test_check_schema_compatibility_fail() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let schema_subject = String::from("avro-result");
    let compatibility_check_subject = String::from("testavro-value");
    let schema = get_schema_by_subject(
        &sr_settings,
        &SubjectNameStrategy::RecordNameStrategy(schema_subject),
    )
    .await
    .unwrap();
    let compatible = is_compatible_schema(
        &sr_settings,
        compatibility_check_subject,
        SuppliedSchema {
            name: None,
            schema_type: schema.schema_type,
            schema: schema.schema,
            references: futures::future::join_all(
                schema
                    .references
                    .iter()
                    .map(|r| to_supplied_reference(&sr_settings, r)),
            )
            .await,
            properties: schema.properties,
            tags: schema.tags,
        },
    )
    .await
    .unwrap();
    assert!(
        !compatible,
        "avro-result schema should not be compatible with testavro-value"
    );
}
