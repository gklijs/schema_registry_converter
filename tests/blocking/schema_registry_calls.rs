use crate::blocking::proto_tests::get_schema_registry_url;
use schema_registry_converter::blocking::schema_registry::{
    SrSettings, get_all_subjects, get_all_versions, get_referenced_schema, get_schema_by_subject,
    is_compatible_schema, perform_sr_call,
};
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{
    RegisteredReference, RegisteredSchema, SrCall, SubjectNameStrategy, SuppliedReference,
    SuppliedSchema,
};

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

#[test]
fn test_get_unknown_schema() {
    let sr_settings = SrSettings::new_builder(get_schema_registry_url())
        .no_proxy()
        .build()
        .unwrap();
    let sr_call = SrCall::GetBySubjectAndVersion("unknown-schema", 1);
    let result = perform_sr_call(&sr_settings, sr_call);
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

fn to_supplied_reference(
    sr_settings: &SrSettings,
    registered_reference: &RegisteredReference,
) -> SuppliedReference {
    let reference_schema = get_referenced_schema(sr_settings, registered_reference).unwrap();
    let references = reference_schema
        .references
        .iter()
        .map(|r| to_supplied_reference(sr_settings, r))
        .collect();
    let mut reference = SuppliedReference::new(
        registered_reference.name.clone(),
        registered_reference.subject.clone(),
        reference_schema.schema,
    )
    .with_references(references);
    reference.properties = reference_schema.properties;
    reference.tags = reference_schema.tags;
    reference
}

fn to_supplied_schema(sr_settings: &SrSettings, schema: RegisteredSchema) -> SuppliedSchema {
    let references = schema
        .references
        .iter()
        .map(|r| to_supplied_reference(sr_settings, r))
        .collect();
    let mut supplied =
        SuppliedSchema::new(None, schema.schema_type, schema.schema).with_references(references);
    supplied.properties = schema.properties;
    supplied.tags = schema.tags;
    supplied
}

#[test]
fn test_check_schema_compatibility_pass() {
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
    .unwrap();
    let compatible = is_compatible_schema(
        &sr_settings,
        compatibility_check_subject,
        to_supplied_schema(&sr_settings, schema),
    )
    .unwrap();
    assert!(compatible, "Identical schema should be compatible");
}

#[test]
fn test_check_schema_compatibility_fail() {
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
    .unwrap();
    let compatible = is_compatible_schema(
        &sr_settings,
        compatibility_check_subject,
        to_supplied_schema(&sr_settings, schema),
    )
    .unwrap();
    assert!(
        !compatible,
        "avro-result schema should not be compatible with testavro-value"
    );
}
