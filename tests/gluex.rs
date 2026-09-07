#![allow(missing_docs)]

use gluex_rs::{GlueX, RunPeriod, SourceConfig};

#[path = "fixtures/rust.rs"]
mod fixtures;

#[test]
fn opening_without_databases_keeps_reference_information_available() {
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::Disabled).unwrap();
    assert!(!gx.capabilities().rcdb());
    assert!(!gx.capabilities().ccdb());
    assert!(RunPeriod::RP2018_08.min_run() > 0);
    let error = gx.sources().rcdb().unwrap_err().to_string();
    assert!(error.contains("RCDB_CONNECTION"));
    assert!(error.contains("unavailable"));
    assert!(gx.to_string().contains("unavailable"));
}

#[test]
fn each_source_can_be_used_independently_and_outlive_the_root() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    for has_rcdb in [false, true] {
        for has_ccdb in [false, true] {
            let gx = GlueX::open(
                if has_rcdb {
                    SourceConfig::sqlite(rcdb.path())
                } else {
                    SourceConfig::Disabled
                },
                if has_ccdb {
                    SourceConfig::sqlite(ccdb.path())
                } else {
                    SourceConfig::Disabled
                },
            )
            .unwrap();
            assert_eq!(gx.capabilities().rcdb(), has_rcdb);
            assert_eq!(gx.capabilities().ccdb(), has_ccdb);
            let sources = gx.sources().clone();
            drop(gx);
            if has_rcdb {
                let values = sources
                    .rcdb()
                    .unwrap()
                    .fetch(
                        ["event_count"],
                        &gluex_rs::rcdb::RCDBContext::default().with_run(2),
                    )
                    .unwrap();
                assert_eq!(values[&2]["event_count"].as_int(), Some(2));
            } else {
                assert!(sources.rcdb().is_err());
            }
            if has_ccdb {
                let reader = sources.ccdb().unwrap();
                let values = reader
                    .fetch("/test/demo/mytable", &reader.default_context([2]))
                    .unwrap();
                assert_eq!(values[&2].named_double("x", 0), Some(1.0));
            } else {
                assert!(sources.ccdb().is_err());
            }
        }
    }
}

#[test]
fn calibration_defaults_are_captured_once_and_contexts_are_independent() {
    let fixture = fixtures::ccdb();
    let before = chrono::Utc::now();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let reader = gx.sources().ccdb().unwrap();
    let captured = reader.opened_at();
    assert!(before <= captured && captured <= chrono::Utc::now());
    let historical = reader
        .default_context([2])
        .with_timestamp_string("2013-02-22 13:40:35")
        .unwrap();
    assert_eq!(
        reader.fetch("/test/demo/mytable", &historical).unwrap()[&2].named_double("x", 0),
        Some(0.0)
    );
    let cloned = reader.clone();
    let current = cloned.default_context([2]);
    assert_eq!(current.timestamp, captured);
    assert_eq!(current.variation, "default");
    let table = cloned.table("/test/demo/mytable").unwrap();
    assert_eq!(table.default_context([2]).timestamp, captured);
    assert_eq!(
        table.fetch(&current).unwrap()[&2].named_double("x", 0),
        Some(1.0)
    );
}

#[test]
fn broken_configured_sources_fail_during_opening() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    assert!(GlueX::open(SourceConfig::sqlite(ccdb.path()), SourceConfig::Disabled).is_err());
    assert!(GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(rcdb.path())).is_err());
    let directory = tempfile::tempdir().unwrap();
    let invalid = directory.path().join("invalid.sqlite");
    std::fs::write(&invalid, b"not a database").unwrap();
    for path in [
        directory.path().join("missing.sqlite"),
        invalid,
        directory.path().to_path_buf(),
    ] {
        for database in [gluex_rs::DatabaseKind::Rcdb, gluex_rs::DatabaseKind::Ccdb] {
            let (rcdb, ccdb) = match database {
                gluex_rs::DatabaseKind::Rcdb => {
                    (SourceConfig::sqlite(&path), SourceConfig::Disabled)
                }
                gluex_rs::DatabaseKind::Ccdb => {
                    (SourceConfig::Disabled, SourceConfig::sqlite(&path))
                }
            };
            let error = GlueX::open(rcdb, ccdb).unwrap_err();
            assert!(
                matches!(error, gluex_rs::GlueXError::Configuration { database: kind, .. } if kind == database)
            );
            assert!(error.to_string().contains("local SQLite"));
        }
    }
}

#[test]
fn environment_configuration_is_isolated() {
    use std::{env, process::Command};

    if let Ok(case) = env::var("GLUEX_TEST_CONFIGURATION_CASE") {
        let explicit = || {
            (
                SourceConfig::sqlite(env::var_os("GLUEX_TEST_RCDB_PATH").unwrap()),
                SourceConfig::sqlite(env::var_os("GLUEX_TEST_CCDB_PATH").unwrap()),
            )
        };
        let result = match case.as_str() {
            "disabled" => GlueX::open(SourceConfig::Disabled, SourceConfig::Disabled),
            "explicit" => {
                let (rcdb, ccdb) = explicit();
                GlueX::open(rcdb, ccdb)
            }
            _ => GlueX::from_env(),
        };
        if case.starts_with("bad") || case == "empty" || case == "unsupported" {
            let error = result.unwrap_err();
            assert!(matches!(error, gluex_rs::GlueXError::Configuration { .. }));
            assert!(!error.to_string().contains("secret-password"));
        } else {
            let gx = result.unwrap();
            assert_eq!(
                gx.capabilities().rcdb(),
                matches!(case.as_str(), "both" | "rcdb" | "explicit")
            );
            assert_eq!(
                gx.capabilities().ccdb(),
                matches!(case.as_str(), "both" | "ccdb" | "explicit")
            );
        }
        return;
    }
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    for case in [
        "absent",
        "both",
        "rcdb",
        "ccdb",
        "disabled",
        "explicit",
        "bad-rcdb",
        "bad-ccdb",
        "empty",
        "unsupported",
    ] {
        let mut child = Command::new(env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "environment_configuration_is_isolated",
                "--nocapture",
            ])
            .env("GLUEX_TEST_CONFIGURATION_CASE", case)
            .env("GLUEX_TEST_RCDB_PATH", rcdb.path())
            .env("GLUEX_TEST_CCDB_PATH", ccdb.path())
            .env_remove("RCDB_CONNECTION")
            .env_remove("CCDB_CONNECTION");
        if matches!(case, "both" | "rcdb") {
            child.env("RCDB_CONNECTION", rcdb.path());
        }
        if matches!(case, "both" | "ccdb") {
            child.env("CCDB_CONNECTION", ccdb.path());
        }
        if matches!(case, "disabled" | "explicit" | "bad-rcdb") {
            child.env("RCDB_CONNECTION", "/missing-rcdb.sqlite");
        }
        if matches!(case, "disabled" | "explicit" | "bad-ccdb") {
            child.env("CCDB_CONNECTION", "/missing-ccdb.sqlite");
        }
        if case == "empty" {
            child.env("RCDB_CONNECTION", "");
        }
        if case == "unsupported" {
            child.env(
                "CCDB_CONNECTION",
                "mysql://user:secret-password@localhost/ccdb",
            );
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "{case}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn incomplete_read_schemas_are_rejected_before_a_capability_is_reported() {
    for change in [
        "DROP TABLE assignments",
        "ALTER TABLE constantSets DROP COLUMN vault",
    ] {
        let fixture = fixtures::ccdb();
        rusqlite::Connection::open(fixture.path())
            .unwrap()
            .execute_batch(change)
            .unwrap();
        assert!(matches!(
            GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())),
            Err(gluex_rs::GlueXError::Configuration {
                database: gluex_rs::DatabaseKind::Ccdb,
                ..
            })
        ));
    }
    for change in [
        "DROP TABLE runs",
        "ALTER TABLE conditions DROP COLUMN time_value",
    ] {
        let fixture = fixtures::rcdb();
        rusqlite::Connection::open(fixture.path())
            .unwrap()
            .execute_batch(change)
            .unwrap();
        assert!(matches!(
            GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled),
            Err(gluex_rs::GlueXError::Configuration {
                database: gluex_rs::DatabaseKind::Rcdb,
                ..
            })
        ));
    }
}
