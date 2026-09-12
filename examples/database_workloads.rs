//! Fixture-backed database workloads for local, non-CI measurements.

use std::{
    collections::{BTreeMap, BTreeSet},
    hint::black_box,
    sync::Arc,
    time::Instant,
};

use gluex_rs::{
    GlueX, RESTVersionSelection, RawValue, ReconstructionSelection, RunPeriod, RunSelection,
    SourceConfig,
    ccdb::{CCDB, CCDBContext, data::ColumnLayout, models::ColumnMeta},
    core::parsers::parse_timestamp,
    lumi::FluxHistograms,
    rcdb::{RCDB, RCDBContext},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[path = "../tests/fixtures/rust.rs"]
mod fixtures;

const WORKLOADS: &[&str] = &[
    "rcdb-cold",
    "rcdb-bulk",
    "rcdb-repeated",
    "ccdb-cold",
    "ccdb-bulk",
    "ccdb-repeated",
    "calibration-stream-shared",
    "raw-read",
    "vault-parse",
    "lumi-cold",
    "lumi-repeated",
];

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn measure<T>(
    iterations: u32,
    mut operation: impl FnMut() -> T,
    summarize_and_validate: impl Fn(&T) -> Value,
) -> Value {
    // Correctness work deliberately surrounds, rather than pollutes, the timed section.
    let expected = summarize_and_validate(&operation());
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(operation());
    }
    let elapsed = start.elapsed();
    let actual = summarize_and_validate(&operation());
    assert_eq!(
        actual, expected,
        "workload output changed across measurement"
    );
    json!({
        "iterations": iterations,
        "elapsed_ns": elapsed.as_nanos(),
        "ns_per_iteration": elapsed.as_nanos() / u128::from(iterations),
        "result_sha256": digest(&serde_json::to_vec(&expected).unwrap()),
    })
}

fn measure_rcdb(workload: &str, iterations: u32) -> Value {
    let fixture = fixtures::rcdb();
    let context = if workload == "rcdb-bulk" {
        RCDBContext::default().with_run_range(0..=60_000)
    } else {
        RCDBContext::default().with_run(2)
    };
    let reader = (workload != "rcdb-cold").then(|| RCDB::open(fixture.path()).unwrap());
    measure(
        iterations,
        || {
            let reader = reader
                .clone()
                .unwrap_or_else(|| RCDB::open(fixture.path()).unwrap());
            reader.fetch(["event_count"], &context).unwrap()
        },
        |rows| {
            let values: BTreeMap<_, _> = rows
                .iter()
                .map(|(run, row)| (*run, row["event_count"].as_int().unwrap()))
                .collect();
            assert_eq!(values[&2], 2);
            assert_eq!(values.len(), if workload == "rcdb-bulk" { 8 } else { 1 });
            json!(values)
        },
    )
}

fn measure_ccdb(workload: &str, iterations: u32) -> Value {
    let fixture = fixtures::ccdb();
    let context = CCDBContext::default()
        .with_run_range(if workload == "ccdb-bulk" {
            0..=30_000
        } else {
            2..=2
        })
        .with_variation("default")
        .with_timestamp(parse_timestamp("2020-02-01 00:00:00").unwrap());
    let reader = (workload != "ccdb-cold").then(|| CCDB::open(fixture.path()).unwrap());
    measure(
        iterations,
        || {
            let reader = reader
                .clone()
                .unwrap_or_else(|| CCDB::open(fixture.path()).unwrap());
            reader.fetch("/test/demo/mytable", &context).unwrap()
        },
        |rows| {
            assert_eq!(rows.len(), if workload == "ccdb-bulk" { 30_001 } else { 1 });
            let values: BTreeMap<_, _> = rows
                .iter()
                .map(|(run, data)| {
                    let cells: Vec<_> = (0..2)
                        .flat_map(|row| (0..3).map(move |column| data.double(column, row).unwrap()))
                        .collect();
                    assert_eq!(cells, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
                    (*run, cells)
                })
                .collect();
            json!(values)
        },
    )
}

#[derive(Debug)]
struct StreamOutcome {
    resolved: usize,
    missing: usize,
    payload_cells: usize,
    payload_addresses: BTreeSet<usize>,
}

fn measure_calibration_stream(iterations: u32) -> Value {
    let fixture = fixtures::ccdb();
    let vault = (0..30_000)
        .map(|index| ((index % 3) + 1).to_string())
        .collect::<Vec<_>>()
        .join("|");
    let connection = rusqlite::Connection::open(fixture.path()).unwrap();
    connection
        .execute_batch("UPDATE typeTables SET nRows = 10000 WHERE id = 81")
        .unwrap();
    connection
        .execute(
            "UPDATE constantSets SET vault = ? WHERE id = 230302",
            [&vault],
        )
        .unwrap();
    drop(connection);
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let table = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .clone();
    measure(
        iterations,
        || {
            let query = table.for_runs(RunSelection::range(0, 30_000)).unwrap();
            let mut outcome = StreamOutcome {
                resolved: 0,
                missing: 0,
                payload_cells: 0,
                payload_addresses: BTreeSet::new(),
            };
            for chunk in query.stream(257).unwrap() {
                let chunk = chunk.unwrap();
                outcome.resolved += chunk.items().len();
                outcome.missing += chunk.report().missing_runs().len();
                for (_, entry) in chunk.items() {
                    let payload = entry.payload();
                    outcome
                        .payload_addresses
                        .insert(std::ptr::from_ref(payload) as usize);
                    outcome.payload_cells = payload.n_rows() * payload.column_names().len();
                }
            }
            outcome
        },
        |outcome| {
            assert_eq!(outcome.resolved, 30_001);
            assert_eq!(outcome.missing, 0);
            assert_eq!(outcome.payload_cells, 30_000);
            assert_eq!(outcome.payload_addresses.len(), 1);
            json!({
                "resolved": outcome.resolved,
                "missing": outcome.missing,
                "payload_cells": outcome.payload_cells,
                "unique_payloads": outcome.payload_addresses.len(),
            })
        },
    )
}

fn raw_value(value: &RawValue) -> Value {
    match value {
        RawValue::Null => Value::Null,
        RawValue::Integer(value) => json!({"integer": value}),
        RawValue::Real(value) => json!({"real": value}),
        RawValue::Text(value) => json!({"text": value}),
        RawValue::Blob(value) => json!({"blob_sha256": digest(value)}),
    }
}

fn measure_raw_read(iterations: u32) -> Value {
    let fixture = fixtures::rcdb();
    let reader = RCDB::open(fixture.path()).unwrap();
    measure(
        iterations,
        || {
            reader
                .raw(
                    "SELECT r.number, c.int_value FROM runs r JOIN conditions c ON c.run_number = r.number JOIN condition_types t ON t.id = c.condition_type_id WHERE t.name = ? ORDER BY r.number",
                    &[RawValue::Text("event_count".into())],
                )
                .unwrap()
        },
        |results| {
            assert_eq!(results.rows().len(), 8);
            assert_eq!(results.columns()[0].name(), "number");
            let rows: Vec<_> = results
                .rows()
                .iter()
                .map(|row| row.values().iter().map(raw_value).collect::<Vec<_>>())
                .collect();
            assert_eq!(rows[0], [json!({"integer": 2}), json!({"integer": 2})]);
            json!(rows)
        },
    )
}

fn measure_vault_parse(iterations: u32) -> Value {
    let fixture = fixtures::ccdb();
    let database = CCDB::open(fixture.path()).unwrap();
    let table = database.table("/test/demo/mytable").unwrap();
    let columns: Vec<ColumnMeta> = table.columns().unwrap();
    let layout = Arc::new(ColumnLayout::new(columns));
    let n_rows = 10_000;
    let vault = (0..(n_rows * 3))
        .map(|index| ((index % 3) + 1).to_string())
        .collect::<Vec<_>>()
        .join("|");
    measure(
        iterations,
        || gluex_rs::ccdb::data::Data::from_vault(&vault, layout.clone(), n_rows).unwrap(),
        |data| {
            assert_eq!(data.n_rows(), n_rows);
            assert_eq!(data.column_names(), ["x", "y", "z"]);
            assert_eq!(data.double(0, 0), Some(1.0));
            assert_eq!(data.double(2, n_rows - 1), Some(3.0));
            json!({
                "rows": data.n_rows(),
                "columns": data.column_names(),
                "first": data.double(0, 0),
                "last": data.double(2, n_rows - 1),
            })
        },
    )
}

fn luminosity_summary(histograms: &FluxHistograms) -> Value {
    json!([
        [
            histograms.tagged_flux.counts(),
            histograms.tagged_flux.errors()
        ],
        [histograms.tagm_flux.counts(), histograms.tagm_flux.errors()],
        [histograms.tagh_flux.counts(), histograms.tagh_flux.errors()],
        [
            histograms.tagged_luminosity.counts(),
            histograms.tagged_luminosity.errors()
        ],
    ])
}

fn measure_luminosity(workload: &str, iterations: u32) -> Value {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let reconstruction = ReconstructionSelection::periods([(
        RunPeriod::RP2018_08,
        RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2).unwrap(),
    )]);
    let persistent = (workload != "lumi-cold").then(|| {
        let gx = GlueX::open(
            SourceConfig::sqlite(rcdb.path()),
            SourceConfig::sqlite(ccdb.path()),
        )
        .unwrap();
        let runs = gx
            .runs(RunSelection::runs([50_685]))
            .unwrap()
            .collect()
            .unwrap();
        (gx, runs)
    });
    measure(
        iterations,
        || {
            let (gx, runs) = if let Some((gx, runs)) = &persistent {
                (gx.clone(), runs.clone())
            } else {
                let gx = GlueX::open(
                    SourceConfig::sqlite(rcdb.path()),
                    SourceConfig::sqlite(ccdb.path()),
                )
                .unwrap();
                let runs = gx
                    .runs(RunSelection::runs([50_685]))
                    .unwrap()
                    .collect()
                    .unwrap();
                (gx, runs)
            };
            gx.workflows()
                .luminosity(&runs, reconstruction.clone(), [8.0, 8.5, 9.0])
                .collect()
                .unwrap()
                .histograms()
                .clone()
        },
        |histograms| {
            assert!(
                (histograms.tagged_flux.counts()[1] / 690_176_020.926_555_9 - 1.0).abs() < 1e-12
            );
            luminosity_summary(histograms)
        },
    )
}

fn peak_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn main() {
    let mut args = std::env::args().skip(1);
    let workload = args.next().unwrap_or_else(|| {
        eprintln!("workload required; choose one of: {}", WORKLOADS.join(", "));
        std::process::exit(2);
    });
    if workload == "--list" {
        println!("{}", WORKLOADS.join("\n"));
        return;
    }
    let iterations: u32 = args
        .next()
        .map_or(Ok(20), |arg| arg.parse())
        .expect("iterations must be an integer");
    assert!(iterations > 0, "iterations must be positive");
    assert!(args.next().is_none(), "unexpected argument");
    let mut measurement = match workload.as_str() {
        "rcdb-cold" | "rcdb-bulk" | "rcdb-repeated" => measure_rcdb(&workload, iterations),
        "ccdb-cold" | "ccdb-bulk" | "ccdb-repeated" => measure_ccdb(&workload, iterations),
        "calibration-stream-shared" => measure_calibration_stream(iterations),
        "raw-read" => measure_raw_read(iterations),
        "vault-parse" => measure_vault_parse(iterations),
        "lumi-cold" | "lumi-repeated" => measure_luminosity(&workload, iterations),
        _ => panic!(
            "unknown workload: {workload}; choose one of: {}",
            WORKLOADS.join(", ")
        ),
    };
    measurement["workload"] = json!(workload);
    measurement["iteration_method"] = json!(
        "one untimed validation, elapsed wall time around repeated operations, one untimed equivalence validation"
    );
    measurement["peak_rss_kib"] = json!(peak_rss_kib());
    measurement["fixtures"] = json!({
        "rcdb_sql_sha256": digest(include_bytes!("../tests/fixtures/rcdb.sql")),
        "ccdb_sql_sha256": digest(include_bytes!("../tests/fixtures/ccdb.sql")),
    });
    println!("{measurement}");
}
