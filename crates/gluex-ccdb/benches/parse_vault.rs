#![allow(missing_docs)]

use std::{hint::black_box, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use gluex_ccdb::{data::ColumnLayout, database::CCDB, models::ColumnMeta};

const TABLE_PATH: &str = "/test/demo/mytable";
const DEFAULT_DB: &str = "ccdb.sqlite";

fn load_layout_and_vaults() -> (Arc<ColumnLayout>, Vec<String>, usize) {
    let db_path = std::env::var("CCDB_BENCH_DB").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let db = CCDB::open(&db_path).expect("failed to open database");
    let table = db
        .table(TABLE_PATH)
        .expect("failed to open benchmark table");
    let columns: Vec<ColumnMeta> = table.columns().expect("failed to load columns");
    let layout = Arc::new(ColumnLayout::new(columns));
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let n_rows = table.meta().n_rows() as usize;

    let connection = db.connection();
    let mut stmt = connection
        .prepare_cached(
            "SELECT cs.vault
             FROM constantSets cs
             JOIN assignments a ON cs.id = a.constantSetId
             WHERE cs.constantTypeId = ?
             ORDER BY a.created DESC",
        )
        .expect("failed to prepare vault query");
    let vaults: Vec<String> = stmt
        .query_map([table.id()], |row| row.get(0))
        .expect("failed to query vaults")
        .collect::<Result<Vec<String>, _>>()
        .expect("failed to collect vaults");
    assert!(!vaults.is_empty(), "no vaults returned for benchmark table");

    (layout, vaults, n_rows)
}

fn bench_parse_vault(c: &mut Criterion) {
    let (layout, vaults, n_rows) = load_layout_and_vaults();
    let first = vaults
        .first()
        .expect("expected at least one vault for benchmark")
        .clone();
    c.bench_function("parse_vault_test_table", |b| {
        b.iter(|| {
            let data =
                gluex_ccdb::data::Data::from_vault(black_box(&first), layout.clone(), n_rows)
                    .expect("parse failed");
            black_box(data);
        });
    });
}

fn bench_parse_multiple_vaults(c: &mut Criterion) {
    let (layout, vaults, n_rows) = load_layout_and_vaults();
    c.bench_function("parse_multiple_vaults_test_table", |b| {
        b.iter(|| {
            for vault in &vaults {
                let data =
                    gluex_ccdb::data::Data::from_vault(black_box(vault), layout.clone(), n_rows)
                        .expect("parse failed");
                black_box(data);
            }
        });
    });
}

criterion_group!(benches, bench_parse_vault, bench_parse_multiple_vaults);
criterion_main!(benches);
