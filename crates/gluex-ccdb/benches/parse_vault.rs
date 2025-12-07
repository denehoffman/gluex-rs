use std::{hint::black_box, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use gluex_ccdb::{data::ColumnLayout, database::CCDB, models::ColumnMeta};

const TABLE_PATH: &str = "/test/demo/mytable";
const DEFAULT_DB: &str = "ccdb.sqlite";

fn load_layout_and_vault() -> (Arc<ColumnLayout>, String, usize) {
    let db_path = std::env::var("CCDB_BENCH_DB").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let db = CCDB::open(&db_path).expect("failed to open database");
    let table = db
        .table(TABLE_PATH)
        .expect("failed to open benchmark table");
    let columns: Vec<ColumnMeta> = table.columns().expect("failed to load columns");
    let layout = Arc::new(ColumnLayout::new(columns));
    let n_rows = table.meta().n_rows() as usize;

    let mut stmt = db
        .connection()
        .prepare_cached(
            "SELECT cs.vault
             FROM constantSets cs
             JOIN assignments a ON cs.id = a.constantSetId
             WHERE cs.constantTypeId = ?
             ORDER BY a.created DESC
             LIMIT 1",
        )
        .expect("failed to prepare vault query");
    let vault: String = stmt
        .query_row([table.id()], |row| row.get(0))
        .expect("failed to load vault");

    (layout, vault, n_rows)
}

fn bench_parse_vault(c: &mut Criterion) {
    let (layout, vault, n_rows) = load_layout_and_vault();
    c.bench_function("parse_vault_test_table", |b| {
        b.iter(|| {
            let data =
                gluex_ccdb::data::Data::from_vault(black_box(&vault), layout.clone(), n_rows)
                    .expect("parse failed");
            black_box(data);
        })
    });
}

criterion_group!(benches, bench_parse_vault);
criterion_main!(benches);
