#![allow(dead_code)]

use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

pub struct TestDatabase {
    path: PathBuf,
}

impl TestDatabase {
    fn from_sql(name: &str, sql: &str) -> Self {
        let id = NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "gluex-rs-{name}-{}-{id}.sqlite",
            std::process::id()
        ));
        let connection = Connection::open(&path).expect("failed to create fixture database");
        connection
            .execute_batch(sql)
            .expect("failed to populate fixture database");
        drop(connection);
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        fs::remove_file(&self.path).expect("failed to delete fixture database");
    }
}

pub fn ccdb() -> TestDatabase {
    TestDatabase::from_sql("ccdb", include_str!("ccdb.sql"))
}

pub fn rcdb() -> TestDatabase {
    TestDatabase::from_sql("rcdb", include_str!("rcdb.sql"))
}
