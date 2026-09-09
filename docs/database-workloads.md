# Local database workloads

The fixture-backed workload suite provides reproducible local latency and peak-memory evidence for database changes. It is deliberately separate from ordinary tests and CI: there are no elapsed-time thresholds, and generated measurements are written beneath the ignored `database-workload-results/` directory.

Run the complete matrix from a clean checkout:

```sh
rtk just database-workloads baseline 20 3
```

This builds a release executable and writes `database-workload-results/baseline.json`. The arguments are the report label, timed iterations per process, and isolated process repetitions. For a fast correctness check, use `rtk just database-workloads smoke 1 1`.

To compare another checkout or revision with the same workload results, retain the first local report and invoke the driver directly after building:

```sh
rtk cargo build --release \
  --example database_workloads
rtk proxy python3 tools/database-workloads/measure.py \
  --label candidate --iterations 20 --repeats 3 \
  --compare database-workload-results/baseline.json \
  --output database-workload-results/candidate.json
```

The driver rejects a comparison if fixture or result fingerprints differ. Each report records the Git revision, tracked-diff and workload-definition fingerprints, fixture and result fingerprints, platform, Rust and Python toolchains, iteration method, raw elapsed samples, latency summaries, and process peak resident memory. On systems without Linux `/proc`, peak RSS is recorded as unavailable rather than estimated with a different method.

Each process performs one untimed validation call, the timed loop, and one untimed equivalence validation. Assertions, JSON conversion, hashing, fixture setup, and process startup are outside the measured section. Result destruction remains inside the loop. Peak RSS is the process high-water mark, so it includes SQLite, fixture setup, validation, allocator retention, and the measured results; it is not query-only memory.

“Cold” means a new reader and source opening for each operation. It does not mean a cold operating-system filesystem cache. “Repeated” reuses an already opened reader. Bulk workloads reuse an opened reader over a large numeric range. Avoid competing builds and workloads, keep the same machine and build profile, and alternate revision order when investigating small changes.

The maintained matrix covers:

| Workload | Public work performed |
| --- | --- |
| `rcdb-cold` | Open RCDB and read `event_count` for run 2 |
| `rcdb-bulk` | Read `event_count` over runs 0–60,000 |
| `rcdb-repeated` | Reuse RCDB and repeat the run-2 read |
| `ccdb-cold` | Open CCDB and resolve `/test/demo/mytable` for run 2 |
| `ccdb-bulk` | Resolve the same table over runs 0–30,000 |
| `ccdb-repeated` | Reuse CCDB and repeat the run-2 resolution |
| `calibration-stream-shared` | Stream runs 0–30,000 in chunks of 257 while one 30,000-cell payload remains shared |
| `raw-read` | Execute a parameterized, ordered public raw RCDB read |
| `vault-parse` | Parse a 10,000-row, three-column vault using fixture metadata |
| `lumi-cold` | Open both sources, resolve run 50685, and execute luminosity |
| `lumi-repeated` | Reuse the session and resolved run set for luminosity |

These measurements support local diagnosis only. Do not commit or publish generated reports, compare unlike fixtures or outputs, infer universal speedups, or turn their timings into normal CI gates.
