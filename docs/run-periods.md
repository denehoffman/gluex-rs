# Run periods and coherent energy

`RunPeriod` maps Hall-D production names to their reserved numeric run ranges. It does not imply that every number in the range was recorded; RCDB determines membership.

## Accepted names

Names are case-insensitive. Every period accepts `YYYY-MM`, `YYYY_MM`, `RunPeriod-YYYY-MM`, and `RunPeriod_YYYY_MM`. Established aliases remain available.

| Period | Runs | Established alias |
| --- | ---: | --- |
| 2016-02 | 10000–19999 | `s16` |
| 2017-01 | 30000–39999 | `s17` |
| 2018-01 | 40000–49999 | `s18` |
| 2018-08 | 50000–59999 | `f18` |
| 2019-01 | 60000–69999 | `s19` |
| 2019-11 | 70000–79999 | `s20` |
| 2021-08 | 80000–89999 | `src` |
| 2021-11 | 90000–99999 | `cpp`, `npp`, `cpp/npp` |
| 2022-05 | 100000–109999 | `s22` |
| 2022-08 | 110000–119999 | `f22` |
| 2023-01 | 120000–129999 | `s23` |
| 2025-01 | 130000–139999 | `s25` |
| 2026-03 | 140000–149999 | — |
| 2026-06 | 150000–159999 | — |

The 2026 periods were confirmed from the RCDB run-period registry. They currently have monitoring/raw entries but no reconstruction rows in the Hall-D data-version service, so no REST revisions are bundled for them yet.

## Coherent-energy table choice

CCDB contains two similarly named tables:

- `/PHOTON_BEAM/coherent_energy` has production table ID 339 and is the beam calibration used by the general luminosity workflow.
- `/ANALYSIS/beam_asymmetry/coherent_energy` has production table ID 340 and is an analysis-specific calibration.

Code resolves these paths through CCDB metadata; it must not depend on the numeric IDs, which can differ in replicas and test databases.

The following values are the default-variation assignments resolved from the online CCDB on 2026-09-11. “Inherited” means the selected assignment has a broader run range than the named period.

| Period | Table 339 (GeV) | Table 340 (GeV) |
| --- | ---: | ---: |
| 2016-02 | 8.4–9.0 (inherited) | 8.4–9.0 (inherited) |
| 2017-01 | 8.2–8.8 | 8.2–8.8 |
| 2018-01 | 8.2–8.8 (inherited) | 8.2–8.8 (inherited) |
| 2018-08 | 8.2–8.8 (inherited) | 8.2–8.8 (inherited) |
| 2019-01 | 8.2–8.8 (inherited) | 8.2–8.8 (inherited) |
| 2019-11 | 8.0–8.6 | 8.0–8.6 |
| 2021-08 | 8.0–8.6 (inherited) | 8.2–8.8 (inherited) |
| 2021-11 | 8.0–8.6 (inherited) | 8.2–8.8 (inherited) |
| 2022-05 | 5.2–5.7 | 8.2–8.8 (inherited) |
| 2022-08 | 8.0–8.6 (inherited) | 8.2–8.8 (inherited) |
| 2023-01 | 8.0–8.6 (inherited) | 8.0–8.6 |
| 2025-01 | 8.0–8.6 (inherited) | 8.3–8.9 |
| 2026-03 | 1.0–1.2 | 1.0–1.2 |
| 2026-06 | 8.0–8.6 (inherited) | 8.2–8.8 (inherited) |

These are observations, not data compiled into the package. Query the CCDB snapshot used by an analysis to obtain reproducible values:

```python
gx.sources.ccdb.coherent_peak(140000)
```

## Online metadata audit

The Hall-D data-version service currently advertises `RunPeriod-2026-03` and `RunPeriod-2026-06`. Running `scripts/sync_rest_versions.py` on 2026-09-11 still produced the same 37 reconstruction rows already in `data/rest_versions.tsv`; the new periods only contain monitoring/raw versions so far.

When a reconstruction version appears, rerun the script and review the TSV diff. Run ranges come from the RCDB run-period registry, while coherent-energy values come from CCDB assignment resolution; neither should be inferred from the other.
