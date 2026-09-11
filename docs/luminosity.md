# Photon flux and luminosity

The luminosity workflow combines a resolved RCDB `RunSet` with CCDB constants and an explicit reconstruction context.

```python
period = gluex.RunPeriod("2018-08")
runs = gx.runs.select(period).collect()
reconstruction = gluex.ReconstructionSelection.periods({period: period.rest(2)})

result = gx.workflows.luminosity(
    runs,
    reconstruction=reconstruction,
    edges=[8.0, 8.5, 9.0],
).compute()

print(result.histograms.tagged_flux.counts)
print(result.histograms.tagged_luminosity.counts)
print(result.report)
print(result.provenance)
```

The input must be a collected `RunSet`; the workflow does not hide an approved-production cut. Apply any RCDB predicate before collecting the runs.

## Coherent-energy filtering

```python
result = (
    gx.workflows.luminosity(
        runs,
        reconstruction=reconstruction,
        edges=[0.5, 1.0, 1.2, 1.5],
    )
    .coherent_peak()
    .compute()
)
```

For every run, the workflow resolves `/PHOTON_BEAM/coherent_energy` from the same CCDB source. Missing or malformed coherent-energy constants are reported as missing workflow input. No run-number-to-energy mapping is compiled into the library.

The `/ANALYSIS/beam_asymmetry/coherent_energy` table is intentionally not used by this general flux workflow. It represents an analysis-specific choice and differs for several periods; see the [comparison](run-periods.md#coherent-energy-table-choice).

## Polarized runs

Calling `.polarized()` applies the RCDB coherent-beam predicate and the converter-dependent inputs required by the calculation. Runs rejected by that constraint remain visible in the report.

## Missing data and provenance

The result separates selected, used, and excluded runs. Provenance records both database sources, requested and resolved reconstruction contexts, calibration cutoff, procedure version, scientific references, assumptions, and known validation gaps. Persist it beside derived histograms when reproducibility matters.
