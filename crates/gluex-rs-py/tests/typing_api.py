"""Static type-check probes for the public ``gluex`` package layout."""

from datetime import datetime, timezone

from gluex import Histogram, Particle, RESTVersionSelection, RunPeriod, ccdb, lumi, rcdb


def typed_api_surface(ccdb_path: str, rcdb_path: str) -> None:
    period: RunPeriod = RunPeriod.RP2018_08
    selection: RESTVersionSelection = RESTVersionSelection.timestamp(
        datetime(2019, 7, 21, 12, tzinfo=timezone.utc)
    )
    histogram: Histogram = Histogram([1.0], [8.0, 9.0])
    particle: Particle = Particle.Phi

    calibrations: dict[int, ccdb.Data] = ccdb.CCDB(ccdb_path).fetch_run_period(
        "/test/demo/mytable",
        run_period=period,
        rest_version=selection,
    )
    filter_expression: rcdb.Expr = rcdb.all(
        rcdb.aliases.approved_production(period),
        rcdb.float_cond("beam_current").gt(2.0),
    )
    run_numbers: list[int] = rcdb.RCDB(rcdb_path).fetch_runs(filters=filter_expression)
    flux: lumi.FluxHistograms = lumi.Luminosity(rcdb_path, ccdb_path).fetch(
        histogram.edges,
        runs=run_numbers,
        rest_version={period: selection},
    )

    _ = (calibrations, particle, flux)
