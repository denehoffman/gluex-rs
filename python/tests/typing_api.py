"""Static type-check probes for the public ``gluex`` package layout."""

from datetime import datetime, timezone

from gluex import Histogram, Particle, RESTVersionSelection, RunPeriod, generation
from gluex.ccdb import CCDB, Data
from gluex.lumi import FluxHistograms, Luminosity
from gluex.rcdb import RCDB, Expr, aliases, float_cond
from gluex.rcdb import all as all_conditions
from laddu import Channel, Dataset


def typed_api_surface(
    ccdb_path: str,
    rcdb_path: str,
    channel: Channel,
    dataset: Dataset,
) -> None:
    period: RunPeriod = RunPeriod.RP2018_08
    selection: RESTVersionSelection = RESTVersionSelection.timestamp(datetime(2019, 7, 21, 12, tzinfo=timezone.utc))
    histogram: Histogram = Histogram([1.0], [8.0, 9.0])
    particle: Particle = Particle.Phi

    calibrations: dict[int, Data] = CCDB(ccdb_path).fetch_run_period(
        '/test/demo/mytable',
        run_period=period,
        rest_version=selection,
    )
    filter_expression: Expr = all_conditions(
        aliases.approved_production(period),
        float_cond('beam_current').gt(2.0),
    )
    run_numbers: list[int] = RCDB(rcdb_path).fetch_runs(filters=filter_expression)
    flux: FluxHistograms = Luminosity(rcdb_path, ccdb_path).fetch(
        histogram.edges,
        runs=run_numbers,
        rest_version={period: selection},
    )
    writer: generation.GlueXHddmWriter = generation.GlueXHddmWriter(generation.GlueXHddmConfig(channel))
    writer.write(dataset, 'events.hddm')

    _ = (calibrations, particle, flux)
