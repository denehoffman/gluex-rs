"""Static type-check probes for the public ``gluex`` package layout."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, assert_type

import gluex
from gluex import Histogram, Particle, RESTVersionSelection, RunPeriod, generation
from gluex.ccdb import CCDB, Data
from gluex.rcdb import RCDB, Expr, aliases, float_cond
from gluex.rcdb import all as all_conditions

if TYPE_CHECKING:
    from pathlib import Path

    from gluex.lumi import FluxHistograms
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
    gx = gluex.open(rcdb=rcdb_path, ccdb=ccdb_path)
    runs = gx.runs.select(run_numbers).collect()
    reconstruction = gluex.ReconstructionSelection.periods({period: selection})
    flux: FluxHistograms = (
        gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=histogram.edges).compute().histograms
    )
    writer: generation.GlueXHddmWriter = generation.GlueXHddmWriter(generation.GlueXHddmConfig(channel))
    writer.write(dataset, 'events.hddm')

    _ = (calibrations, particle, flux)


def typed_session_surface(rcdb_path: Path, ccdb_path: str) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=ccdb_path)
    assert_type(gx, gluex.GlueX)
    assert_type(gx.capabilities, gluex.Capabilities)
    assert_type(gx.capabilities.rcdb, bool)
    assert_type(gx.sources, gluex.Sources)
    assert_type(gx.sources.rcdb, RCDB)
    assert_type(gx.sources.ccdb, CCDB)
    assert_type(gx.sources.ccdb.opened_at, datetime)
    assert_type(gluex.open(rcdb=None, ccdb=gluex.DISABLED), gluex.GlueX)
    assert_type(gluex.GlueX(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED), gluex.GlueX)


def invalid_session_arguments() -> None:
    # These suppressions must remain necessary: checking with --error-on-warning
    # detects a regression to permissive or unresolved generated annotations.
    gluex.open(rcdb=False)  # ty: ignore[invalid-argument-type]
    gluex.open(gluex.DISABLED)  # ty: ignore[too-many-positional-arguments]
