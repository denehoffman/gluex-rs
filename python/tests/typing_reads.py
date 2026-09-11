"""Static positive and negative probes for the installed database reading APIs."""

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import assert_type

import gluex
import polars as pl


def typed_luminosity(
    gx: gluex.GlueX,
    result: gluex.RunSet,
    reconstruction: gluex.ReconstructionSelection,
) -> None:
    luminosity = gx.luminosity(result, reconstruction=reconstruction, edges=[8.0, 9.0]).compute()
    assert_type(luminosity, gluex.LuminosityResult)
    assert_type(gx.operations, gluex.Operations)
    assert_type(luminosity.provenance.runs, gluex.RunProvenance)
    assert_type(luminosity.provenance.rcdb_source, str)
    assert_type(luminosity.provenance.ccdb_source, str)
    assert_type(luminosity.provenance.requested_reconstruction, gluex.ReconstructionSelection)
    assert_type(luminosity.provenance.calibration_default_as_of, datetime)
    assert_type(luminosity.provenance.coherent_peak, bool)
    assert_type(luminosity.provenance.polarized, bool)
    assert_type(
        gx.workflows.luminosity(result, reconstruction=reconstruction, edges=[8.0, 9.0]).fallback_to(run=50685),
        gluex.LuminosityQuery,
    )


def typed_discovery(gx: gluex.GlueX) -> None:
    assert_type(gluex.connect(), gluex.GlueX)
    period = gluex.RunPeriod('s17')
    assert_type(period, gluex.RunPeriod)
    assert_type(period.rest(5), gluex.CalibratedRunPeriod)
    assert_type(period.rest(5, variation='recon'), gluex.CalibratedRunPeriod)
    assert_type(period.at(datetime.now().astimezone()), gluex.CalibratedRunPeriod)
    assert_type(gx.runs.aliases, gluex.RunAliases)
    assert_type(gx.runs.aliases.approved_production('S17'), gluex.RunPredicate)
    assert_type(gx.runs.aliases.is_coherent_beam, gluex.RunPredicate)


def typed_reads(gx: gluex.GlueX) -> None:  # noqa: PLR0915
    catalog = gx.runs.conditions
    assert_type(catalog, gluex.ConditionCatalog)
    assert_type(catalog.keys(), tuple[str, ...])
    assert_type(catalog.items(), tuple[tuple[str, gluex.ConditionDefinition], ...])
    assert_type(iter(catalog), Iterator[str])
    assert_type(catalog['event_count'].value_type, str)
    query = gx.runs.select(gluex.RunSelection.between(2, 5))
    assert_type(query, gluex.RunQuery)
    assert_type(gx.runs.select(2, 'f18'), gluex.RunQuery)
    assert_type(query.selection, gluex.RunSelection)
    assert_type(query.provenance, gluex.RunProvenance)
    result = query.collect()
    assert_type(result, gluex.RunSet)
    assert_type(result.numbers, tuple[int, ...])
    assert_type(iter(result), Iterator[int])
    assert_type(result[0], int)
    assert_type(result.provenance.source, str)
    assert_type(query.stream(chunk_size=2), gluex.RunStream)
    assert_type(query.first(), int | None)
    assert_type(query.count(), int)
    projected = query.columns('event_count')
    assert_type(projected.stream(), gluex.ConditionStream)
    assert_type(projected.strict(), gluex.ConditionQuery)
    assert_type(projected.fill('event_count', value=0), gluex.ConditionQuery)
    assert_type(projected.collect().to_polars(), pl.DataFrame)
    table = gx.calibrations['/TARGET/density']
    calibration = table.for_runs(query)
    assert_type(table.for_runs([50685], variation='default', missing_policy='strict'), gluex.CalibrationQuery)
    assert_type(calibration, gluex.CalibrationQuery)
    assert_type(
        table.for_runs(
            gluex.RunPeriod('s17').rest(4),
            gluex.RunPeriod('s18'),
            gluex.RunPeriod('f18').rest(2),
        ),
        gluex.CalibrationQuery,
    )
    assert_type(calibration.stream(), gluex.CalibrationStream)
    assert_type(calibration.fallback_to(50685), gluex.CalibrationQuery)
    assert_type(calibration.collect().to_polars(), pl.DataFrame)
    calibration_tables = gx.calibrations.select(2, 3).tables('/TARGET/density')
    assert_type(calibration_tables, gluex.CalibrationTablesQuery)
    assert_type(calibration_tables.collect(), gluex.CalibrationResults)
    assert_type(calibration_tables.collect().to_polars(), pl.DataFrame)
    calibrated_runs = gluex.RunSelection.runs([30274, 30275]).at(datetime.now(timezone.utc), variation='mc')
    assert_type(calibrated_runs, gluex.CalibratedRunSelection)
    assert_type(gluex.RunSelection.runs([30274]).rest(5), gluex.CalibratedRunSelection)
    reconstruction = gluex.ReconstructionSelection.periods({'F18': 2})
    concise_reconstruction = gluex.ReconstructionSelection.periods(gluex.RunPeriod('f18').rest(2))
    assert_type(concise_reconstruction, gluex.ReconstructionSelection)
    assert_type(reconstruction.resolve(gluex.RunPeriod('f18')), tuple[str, str])
    assert_type(calibration.with_reconstruction(reconstruction), gluex.CalibrationQuery)
    assert_type(table.for_runs(50685, reconstruction={'F18': 2}), gluex.CalibrationQuery)
    typed_luminosity(gx, result, reconstruction)
    assert_type(
        gx.luminosity(result, reconstruction=gluex.RunPeriod('f18').rest(2), edges=[8.0, 9.0]),
        gluex.LuminosityQuery,
    )
    for reader in (gx.sources.rcdb, gx.sources.ccdb):
        raw = reader.raw('SELECT ?, ?, ?, ?, ?', parameters=[2, 1.5, 'text', b'bytes', None])
        assert_type(raw, gluex.RawResults)
        assert_type(raw.columns, tuple[gluex.RawColumn, ...])
        assert_type(raw.columns[0].declared_type, str | None)
        assert_type(raw.rows, tuple[gluex.RawRow, ...])
        assert_type(raw.rows[0][0], int | float | str | bytes | None)
        assert_type(raw.rows[0]['column'], int | float | str | bytes | None)
        assert_type(raw.rows[0].values, tuple[int | float | str | bytes | None, ...])


def invalid_reads(gx: gluex.GlueX, result: gluex.RunSet, table: gluex.CalibrationTable) -> None:
    query = gx.runs.select(2)
    calibration = table.for_runs(2)
    gluex.RunSelection.between('2', 5)  # ty: ignore[invalid-argument-type]
    gx.sources.rcdb.raw('SELECT ?', [2])  # ty: ignore[too-many-positional-arguments]
    gx.sources.ccdb.raw('SELECT ?', parameters=[object()])  # ty: ignore[invalid-argument-type]
    result.numbers = ()  # ty: ignore[invalid-assignment]
    table.for_runs([2], variation=2)  # ty: ignore[invalid-argument-type]
    table.for_runs([2], missing_policy='guess')
    gx.runs.select(object())  # ty: ignore[invalid-argument-type]
    gx.luminosity(
        result,
        reconstruction=gluex.RunPeriod('f18'),  # ty: ignore[invalid-argument-type]
        edges=[8.0, 9.0],
    )
    query.stream(2)  # ty: ignore[too-many-positional-arguments]
    calibration.with_reconstruction(query)  # ty: ignore[invalid-argument-type]
