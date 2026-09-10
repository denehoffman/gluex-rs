"""Static positive and negative probes for the installed database reading APIs."""

from collections.abc import Iterator
from datetime import datetime
from typing import assert_type

import gluex


def typed_luminosity(
    gx: gluex.GlueX,
    result: gluex.RunSet,
    reconstruction: gluex.ReconstructionSelection,
) -> None:
    luminosity = gx.workflows.luminosity(
        result, reconstruction=reconstruction, edges=[8.0, 9.0]
    ).collect()
    assert_type(luminosity, gluex.LuminosityResult)
    assert_type(luminosity.provenance.runs, gluex.RunProvenance)
    assert_type(luminosity.provenance.rcdb_source, str)
    assert_type(luminosity.provenance.ccdb_source, str)
    assert_type(luminosity.provenance.requested_reconstruction, gluex.ReconstructionSelection)
    assert_type(luminosity.provenance.calibration_default_as_of, datetime)
    assert_type(luminosity.provenance.coherent_peak, bool)
    assert_type(luminosity.provenance.polarized, bool)
    assert_type(
        gx.workflows
        .luminosity(result, reconstruction=reconstruction, edges=[8.0, 9.0])
        .fallback_to(run=50685),
        gluex.LuminosityQuery,
    )


def typed_reads(gx: gluex.GlueX) -> None:
    catalog = gx.conditions
    assert_type(catalog, gluex.ConditionCatalog)
    assert_type(catalog.keys(), tuple[str, ...])
    assert_type(catalog.items(), tuple[tuple[str, gluex.ConditionDefinition], ...])
    assert_type(iter(catalog), Iterator[str])
    assert_type(catalog['event_count'].value_type, str)
    query = gx.runs(gluex.RunSelection.range(2, 5))
    assert_type(query, gluex.RunQuery)
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
    projected = query.select(['event_count'])
    assert_type(projected.stream(), gluex.ConditionStream)
    assert_type(projected.strict(), gluex.ConditionQuery)
    assert_type(projected.fill('event_count', value=0), gluex.ConditionQuery)
    table = gx.calibrations['/TARGET/density']
    calibration = table.for_runs(query)
    assert_type(calibration, gluex.CalibrationQuery)
    assert_type(calibration.stream(), gluex.CalibrationStream)
    assert_type(calibration.fallback_to(50685), gluex.CalibrationQuery)
    reconstruction = gluex.ReconstructionSelection.periods(
        {gluex.RunPeriod.RP2018_08: gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2)}
    )
    assert_type(calibration.with_reconstruction(reconstruction), gluex.CalibrationQuery)
    typed_luminosity(gx, result, reconstruction)
    for reader in (gx.sources.rcdb, gx.sources.ccdb):
        raw = reader.raw('SELECT ?, ?, ?, ?, ?', parameters=[2, 1.5, 'text', b'bytes', None])
        assert_type(raw, gluex.RawResults)
        assert_type(raw.columns, tuple[gluex.RawColumn, ...])
        assert_type(raw.columns[0].declared_type, str | None)
        assert_type(raw.rows, tuple[gluex.RawRow, ...])
        assert_type(raw.rows[0][0], int | float | str | bytes | None)
        assert_type(raw.rows[0]['column'], int | float | str | bytes | None)
        assert_type(raw.rows[0].values, tuple[int | float | str | bytes | None, ...])

    gx.runs([2])  # ty: ignore[invalid-argument-type]
    gluex.RunSelection.range('2', 5)  # ty: ignore[invalid-argument-type]
    gx.sources.rcdb.raw('SELECT ?', [2])  # ty: ignore[too-many-positional-arguments]
    gx.sources.ccdb.raw('SELECT ?', parameters=[object()])  # ty: ignore[invalid-argument-type]
    result.numbers = ()  # ty: ignore[invalid-assignment]
    table.for_runs([2])  # ty: ignore[invalid-argument-type]
    query.stream(2)  # ty: ignore[too-many-positional-arguments]
    calibration.with_reconstruction(query)  # ty: ignore[invalid-argument-type]
