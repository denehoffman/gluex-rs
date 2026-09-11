"""Canonical luminosity through the root GlueX workflow."""

from __future__ import annotations

import shutil
import sqlite3
from typing import TYPE_CHECKING

import gluex
import pytest
from gluex import RESTVersionSelection, RunPeriod

if TYPE_CHECKING:
    from pathlib import Path

TAGGED_FLUX = 690_176_020.9265559
TAGGED_LUMINOSITY = 0.0008695639199135528


def test_root_luminosity_workflow_retains_run_and_procedure_evidence() -> None:
    gx = gluex.open()
    runs = gx.runs.select(50685).collect()
    reconstruction = gluex.ReconstructionSelection.periods(
        {RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)}
    )
    query = gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0])
    assert 'lazy=True' in repr(query)
    result = query.compute()

    assert result.histograms.tagged_flux.counts[1] == pytest.approx(TAGGED_FLUX)
    assert result.histograms.tagged_luminosity.counts[1] == pytest.approx(TAGGED_LUMINOSITY)
    assert result.report.selected_runs == (50685,)
    assert result.report.used_runs == (50685,)
    assert result.report.excluded_runs == ()
    assert result.report.complete
    assert result.provenance.procedure_version == 'gluex-luminosity-v1'
    assert result.provenance.procedure_status == 'canonical'
    assert any('pair production' in reference for reference in result.provenance.references)
    assert any('target length' in assumption for assumption in result.provenance.assumptions)
    assert any('coherent-peak' in gap for gap in result.provenance.validation_gaps)
    assert any('REST' in gap for gap in result.provenance.validation_gaps)
    assert any('72436' in exception for exception in result.provenance.exceptions)
    assert result.provenance.missing_policy == 'strict'
    assert result.provenance.runs.source == str(runs.provenance.source)
    assert result.provenance.coherent_peak is False
    assert result.provenance.polarized is False
    assert result.provenance.rcdb_source
    assert result.provenance.ccdb_source
    assert 'periods' in repr(result.provenance.requested_reconstruction)
    assert result.provenance.calibration_default_as_of.tzinfo is not None
    assert result.provenance.resolved_reconstruction['F18'][0] == 'default'


def test_concise_workflow_navigation_matches_explicit_path() -> None:
    gx = gluex.open()
    runs = gx.runs.select(50685).collect()
    reconstruction = gluex.ReconstructionSelection.periods({'F18': 2})

    concise = gx.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0]).compute()
    explicit = gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0]).compute()
    assert concise.histograms.tagged_flux.counts == explicit.histograms.tagged_flux.counts
    assert type(concise) is type(explicit)
    assert repr(concise.provenance) == repr(explicit.provenance)
    assert 'luminosity=available' in repr(gx.workflows)
    assert 'future' in repr(gx.operations)
    assert 'conversion' in repr(gx.operations)
    with pytest.raises(AttributeError):
        gx.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 9.0]).collect()  # ty: ignore[unresolved-attribute]


def test_four_period_reconstruction_mapping_preserves_rest_variations() -> None:
    selection = gluex.ReconstructionSelection.periods(
        gluex.RunPeriod('s17').rest(5),
        gluex.RunPeriod('s18').rest(2),
        gluex.RunPeriod('f18').rest(2),
        gluex.RunPeriod('s20').rest(1),
    )

    assert selection.resolve('S17') == ('recon_2017_01_ver05', '2025-11-26T13:41:24+00:00')
    assert selection.resolve('S18')[0] == 'default'
    assert selection.resolve('F18')[0] == 'default'
    assert selection.resolve('S20')[0] == 'default'


def test_single_period_rest_selection_is_accepted_directly_by_luminosity() -> None:
    gx = gluex.open()
    period = gluex.RunPeriod('f18')
    runs = gx.runs.select(50685).collect()

    result = gx.luminosity(runs, reconstruction=period.rest(2), edges=[8.0, 9.0]).compute()

    assert result.provenance.resolved_reconstruction['F18'][0] == 'default'


def test_workflow_settings_are_keyword_only() -> None:
    gx = gluex.open()
    runs = gx.runs.select(50685).collect()
    reconstruction = gluex.ReconstructionSelection.latest()

    with pytest.raises(TypeError):
        gx.workflows.luminosity(runs, reconstruction, [8.0, 9.0])  # ty: ignore[missing-argument, too-many-positional-arguments]
    query = gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 9.0])
    with pytest.raises(TypeError):
        query.coherent_peak(True)  # ty: ignore[too-many-positional-arguments]  # noqa: FBT003
    with pytest.raises(TypeError):
        query.polarized(True)  # ty: ignore[too-many-positional-arguments]  # noqa: FBT003
    with pytest.raises(TypeError):
        query.fallback_to(50685)  # ty: ignore[missing-argument, too-many-positional-arguments]
    with pytest.raises(TypeError):
        query.timeout(1.0)  # ty: ignore[missing-argument, too-many-positional-arguments]


def test_coherent_peak_reference_case_records_the_selection() -> None:
    gx = gluex.open()
    runs = gx.runs.select(50685).collect()
    reconstruction = gluex.ReconstructionSelection.periods(
        {RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)}
    )

    result = (
        gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0]).coherent_peak().compute()
    )

    assert result.histograms.tagged_flux.counts == [0.0, 0.0]
    assert result.histograms.tagged_luminosity.counts == [0.0, 0.0]
    assert result.provenance.coherent_peak is True


def test_explicit_fallback_substitutes_and_reports_selected_run(
    tmp_path: Path, rcdb_path: Path, ccdb_path: Path
) -> None:
    rcdb = shutil.copy2(rcdb_path, tmp_path / 'rcdb.sqlite')
    ccdb = shutil.copy2(ccdb_path, tmp_path / 'ccdb.sqlite')
    with sqlite3.connect(ccdb) as connection:
        connection.execute('UPDATE runRanges SET runMax = 50685 WHERE id = 2')
    gx = gluex.open(rcdb=str(rcdb), ccdb=str(ccdb))
    runs = gx.runs.select(50697).collect()
    reconstruction = gluex.ReconstructionSelection.periods(
        {RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)}
    )

    result = (
        gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0])
        .fallback_to(run=50685)
        .compute()
    )

    assert result.report.selected_runs == (50697,)
    assert result.report.used_runs == (50697,)
    assert result.report.substitutions == ((50697, 50685),)
    assert result.histograms.tagged_flux.counts[1] == pytest.approx(TAGGED_FLUX)
    assert result.provenance.missing_policy == 'fallback'


def test_missing_policies_never_hide_incompatible_calibration_schemas(
    tmp_path: Path, rcdb_path: Path, ccdb_path: Path
) -> None:
    rcdb = shutil.copy2(rcdb_path, tmp_path / 'rcdb.sqlite')
    ccdb = shutil.copy2(ccdb_path, tmp_path / 'ccdb.sqlite')
    with sqlite3.connect(ccdb) as connection:
        connection.execute("UPDATE columns SET columnType = 'string' WHERE id = 1012")
    gx = gluex.open(rcdb=str(rcdb), ccdb=str(ccdb))
    runs = gx.runs.select(50685).collect()
    reconstruction = gluex.ReconstructionSelection.periods(
        {RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)}
    )
    query = gx.workflows.luminosity(runs, reconstruction=reconstruction, edges=[8.0, 8.5, 9.0])

    with pytest.raises(RuntimeError, match='expected a double'):
        query.report_missing().compute()
    with pytest.raises(RuntimeError, match='expected a double'):
        query.fallback_to(run=50697).compute()


def test_session_cache_controls_are_bounded_and_inspectable() -> None:
    gx = gluex.open()
    assert gx.cache_info.calibration_payload_capacity == 128
    gx.set_calibration_payload_cache_capacity(0)
    assert gx.cache_info.calibration_payload_capacity == 1
    gx.clear_caches()
    assert gx.cache_info.ccdb_metadata_entries == 0
