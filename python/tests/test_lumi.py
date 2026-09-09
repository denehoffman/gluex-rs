"""Canonical luminosity through the root GlueX workflow."""

from __future__ import annotations

import gluex
import pytest
from gluex import RESTVersionSelection, RunPeriod

TAGGED_FLUX = 690_176_020.9265559
TAGGED_LUMINOSITY = 0.0008695639199135528


def test_root_luminosity_workflow_retains_run_and_procedure_evidence() -> None:
    gx = gluex.open()
    runs = gx.runs(gluex.RunSelection.runs([50685])).collect()
    reconstruction = gluex.ReconstructionSelection.periods(
        {RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)}
    )
    query = gx.workflows.luminosity(runs, reconstruction, [8.0, 8.5, 9.0])
    assert 'lazy=True' in repr(query)
    result = query.collect()

    assert result.histograms.tagged_flux.counts[1] == pytest.approx(TAGGED_FLUX)
    assert result.histograms.tagged_luminosity.counts[1] == pytest.approx(TAGGED_LUMINOSITY)
    assert result.report.selected_runs == (50685,)
    assert result.report.used_runs == (50685,)
    assert result.report.excluded_runs == ()
    assert result.report.complete
    assert result.provenance.procedure_version == 'gluex-luminosity-v1'
    assert result.provenance.procedure_status == 'provisional'
    assert result.provenance.missing_policy == 'strict'
    assert result.provenance.runs.source == str(runs.provenance.source)
    assert result.provenance.coherent_peak is False
    assert result.provenance.polarized is False
    assert result.provenance.rcdb_source
    assert result.provenance.ccdb_source
    assert result.provenance.resolved_reconstruction['F18'][0] == 'default'


def test_session_cache_controls_are_bounded_and_inspectable() -> None:
    gx = gluex.open()
    assert gx.cache_info.calibration_payload_capacity == 128
    gx.set_calibration_payload_cache_capacity(0)
    assert gx.cache_info.calibration_payload_capacity == 1
    gx.clear_caches()
    assert gx.cache_info.ccdb_metadata_entries == 0
