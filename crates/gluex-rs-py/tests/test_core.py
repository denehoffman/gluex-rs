"""Tests for shared ``gluex`` root types."""

from __future__ import annotations

import datetime as dt

import gluex
import pytest


def test_histogram_validates_data() -> None:
    histogram = gluex.Histogram([1.0, 4.0], [0.0, 1.0, 2.0])
    assert histogram.errors == [1.0, 2.0]
    assert histogram.as_dict()["counts"] == [1.0, 4.0]

    with pytest.raises(ValueError):
        gluex.Histogram([1.0], [0.0, 1.0, 2.0])


def test_run_period_and_rest_selection_are_shared_root_types() -> None:
    assert gluex.RunPeriod("f18") == gluex.RunPeriod.RP2018_08
    assert gluex.RunPeriod.from_run(50000) == gluex.RunPeriod.RP2018_08
    assert gluex.RunPeriod.RP2018_08.short_name == "F18"
    assert gluex.RunPeriod.RP2018_08.contains(50000)
    assert gluex.RunPeriod.RP2018_08.coherent_peak() == (8.2, 8.8)

    selection = gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2)
    assert selection.resolve_timestamp(gluex.RunPeriod.RP2018_08) == dt.datetime(
        2019, 7, 21, 12, tzinfo=dt.timezone.utc
    )
    timestamp = gluex.parse_timestamp("2020-02")
    assert timestamp == dt.datetime(2020, 2, 29, 23, 59, 59, tzinfo=dt.timezone.utc)


def test_detector_and_polarization_root_values() -> None:
    assert gluex.DetectorSystem("Cherenkov") == gluex.DetectorSystem.CHERENKOV
    assert str(gluex.DetectorSystem.CHERENKOV) == "Cherenkov"
    assert gluex.DetectorSystem("ST") == gluex.DetectorSystem.START
    assert gluex.Polarization.PARA0 == gluex.Polarization.PARA0


def test_particle_root_values_preserve_external_conversions() -> None:
    assert str(gluex.Particle.Phi) == "phiMeson"
    assert gluex.Particle(str(gluex.Particle.Phi)) == gluex.Particle.Phi
    assert gluex.Particle.from_particle_type("Phi") == gluex.Particle.Phi
    assert gluex.Particle.KStar892Zero.to_particle_type() == "K*(892)0"
    assert str(gluex.Particle.KStar892Zero) == "KStar_892_0"
    assert gluex.Particle.Phi.to_evtgen() == "phi"
    assert gluex.Particle.from_geant4("phi") == gluex.Particle.Phi
    assert gluex.Particle.from_pdg(2212) == gluex.Particle.Proton
    assert gluex.Particle.Proton.charge() == gluex.Charge.Positive
    assert gluex.Particle.Proton.is_final_state()
