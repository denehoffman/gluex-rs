"""Integration checks for experiment file readers and writers."""

import gluex
import laddu
import numpy as np
import pytest


def sample_dataset():
    proton_mass = 0.93827208816
    return laddu.Dataset.from_arrays(
        p4s={
            "beam": np.array([[5.0, 0.0, 0.0, 5.0]]),
            "target": np.array([[proton_mass, 0.0, 0.0, 0.0]]),
            "proton": np.array([[2.5, 0.0, 0.0, 2.0]]),
            "meson": np.array([[3.43827208816, 0.0, 0.0, 3.0]]),
        },
        scalars={
            "polarization_fraction": np.array([0.4]),
            "polarization_angle": np.array([0.25]),
        },
        weights=np.array([1.5]),
    )


def test_pawian_dataset_round_trip(tmp_path):
    path = tmp_path / "events.dat"
    names = ["proton", "meson"]
    gluex.io.write_pawian(
        sample_dataset(), path, particles=2, final_state_names=names,
        weights=True, polarization=True, order="e_px_py_pz", unit="MeV",
    )
    data = gluex.io.read_pawian(
        path, particles=2, final_state_names=names,
        weights=True, polarization=True, order="e_px_py_pz", unit="MeV",
    )
    assert data.p4_names() == ["beam", "target", *names]
    assert data.scalar_names() == ["polarization_fraction", "polarization_angle"]
    np.testing.assert_allclose(data.weights(), [1.5])
    assert len(data) == 1


def test_fsroot_dataset_round_trip(tmp_path):
    path = tmp_path / "events.root"
    names = ["proton", "meson"]
    gluex.io.write_fsroot(
        sample_dataset(), path, particles=2, final_state_names=names,
        prefix="kin_", weight_branch="evt_weight",
    )
    data = gluex.io.read_fsroot(
        path, particles=2, final_state_names=names,
        prefix="kin_", weight_branch="evt_weight",
    )
    assert data.p4_names() == ["beam", "target", *names]
    np.testing.assert_allclose(data.weights(), [1.5])


def test_pawian_rejects_inconsistent_initial_state(tmp_path):
    path = tmp_path / "bad.dat"
    path.write_text("1 0 0 1\n")
    data = gluex.io.read_pawian(path, particles=1)
    with pytest.raises(Exception, match="inconsistent with a z-axis photon"):
        len(data)
