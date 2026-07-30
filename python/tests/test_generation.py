"""Tests for streaming laddu 0.20 datasets to GlueX HDDM."""

from pathlib import Path
from typing import cast

import laddu
import numpy as np
from gluex import generation


def _channel() -> laddu.Channel:
    photon = laddu.Particle('gamma', mass=0.0, ids={'pdg': 22})
    proton = laddu.Particle('proton', mass=0.938_272_046, ids={'pdg': 2212})
    kshort = laddu.Particle('KShort', mass=0.497_614, ids={'pdg': 310})
    edges = [
        laddu.Edge('beam', p4='beam', particle=photon, output=True),
        laddu.Edge('target', p4='target', particle=proton, output=True),
        laddu.Edge('kshort', p4='kshort', particle=kshort, output=True),
        laddu.Edge('recoil', p4='recoil', particle=proton, output=True),
    ]
    vertices = [
        laddu.Vertex(
            'production',
            incoming=['beam', 'target'],
            outgoing=['kshort', 'recoil'],
        ),
    ]
    return laddu.Channel('gamma p -> KShort p', edges=edges, vertices=vertices)


def _dataset() -> laddu.Dataset:
    return laddu.Dataset.from_arrays(
        p4s={
            'beam': np.array([[9.0, 0.0, 0.0, 9.0], [9.0, 0.0, 0.0, 9.0]]),
            'target': np.array(
                [
                    [0.938_272_046, 0.0, 0.0, 0.0],
                    [0.938_272_046, 0.0, 0.0, 0.0],
                ]
            ),
            'kshort': np.array([[4.5, 0.2, 0.1, 4.45], [4.3, -0.1, 0.2, 4.25]]),
            'recoil': np.array([[5.438, -0.2, -0.1, 4.55], [5.638, 0.1, -0.2, 4.75]]),
        },
        scalars={},
        weights=cast('list[float]', np.array([1.0, 0.5])),
    )


def test_generation_writer_consumes_laddu_dataset(tmp_path: Path) -> None:
    config = generation.GlueXHddmConfig(
        _channel(),
        run_number=90_000,
        first_event_number=7,
        random_seed=12_345,
        vertex=(0.1, 0.2, 50.0),
    )
    assert config.beam == 'beam'
    assert config.target == 'target'

    output = tmp_path / 'events.hddm'
    generation.GlueXHddmWriter(config).write(_dataset(), output)
    assert output.stat().st_size > 0
