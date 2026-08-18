"""Tests for streaming laddu 0.21.2 datasets to GlueX HDDM."""

import json
import subprocess
import sys
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


def _generation_channel() -> laddu.Channel:
    return laddu.Channel(
        'gamma p -> pi+ n',
        edges=[
            laddu.Edge(
                'beam',
                particle=laddu.particles.PHOTON,
                initial_momentum=laddu.InitialMomentum.uniform_energy(
                    low=8.0,
                    high=9.0,
                    direction=[0.0, 0.0, 1.0],
                ),
            ),
            laddu.Edge(
                'target',
                particle=laddu.particles.PROTON,
                initial_momentum=laddu.InitialMomentum.momentum([0.0, 0.0, 0.0]),
            ),
            laddu.Edge('pi_plus', particle=laddu.particles.PI_PLUS, output=True),
            laddu.Edge('neutron', particle=laddu.particles.NEUTRON, output=True),
        ],
        vertices=[
            laddu.Vertex(
                'production',
                incoming=['beam', 'target'],
                outgoing=['pi_plus', 'neutron'],
                generation=laddu.VertexProposal.t_exchange(
                    incoming='beam',
                    outgoing='pi_plus',
                ),
            ),
        ],
    )


def test_generation_manifest_from_native_channel_json() -> None:
    channel = _generation_channel()
    restored = laddu.Channel.from_json(channel.to_json())
    manifest = json.loads(generation.config_json(restored))

    assert manifest['version'] == 1
    assert manifest['beam']['name'] == 'beam'
    assert manifest['target']['name'] == 'target'
    assert manifest['production']['transfer']['outgoing'] == 'pi_plus'
    assert 'generation' not in manifest


def test_python_generation_config_includes_model_parameters_and_scalars(
    tmp_path: Path,
) -> None:
    scale = laddu.parameter('scale', initial=2.0)
    model = laddu.Model(scale * (laddu.scalar('polarization') + 1.0))
    config = generation.GenerationConfig(
        _generation_channel(),
        model=model,
        parameters={'scale': 3.0},
        scalars={
            'polarization': generation.Scalar.uniform(-0.5, 0.5),
            'setting': generation.Scalar.fixed(7.0),
        },
        pilot_proposals=250,
        safety_scale=1.5,
    )
    config.add_scalar(
        'calibration',
        generation.Scalar.histogram([0.0, 1.0, 2.0], [1.0, 3.0]),
    )
    config.max_weight = 100.0
    config.validate()

    path = tmp_path / 'generation.json'
    config.write(path)
    manifest = json.loads(path.read_text())

    assert manifest['parameters'] == [3.0]
    assert manifest['scalars']['polarization'] == {
        'kind': 'uniform',
        'min': -0.5,
        'max': 0.5,
    }
    assert manifest['scalars']['setting'] == {'kind': 'fixed', 'value': 7.0}
    assert manifest['scalars']['calibration']['kind'] == 'histogram'
    assert manifest['generation'] == {
        'max_weight': 100.0,
        'pilot_proposals': 250,
        'safety_scale': 1.5,
    }
    assert isinstance(manifest['model'], dict)


def test_cli_executes_python_authored_model_config(tmp_path: Path) -> None:
    model = laddu.Model(laddu.scalar('intensity') + 1.0)
    config = generation.GenerationConfig(
        _generation_channel(),
        model=model,
        scalars={'intensity': generation.Scalar.uniform(0.0, 1.0)},
        pilot_proposals=100,
    )
    manifest = tmp_path / 'generation.json'
    output = tmp_path / 'events.hddm'
    config.write(manifest)
    gluex = Path(sys.executable).with_name('gluex')

    subprocess.run(  # noqa: S603 - executable is the sibling of sys.executable
        [str(gluex), 'gen', 'check', str(manifest)],
        check=True,
        capture_output=True,
        text=True,
    )
    result = subprocess.run(  # noqa: S603 - executable is the sibling of sys.executable
        [
            str(gluex),
            'gen',
            'run',
            str(manifest),
            '--events',
            '5',
            '--run-number',
            '90000',
            '--seed',
            '1234',
            '--output',
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert output.stat().st_size > 0
    assert 'wrote 5 events' in result.stdout


def test_python_generation_config_runs_directly(tmp_path: Path) -> None:
    config = generation.GenerationConfig(_generation_channel())
    output = tmp_path / 'python-events.hddm'

    report = config.run(
        output,
        events=5,
        run_number=90_000,
        seed=1234,
    )

    assert output.stat().st_size > 0
    assert report['requested'] == 5
    assert report['produced'] == 5
    assert report['seed'] == 1234


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
