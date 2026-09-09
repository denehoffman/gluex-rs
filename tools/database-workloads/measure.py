"""Run isolated database workloads and write a local reproducibility report."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import statistics
import subprocess
from pathlib import Path
from typing import Any

WORKLOADS = (
    'rcdb-cold',
    'rcdb-bulk',
    'rcdb-repeated',
    'ccdb-cold',
    'ccdb-bulk',
    'ccdb-repeated',
    'calibration-stream-shared',
    'raw-read',
    'vault-parse',
    'lumi-cold',
    'lumi-repeated',
)

WORKLOAD_DEFINITION_FILES = (
    Path('examples/database_workloads.rs'),
    Path('tools/database-workloads/measure.py'),
)


def command(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()  # noqa: S603


def repository_state(repository: Path) -> dict[str, Any]:
    status = command('git', '-C', str(repository), 'status', '--porcelain=v1')
    diff = subprocess.check_output(  # noqa: S603
        ['git', '-C', str(repository), 'diff', '--binary', 'HEAD']  # noqa: S607
    )
    return {
        'commit': command('git', '-C', str(repository), 'rev-parse', 'HEAD'),
        'dirty': bool(status),
        'tracked_diff_sha256': hashlib.sha256(diff).hexdigest(),
    }


def definition_fingerprint(repository: Path) -> str:
    digest = hashlib.sha256()
    for relative_path in WORKLOAD_DEFINITION_FILES:
        digest.update(str(relative_path).encode())
        digest.update((repository / relative_path).read_bytes())
    return digest.hexdigest()


def summarize(measurements: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {}
    for workload in WORKLOADS:
        samples = [item['ns_per_iteration'] for item in measurements if item['workload'] == workload]
        peaks = [
            item['peak_rss_kib']
            for item in measurements
            if item['workload'] == workload and item['peak_rss_kib'] is not None
        ]
        summary[workload] = {
            'samples': len(samples),
            'latency_ns_per_iteration': {
                'min': min(samples),
                'median': statistics.median(samples),
                'mean': statistics.fmean(samples),
                'max': max(samples),
            },
            'peak_rss_kib': {
                'median': statistics.median(peaks) if peaks else None,
                'max': max(peaks) if peaks else None,
            },
        }
    return summary


def validate_comparison(measurements: list[dict[str, Any]], previous_report: dict[str, Any]) -> None:
    previous = previous_report['measurements']
    for measurement in measurements:
        matches = [item for item in previous if item['workload'] == measurement['workload']]
        if not matches:
            message = f'{measurement["workload"]}: missing from comparison'
            raise ValueError(message)
        for key in ('result_sha256', 'fixtures'):
            if any(item[key] != measurement[key] for item in matches):
                message = f'{measurement["workload"]}: {key} differs'
                raise ValueError(message)


def main() -> None:
    repository = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--binary',
        type=Path,
        default=repository / 'target/release/examples/database_workloads',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=repository / 'database-workload-results/local.json',
    )
    parser.add_argument('--label', default='local')
    parser.add_argument('--iterations', type=int, default=20)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--compare', type=Path)
    args = parser.parse_args()
    if args.iterations < 1 or args.repeats < 1:
        parser.error('iterations and repeats must be positive')

    measurements = []
    for workload in WORKLOADS:
        for repeat in range(args.repeats):
            measurement = json.loads(command(str(args.binary.resolve()), workload, str(args.iterations)))
            measurement['repeat'] = repeat + 1
            measurements.append(measurement)
        print(f'{workload}: {args.repeats} isolated process(es)', flush=True)

    comparison = None
    if args.compare:
        comparison = json.loads(args.compare.read_text())
        validate_comparison(measurements, comparison)

    report = {
        'schema_version': 1,
        'label': args.label,
        'revision': repository_state(repository),
        'workload_definition_sha256': definition_fingerprint(repository),
        'platform': {
            'description': platform.platform(),
            'machine': platform.machine(),
            'python': platform.python_version(),
        },
        'toolchain': {
            'rustc': command('rustc', '-vV'),
            'cargo': command('cargo', '--version'),
        },
        'iteration_method': {
            'processes_per_workload': args.repeats,
            'timed_iterations_per_process': args.iterations,
            'process_isolation': True,
            'filesystem_cache': 'not cleared; fixtures and validation touch each database',
        },
        'measurements': measurements,
        'summary': summarize(measurements),
    }
    if comparison is not None:
        report['comparison'] = {
            'label': comparison.get('label'),
            'revision': comparison.get('revision', comparison.get('base_commit')),
            'summary': comparison.get('summary'),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'wrote {args.output}')


if __name__ == '__main__':
    main()
