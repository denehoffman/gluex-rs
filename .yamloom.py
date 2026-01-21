from __future__ import annotations

from dataclasses import dataclass

from yamloom import (
    Environment,
    Events,
    Job,
    Matrix,
    Permissions,
    PullRequestEvent,
    PushEvent,
    Strategy,
    Workflow,
    WorkflowDispatchEvent,
    script,
)
from yamloom.actions.github.artifacts import download_artifact, upload_artifact
from yamloom.actions.github.scm import checkout
from yamloom.actions.packaging.python import maturin
from yamloom.actions.toolchains.python import setup_python, setup_uv
from yamloom.actions.toolchains.rust import setup_rust
from yamloom.expressions import context


@dataclass
class Target:
    runner: str
    target: str
    skip_python_versions: list[str] | None = None


DEFAULT_PYTHON_VERSIONS = [
    '3.9',
    '3.10',
    '3.11',
    '3.12',
    '3.13',
    '3.13t',
    '3.14',
    '3.14t',
    'pypy3.11',
]


def resolve_python_versions(skip: list[str] | None) -> list[str]:
    if not skip:
        return DEFAULT_PYTHON_VERSIONS
    skipped = set(skip)
    return [version for version in DEFAULT_PYTHON_VERSIONS if version not in skipped]


def create_build_job(job_name: str, name: str, manifest_path: str, targets: list[Target], *, needs: list[str]) -> Job:
    def platform_entry(target: Target) -> dict[str, object]:
        entry = {
            'runner': target.runner,
            'target': target.target,
            'python_versions': resolve_python_versions(target.skip_python_versions),
        }
        python_arch = ('arm64' if target.target == 'aarch64' else target.target) if name == 'windows' else None
        if python_arch is not None:
            entry['python_arch'] = python_arch
        return entry

    return Job(
        [
            checkout(),
            script(
                f'printf "%s\n" {context.matrix.platform.python_versions.as_array().join(" ")} >> version.txt',
            ),
            setup_python(
                python_version_file='version.txt',
                architecture=context.matrix.platform.python_arch.as_str() if name == 'windows' else None,
            ),
            maturin(
                name='Build wheels',
                target=context.matrix.platform.target.as_str(),
                args=f'--release --out dist --manifest-path {manifest_path} --interpreter {context.matrix.platform.python_versions.as_array().join(" ")}',
                sccache=~context.github.ref.startswith('refs/tags/'),
                manylinux='musllinux_1_2' if name == 'musllinux' else ('auto' if name == 'linux' else None),
            ),
            upload_artifact(
                path='dist',
                artifact_name=f'wheels-{name}-{context.matrix.platform.target}',
            ),
        ],
        name=job_name,
        runs_on=context.matrix.platform.runner.as_str(),
        strategy=Strategy(
            fast_fail=False,
            matrix=Matrix(
                platform=[platform_entry(target) for target in targets],
            ),
        ),
        needs=needs,
        condition=context.github.ref.startswith('refs/tags/') | (context.github.event_name == 'workflow_dispatch'),
    )


def generate_release(name: str, manifest_path: str) -> Workflow:
    return Workflow(
        name=f'Build and Release {name}',
        on=Events(
            push=PushEvent(branches=['main'], tags=['*']),
            pull_request=PullRequestEvent(),
            workflow_dispatch=WorkflowDispatchEvent(),
        ),
        permissions=Permissions(contents='read'),
        jobs={
            'build-check': Job(
                [
                    checkout(),
                    setup_rust(components=['clippy']),
                    setup_uv(python_version='3.9'),
                    script('cargo clippy'),
                    script(
                        'uv venv',
                        '. .venv/bin/activate',
                        'echo PATH=$PATH >> $GITHUB_ENV',
                        'uv pip install pytest yamloom',
                        f'uvx --with "maturin[patchelf]>=1.7,<2" maturin develop --uv --manifest-path {manifest_path}',
                    ),
                    script('uvx ruff check', 'uvx ty check'),
                ],
                runs_on='ubuntu-latest',
            ),
            'linux': create_build_job(
                'Build Linux Wheels',
                'linux',
                manifest_path,
                [
                    Target(
                        'ubuntu-22.04',
                        target,
                    )
                    for target in [
                        'x86_64',
                        'x86',
                        'aarch64',
                        'armv7',
                        's390x',
                        'ppc64le',
                    ]
                ],
                needs=['build-check'],
            ),
            'musllinux': create_build_job(
                'Build (musl) Linux Wheels',
                'musllinux',
                manifest_path,
                [
                    Target(
                        'ubuntu-22.04',
                        target,
                    )
                    for target in [
                        'x86_64',
                        'x86',
                        'aarch64',
                        'armv7',
                    ]
                ],
                needs=['build-check'],
            ),
            'windows': create_build_job(
                'Build Windows Wheels',
                'windows',
                manifest_path,
                [
                    Target(
                        'windows-latest',
                        'x64',
                    ),
                    Target('windows-latest', 'x86', ['pypy3.11']),
                    Target(
                        'windows-11-arm',
                        'aarch64',
                        ['3.9', '3.10', '3.11', '3.13t', '3.14t', 'pypy3.11'],
                    ),
                ],
                needs=['build-check'],
            ),
            'macos': create_build_job(
                'Build macOS Wheels',
                'macos',
                manifest_path,
                [
                    Target(
                        'macos-15-intel',
                        'x86_64',
                    ),
                    Target(
                        'macos-latest',
                        'aarch64',
                    ),
                ],
                needs=['build-check'],
            ),
            'sdist': Job(
                [
                    checkout(),
                    maturin(name='Build sdist', command='sdist', args='--out dist --manifest-path {manifest_path}'),
                    upload_artifact(path='dist', artifact_name='wheels-sdist'),
                ],
                name='Build Source Distribution',
                runs_on='ubuntu-22.04',
                needs=['build-check'],
                condition=context.github.ref.startswith('refs/tags/')
                | (context.github.event_name == 'workflow_dispatch'),
            ),
            'release': Job(
                [
                    download_artifact(),
                    setup_uv(),
                    script(
                        'uv publish --trusted-publishing always wheels-*/*',
                    ),
                ],
                name='Release',
                runs_on='ubuntu-22.04',
                condition=context.github.ref.startswith('refs/tags/')
                | (context.github.event_name == 'workflow_dispatch'),
                needs=['linux', 'musllinux', 'windows', 'macos', 'sdist'],
                permissions=Permissions(id_token='write', contents='write'),  # noqa: S106
                environment=Environment('pypi'),
            ),
        },
    )


if __name__ == '__main__':
    generate_release('gluex_ccdb', 'crates/gluex-ccdb-py/Cargo.toml').dump('.github/workflows/maturin_gluex_ccdb.yml')
    generate_release('gluex_rcdb', 'crates/gluex-rcdb-py/Cargo.toml').dump('.github/workflows/maturin_gluex_rcdb.yml')
    generate_release('gluex_lumi', 'crates/gluex-lumi-py/Cargo.toml').dump('.github/workflows/maturin_gluex_lumi.yml')
