from yamloom import (
    Environment,
    Events,
    Job,
    Permissions,
    PullRequestEvent,
    PushEvent,
    Workflow,
    WorkflowDispatchEvent,
    script,
    sync,
)
from yamloom.actions.github.artifacts import DownloadArtifact
from yamloom.actions.github.release import ReleasePlease
from yamloom.actions.github.scm import Checkout
from yamloom.actions.toolchains.python import SetupUV
from yamloom.actions.toolchains.rust import SetupRust
from yamloom.expressions import context
from yamloom.workflows import MaturinBuildSuite

build_condition = context.github.ref.startswith('refs/tags/') | (context.github.event_name == 'workflow_dispatch')

build_jobs = MaturinBuildSuite(
    python_profile='all',
    needs=['build-test-check'],
    condition=build_condition,
    sccache=~context.github.ref.startswith('refs/tags/'),
    minimum_python='3.10',
    args=('--release', '--out', 'dist', '--generate-stubs'),
    manifest_path='Cargo.toml',
).jobs()

release_workflow = Workflow(
    name='Build and Release (Python)',
    on=Events(
        push=PushEvent(branches=['main'], tags=['*']),
        pull_request=PullRequestEvent(),
        workflow_dispatch=WorkflowDispatchEvent(),
    ),
    jobs={
        'build-test-check': Job(
            runs_on='ubuntu-latest',
            steps=[
                Checkout(),
                SetupRust(components=['clippy']),
                SetupUV(python_version='3.10'),
                script('cargo clippy --all-targets --all-features -- -D warnings'),
                script('cargo test'),
                script('cargo check --all-targets --features python'),
                script(
                    'uv venv',
                    '. .venv/bin/activate',
                    'echo PATH=$PATH >> $GITHUB_ENV',
                    'uvx maturin develop --uv --generate-stubs',
                ),
                script('uv pip install pytest'),
                script('uvx ruff check .yamloom.py python'),
                script('uvx ty check python/tests'),
                script('uv run pytest python/tests'),
            ],
        ),
        **build_jobs,
        'release_python': Job(
            name='Release (Python)',
            runs_on='ubuntu-22.04',
            steps=[
                DownloadArtifact(),
                SetupUV(),
                script(
                    'uv publish --trusted-publishing always wheels-*/*',
                    permissions=Permissions(id_token='write', contents='write'),  # noqa: S106
                ),
            ],
            condition=build_condition,
            needs=list(build_jobs.keys()),
            environment=Environment('pypi'),
        ),
        'release_rust': Job(
            name='Release (Rust)',
            runs_on='ubuntu-22.04',
            steps=[
                Checkout(),
                SetupRust(),
                script(f'cargo publish --token {context.secrets.CARGO_REGISTRY_TOKEN}'),
            ],
            condition=build_condition,
            needs=list(build_jobs.keys()),
        ),
    },
)

release_please_workflow = Workflow(
    name='Release Please',
    on=Events(
        push=PushEvent(
            branches=['main'],
        ),
    ),
    jobs={
        'release-please': Job(
            steps=[
                ReleasePlease(
                    token=context.secrets.RELEASE_PLEASE,
                    id='release',
                ),
            ],
            runs_on='ubuntu-latest',
        )
    },
)

if __name__ == '__main__':
    sync(
        {
            'maturin_gluex_rs.yml': release_workflow,
            'release-please.yml': release_please_workflow,
        }
    )
