#!/usr/bin/env python3
"""Add imports that PyO3 cannot infer for external annotation strings."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib.util
import io
import zipfile
from pathlib import Path

EXTERNAL_IMPORTS = 'from collections.abc import Sequence\nimport laddu\n'


def finalize(text: str) -> str:
    """Return an idempotently finalized generated ``generation.pyi``."""
    if EXTERNAL_IMPORTS in text:
        return text
    header_end = text.find('\n\n')
    if header_end < 0:
        message = 'generated stub has no import header'
        raise ValueError(message)
    return text[: header_end + 1] + EXTERNAL_IMPORTS + text[header_end + 1 :]


def finalize_file(path: Path) -> None:
    path.write_text(finalize(path.read_text()))


def finalize_wheel(path: Path) -> None:
    with zipfile.ZipFile(path, 'r') as archive:
        members = {info.filename: archive.read(info.filename) for info in archive.infolist()}
    targets = [name for name in members if name.endswith('/generation.pyi')]
    if len(targets) != 1:
        message = f'expected one generated generation.pyi in {path}, found {len(targets)}'
        raise ValueError(message)
    target = targets[0]
    members[target] = finalize(members[target].decode()).encode()
    records = [name for name in members if name.endswith('.dist-info/RECORD')]
    if len(records) != 1:
        message = f'expected one RECORD in {path}, found {len(records)}'
        raise ValueError(message)
    record = records[0]
    rows = []
    for name, data in members.items():
        if name == record:
            continue
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
        rows.append((name, f'sha256={digest}', str(len(data))))
    rows.append((record, '', ''))
    output = io.StringIO(newline='')
    csv.writer(output, lineterminator='\n').writerows(rows)
    members[record] = output.getvalue().encode()
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)


def installed_stub() -> Path:
    spec = importlib.util.find_spec('gluex')
    if spec is None or spec.origin is None:
        message = 'the installed gluex extension could not be located'
        raise RuntimeError(message)
    return Path(spec.origin).with_name('generation.pyi')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', nargs='*', type=Path)
    args = parser.parse_args()
    paths = args.paths or [installed_stub()]
    for path in paths:
        if path.suffix == '.whl':
            finalize_wheel(path)
        else:
            finalize_file(path)


if __name__ == '__main__':
    main()
