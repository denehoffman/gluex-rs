#!/usr/bin/env python3
"""Refresh bundled REST-version metadata from the Hall-D data-version service."""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

SOURCE_URL = 'https://halldweb.jlab.org/cgi-bin/data_monitoring/monitoring/dataVersions.py'
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / 'data' / 'rest_versions.tsv'


class DataVersionsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.periods: list[str] = []
        self.tables: list[list[list[str]]] = []
        self._in_run_period_select = False
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == 'select' and (attributes.get('name') == 'runPeriod' or attributes.get('id') == 'runPeriod'):
            self._in_run_period_select = True
        elif tag == 'option' and self._in_run_period_select:
            value = attributes.get('value')
            if value:
                query_period = parse_qs(urlparse(value).query).get('runPeriod', [value])[0]
                self.periods.append(query_period)
        elif tag == 'table':
            self._table = []
        elif tag == 'tr' and self._table is not None:
            self._row = []
        elif tag in {'td', 'th'} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == 'select':
            self._in_run_period_select = False
        elif tag in {'td', 'th'} and self._cell is not None and self._row is not None:
            self._row.append(' '.join(''.join(self._cell).split()))
            self._cell = None
        elif tag == 'tr' and self._row is not None and self._table is not None:
            if self._row:
                self._table.append(self._row)
            self._row = None
        elif tag == 'table' and self._table is not None:
            self.tables.append(self._table)
            self._table = None


def fetch(run_period: str | None = None) -> DataVersionsParser:
    url = SOURCE_URL
    if run_period is not None:
        url = f'{url}?{urlencode({"runPeriod": run_period})}'
    curl = shutil.which('curl')
    if curl is None:
        message = 'curl is required to refresh REST-version metadata'
        raise RuntimeError(message)
    source = subprocess.run(  # noqa: S603
        [curl, '-fsSL', url],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    parser = DataVersionsParser()
    parser.feed(source)
    return parser


def parse_context(context: str) -> tuple[str, str]:
    fields = dict(part.split('=', 1) for part in context.split() if '=' in part)
    timestamp = fields.get('calibtime')
    if timestamp is None:
        message = f'CCDB context has no calibtime: {context!r}'
        raise ValueError(message)
    components = [int(component) for component in re.findall(r'\d+', timestamp)]
    components.extend([0] * (6 - len(components)))
    year, month, day, hour, minute, second = components[:6]
    normalized = f'{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z'
    return fields.get('variation', 'default'), normalized


def reconstruction_rows(parser: DataVersionsParser, run_period: str) -> list[list[str | int]]:
    table = next(table for table in parser.tables if table and 'CCDB Context' in table[0] and 'Data Type' in table[0])
    header = {name: index for index, name in enumerate(table[0])}
    rows: list[list[str | int]] = []
    for row in table[1:]:
        if row[header['Data Type']] != 'recon':
            continue
        revision = int(row[header['Revision']].removeprefix('ver'))
        variation, timestamp = parse_context(row[header['CCDB Context']])
        rows.append([run_period, revision, variation, timestamp])
    return rows


def main() -> None:
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT)
    args = argument_parser.parse_args()

    periods = fetch().periods
    rows_by_version: dict[tuple[str, int], list[str | int]] = {}
    for run_period in periods:
        for row in reconstruction_rows(fetch(run_period), run_period):
            key = (str(row[0]), int(row[1]))
            previous = rows_by_version.get(key)
            if previous is None or str(row[3]) > str(previous[3]):
                rows_by_version[key] = row
    rows = sorted(rows_by_version.values(), key=lambda row: (str(row[0]), int(row[1])))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(f'{args.output.suffix}.tmp')
    with temporary.open('w', encoding='utf-8', newline='') as output:
        writer = csv.writer(output, delimiter='\t', lineterminator='\n')
        writer.writerow(['run_period', 'revision', 'variation', 'timestamp'])
        writer.writerows(rows)
    temporary.replace(args.output)
    print(f'wrote {len(rows)} REST versions to {args.output}')


if __name__ == '__main__':
    main()
