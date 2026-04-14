#!/usr/bin/env python3
"""Check that tracing-sensitive hotspots use the shared tracing helpers."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

HOTSPOT_FILES = [
    'rust/lance-encoding/src/decoder.rs',
    'rust/lance-table/src/utils/stream.rs',
    'rust/lance/src/utils/future.rs',
    'rust/lance/src/dataset/fragment.rs',
    'rust/lance/src/io/exec/filtered_read.rs',
    'rust/lance/src/io/exec/scan.rs',
    'rust/lance/src/io/exec/pushdown_scan.rs',
]


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    suggestion: str


RULES = [
    Rule(
        name='raw tokio spawn',
        pattern=re.compile(r'tokio::(?:task::)?spawn\('),
        suggestion='use spawn_in_current_span(...) or spawn_in_span(...)',
    ),
    Rule(
        name='manual boxed future span capture',
        pattern=re.compile(r'\.in_current_span\(\)\s*\.boxed\('),
        suggestion='use boxed_in_current_span(...) or ReadBatchTask::from_future(...)',
    ),
    Rule(
        name='manual boxed stream span capture',
        pattern=re.compile(r'\.stream_in_current_span\(\)\s*\.boxed\('),
        suggestion='use boxed_stream_in_current_span(...)',
    ),
]


def line_number(text: str, index: int) -> int:
    return text.count('\n', 0, index) + 1


def line_text(text: str, index: int) -> str:
    start = text.rfind('\n', 0, index)
    end = text.find('\n', index)
    if start == -1:
        start = 0
    else:
        start += 1
    if end == -1:
        end = len(text)
    return text[start:end].strip()


def main() -> int:
    violations: list[str] = []

    for relative_path in HOTSPOT_FILES:
        file_path = ROOT / relative_path
        if not file_path.exists():
            violations.append(f'{relative_path}: file does not exist')
            continue

        text = file_path.read_text(encoding='utf-8')
        for rule in RULES:
            for match in rule.pattern.finditer(text):
                lineno = line_number(text, match.start())
                snippet = line_text(text, match.start())
                violations.append(
                    f'{relative_path}:{lineno}: {rule.name}; {rule.suggestion}\n'
                    f'    {snippet}'
                )

    if violations:
        print('Tracing context policy violations found:\n')
        for violation in violations:
            print(f'- {violation}')
        return 1

    print('Tracing context policy passed.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
