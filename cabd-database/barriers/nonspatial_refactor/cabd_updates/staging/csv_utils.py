from __future__ import annotations

from collections.abc import Sequence


def is_blank_csv_row(row: Sequence[str]) -> bool:
    return not any(value.strip() for value in row)
