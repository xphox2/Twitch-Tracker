#!/usr/bin/env python3
"""Repair daily archive CSVs to a stable schema.

The tracker historically wrote archive rows using `stats.keys()` as the CSV
fieldnames. When new fields were added, later rows no longer matched the header
row, which breaks consumers like `dashboard.html`.

This script rewrites each `data/archive/daily/stats_archive_YYYY-MM-DD.csv` to a
canonical header order, preserving existing values.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from pathlib import Path


ARCHIVE_FIELDS = [
    "timestamp",
    "subscribers",
    "followers",
    "bits_today",
    "bits_total",
    "subs_today_revenue",
    "subs_total_revenue",
    "artist_bits_monthly",
    "artist_subs_monthly",
    "artist_gifted_monthly",
    "artist_raise_30",
    "artist_yearly",
    "total_revenue",
    "tiers",
    "gifted_tiers",
]


def _looks_like_dict(s: str) -> bool:
    t = (s or "").strip()
    return t.startswith("{") and t.endswith("}") and ("1000" in t)


def _to_float(s: str) -> float:
    try:
        return float((s or "").strip())
    except Exception:
        return 0.0


def parse_row(fields: list[str]) -> dict:
    if not fields:
        return {}

    out = {k: 0 for k in ARCHIVE_FIELDS}
    out["tiers"] = {}
    out["gifted_tiers"] = {}

    # Timestamp always first.
    out["timestamp"] = (fields[0] or "").strip()
    rest = [f.strip() for f in fields[1:]]

    # Peel off up to 2 dict fields from the end.
    dicts: list[str] = []
    while rest and _looks_like_dict(rest[-1]) and len(dicts) < 2:
        dicts.append(rest.pop(-1))
    dicts.reverse()
    if len(dicts) == 1:
        out["tiers"] = dicts[0]
    elif len(dicts) == 2:
        out["tiers"] = dicts[0]
        out["gifted_tiers"] = dicts[1]

    # Remaining fields should be numeric, in known order.
    numeric_keys_full = [
        "subscribers",
        "followers",
        "bits_today",
        "bits_total",
        "subs_today_revenue",
        "subs_total_revenue",
        "artist_bits_monthly",
        "artist_subs_monthly",
        "artist_gifted_monthly",
        "artist_raise_30",
        "artist_yearly",
        "total_revenue",
    ]
    numeric_keys_legacy = [k for k in numeric_keys_full if k != "artist_gifted_monthly"]

    keys = numeric_keys_full if len(rest) == len(numeric_keys_full) else numeric_keys_legacy
    for k, v in zip(keys, rest):
        out[k] = _to_float(v)

    # Fill missing derived values if needed.
    if out.get("total_revenue", 0.0) == 0.0 and (out.get("bits_total", 0.0) or out.get("subs_total_revenue", 0.0)):
        out["total_revenue"] = round(float(out.get("bits_total", 0.0)) + float(out.get("subs_total_revenue", 0.0)), 2)

    return out


def repair_file(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return False
        rows = [r for r in reader if r and any(c.strip() for c in r)]

    # If it's already the canonical schema and the first data row matches, skip.
    if header == ARCHIVE_FIELDS:
        return False

    repaired = []
    for r in rows:
        repaired.append(parse_row(r))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(path.suffix + f".bak.{ts}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    os.replace(path, backup)

    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ARCHIVE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in repaired:
            writer.writerow({k: row.get(k, 0) for k in ARCHIVE_FIELDS})

    os.replace(tmp, path)
    print(f"Repaired {path.name} (backup: {backup.name})")
    return True


def main() -> int:
    daily_dir = Path("data") / "archive" / "daily"
    if not daily_dir.exists():
        print(f"Not found: {daily_dir}")
        return 1

    files = sorted(daily_dir.glob("stats_archive_*.csv"))
    if not files:
        print("No daily archive CSVs found.")
        return 0

    changed = 0
    for p in files:
        try:
            if repair_file(p):
                changed += 1
        except Exception as e:
            print(f"Failed to repair {p.name}: {e}")

    print(f"Done. Repaired {changed} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
