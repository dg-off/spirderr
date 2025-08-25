#!/usr/bin/env python3
"""Merge multiple guides JSONL snapshots into a single deduplicated file.

Deduplication key: canonical_url; if multiple versions exist we keep the highest version.
If versions tie, prefer the record with is_updated True, else the latest (by last_seen/first_seen if present).

Usage (from project root):
  python merge_guides.py \
      --inputs backups/guides.*.jsonl data/guides.jsonl data/guides_full.jsonl \
      --output backups/guides_merged.jsonl

You can also just run with no args to auto-discover backups/guides.*.jsonl and data/guides*.jsonl.
"""
from __future__ import annotations
import argparse, glob, json, sys, os, re, datetime
from typing import Dict, Any, List

DEFAULT_GLOB_PATTERNS = [
    'backups/guides.*.jsonl',
    'data/guides.jsonl',
    'data/guides_full.jsonl',
]

TS_FORMATS = [
    '%Y-%m-%dT%H:%M:%S.%f%z',
    '%Y-%m-%dT%H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%S.%f',
    '%Y-%m-%dT%H:%M:%S',
]

def parse_ts(val: str | None):
    if not val or not isinstance(val, str):
        return None
    for fmt in TS_FORMATS:
        try:
            # allow Z
            v = val.replace('Z', '+00:00')
            return datetime.datetime.strptime(v, fmt)
        except Exception:
            continue
    return None


def choose_record(existing: dict, new: dict) -> dict:
    if existing is None:
        return new
    # Prefer higher version
    ev = existing.get('version') or 0
    nv = new.get('version') or 0
    if nv > ev:
        return new
    if ev > nv:
        return existing
    # Same version: prefer updated flag
    if new.get('is_updated') and not existing.get('is_updated'):
        return new
    if existing.get('is_updated') and not new.get('is_updated'):
        return existing
    # Same updated status: prefer newer last_seen timestamp
    e_last = parse_ts(existing.get('last_seen'))
    n_last = parse_ts(new.get('last_seen'))
    if e_last and n_last:
        if n_last > e_last:
            return new
        if e_last > n_last:
            return existing
    # Fall back to keep existing
    return existing


def iter_jsonl(path: str):
    try:
        with open(path, 'rb') as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception as e:
                    sys.stderr.write(f"[WARN] Failed parsing line {i} in {path}: {e}\n")
    except FileNotFoundError:
        return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--inputs', nargs='*', help='Explicit input JSONL files (glob patterns allowed).')
    ap.add_argument('--output', default='backups/guides_merged.jsonl', help='Output JSONL file path.')
    args = ap.parse_args()

    patterns = args.inputs if args.inputs else DEFAULT_GLOB_PATTERNS
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    # Deduplicate & keep stable order
    seen_paths = set()
    ordered_files = []
    for p in files:
        if os.path.isfile(p) and p not in seen_paths:
            seen_paths.add(p)
            ordered_files.append(p)

    if not ordered_files:
        print('No input files found.', file=sys.stderr)
        return 1

    print(f"[MERGE] Inputs ({len(ordered_files)}):")
    for f in ordered_files:
        print(f"  - {f}")

    merged: Dict[str, dict] = {}
    total = 0
    for f in ordered_files:
        for rec in iter_jsonl(f):
            total += 1
            key = rec.get('canonical_url')
            if not key:
                continue
            existing = merged.get(key)
            merged[key] = choose_record(existing, rec) if existing else rec

    out_path = args.output

    def _unique_path(path: str) -> str:
        """Return a path that does not exist yet by appending a timestamp (and counter if needed).

        Examples:
          backups/guides_merged.jsonl -> backups/guides_merged-20250825-123456.jsonl
        """
        dirpath = os.path.dirname(path) or '.'
        base = os.path.basename(path)
        name, ext = os.path.splitext(base)
        candidate = path
        if os.path.exists(candidate):
            ts = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
            candidate = os.path.join(dirpath, f"{name}-{ts}{ext}")
            i = 1
            while os.path.exists(candidate):
                candidate = os.path.join(dirpath, f"{name}-{ts}-{i}{ext}")
                i += 1
        return candidate

    # Ensure output dir exists first
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    final_out = _unique_path(out_path)
    # Use exclusive-create mode to avoid any accidental overwrite in race conditions
    with open(final_out, 'xb') as out:
        for rec in merged.values():
            out.write(json.dumps(rec, ensure_ascii=False).encode('utf-8') + b'\n')
    print(f"[MERGE] Wrote {len(merged)} unique guides (from {total} records) -> {final_out}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
