#!/usr/bin/env python3
"""Simple throughput benchmarks for Argus subsystems (run locally)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from argus_core.extraction.engine import ExtractionEngine
from argus_core.storage import HtmlStorage


def bench_extraction(iterations: int = 100) -> dict:
    fixture = (Path(__file__).resolve().parents[1] / "tests/fixtures/sample.html").read_bytes()
    engine = ExtractionEngine()
    start = time.perf_counter()
    for _ in range(iterations):
        engine.extract(fixture, "https://example.com")
    elapsed = time.perf_counter() - start
    return {"iterations": iterations, "seconds": round(elapsed, 3), "ops_per_sec": round(iterations / elapsed, 1)}


def bench_checksum(iterations: int = 10000) -> dict:
    data = b"x" * 50000
    start = time.perf_counter()
    for _ in range(iterations):
        HtmlStorage.compute_checksum(data)
    elapsed = time.perf_counter() - start
    return {"iterations": iterations, "seconds": round(elapsed, 3), "ops_per_sec": round(iterations / elapsed, 1)}


def bench_json_serialize(iterations: int = 10000) -> dict:
    payload = {"url": "https://example.com", "title": "Test", "links": ["https://a.com"] * 50}
    start = time.perf_counter()
    for _ in range(iterations):
        json.dumps(payload)
        hashlib.sha256(json.dumps(payload).encode()).hexdigest()
    elapsed = time.perf_counter() - start
    return {"iterations": iterations, "seconds": round(elapsed, 3), "ops_per_sec": round(iterations / elapsed, 1)}


def main() -> None:
    results = {
        "extraction": bench_extraction(),
        "checksum": bench_checksum(),
        "json_serialize": bench_json_serialize(),
    }
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
