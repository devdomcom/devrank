#!/usr/bin/env python3
"""Terminal-first CLI tool for testing API endpoints locally.

Uses FastAPI TestClient (httpx under the hood) — no running server needed.
To hit a live server instead, pass --base-url http://localhost:8000.

Usage:
    uv run python -m impact.api.test_endpoints              # in-process (default)
    uv run python -m impact.api.test_endpoints --base-url http://localhost:8000  # live server
"""
from __future__ import annotations

import argparse
import sys

import httpx


def _in_process_client() -> httpx.Client:
    from starlette.testclient import TestClient

    from impact.api.app import app

    return TestClient(app)  # type: ignore[return-value]


def _live_client(base_url: str) -> httpx.Client:
    return httpx.Client(base_url=base_url)


def run_tests(client: httpx.Client) -> bool:
    passed = 0
    failed = 0

    def _check(method: str, path: str, expected_status: int, expected_body: dict | None = None) -> None:  # noqa: ANN001
        nonlocal passed, failed
        resp = client.request(method, path)
        ok = resp.status_code == expected_status
        if expected_body and ok:
            ok = resp.json() == expected_body

        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {method} {path} -> {resp.status_code}")
        if ok:
            print(f"         {resp.json()}")
            passed += 1
        else:
            print(f"         Expected {expected_status}, got {resp.status_code}: {resp.text}")
            failed += 1

    print("Running endpoint tests\n")

    _check(
        "GET",
        "/api/v1/health",
        200,
        {"status": "healthy", "service": "devrank-impact"},
    )

    _check("GET", "/api/v1/metrics", 200)

    # Test new roles endpoints (list + detail; follows metrics pattern)
    _check("GET", "/api/v1/roles", 200)
    _check("GET", "/api/v1/roles/senior_dev", 200)
    _check("GET", "/api/v1/roles/nonexistent", 404)  # proper error

    # Test compare endpoint (new; triggers validation on missing body/windows)
    _check("POST", "/api/v1/metrics/compare", 422)  # missing body -> error

    # Test dump upload (new; triggers on no file)
    _check("POST", "/api/v1/dumps/upload", 422)  # no file -> error

    # Test compute endpoint + deps chain (triggers validation/error handler)
    _check("POST", "/api/v1/metrics/compute", 422)  # missing body -> error

    # Test dynamic single metric + dep injection (chain + report params; error on bad dump)
    _check(
        "GET",
        "/api/v1/metrics/active_weeks?dump_path=/tmp&user_login=test",
        400,  # dep/bundle error
    )

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Test DevRank API endpoints from the terminal.")
    parser.add_argument(
        "--base-url",
        default=None,
        help="Hit a live server instead of in-process TestClient (e.g. http://localhost:8000).",
    )
    args = parser.parse_args()

    if args.base_url:
        client = _live_client(args.base_url)
    else:
        client = _in_process_client()

    with client:
        ok = run_tests(client)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
