#!/usr/bin/env python3
"""
Smoke test for eBay Trading API application keys (no user sign-in, no user tokens).

Calls GeteBayOfficialTime against https://api.ebay.com/ws/api.dll with headers:
  X-EBAY-API-APP-NAME, X-EBAY-API-DEV-NAME, X-EBAY-API-CERT-NAME

This does not fetch listing prices or URLs. For public marketplace search (prices + item
links only, no member OAuth), use the Buy Browse API with an application access token
(client_credentials grant) — same App ID + Cert ID as client_id / client_secret per eBay.

Reference: https://developer.ebay.com/api-docs/user-guides/static/make-a-call/using-xml.html
"""

from __future__ import annotations

import argparse
import os
import sys
import textwrap
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

DEFAULT_ENV = Path(__file__).resolve().parent / "ebay_listing_checker.env"
ENV_EXAMPLE = Path(__file__).resolve().parent / "ebay_listing_checker.env.example"


def _local_tag(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    raw = path.read_text(encoding="utf-8", errors="replace")
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        key, _, val = s.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if not key:
            continue
        if key not in os.environ or os.environ[key] == "":
            os.environ[key] = val


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    return (v or "").strip()


def trading_endpoint() -> str:
    if _env("EBAY_USE_SANDBOX", "false").lower() in ("1", "true", "yes"):
        return "https://api.sandbox.ebay.com/ws/api.dll"
    return "https://api.ebay.com/ws/api.dll"


def build_geteBayOfficial_time_xml() -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<GeteBayOfficialTimeRequest xmlns="urn:ebay:apis:eBLBaseComponents">\n'
        "</GeteBayOfficialTimeRequest>\n"
    )


def post_trading(*, xml_body: str, call_name: str) -> tuple[int, str]:
    app_id = _env("EBAY_APP_ID")
    dev_id = _env("EBAY_DEV_ID")
    cert_id = _env("EBAY_CERT_ID")
    if not app_id or not dev_id or not cert_id:
        print(
            "Missing EBAY_APP_ID / EBAY_DEV_ID / EBAY_CERT_ID. "
            f"Copy {ENV_EXAMPLE.name} to {DEFAULT_ENV.name} and fill keys.",
            file=sys.stderr,
            flush=True,
        )
        raise ValueError("missing EBAY_APP_ID / EBAY_DEV_ID / EBAY_CERT_ID")

    compat = _env("EBAY_COMPATIBILITY_LEVEL", "1271")
    site_id = _env("EBAY_SITE_ID", "0")

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "X-EBAY-API-CALL-NAME": call_name,
        "X-EBAY-API-SITEID": site_id,
        "X-EBAY-API-COMPATIBILITY-LEVEL": compat,
        "X-EBAY-API-APP-NAME": app_id,
        "X-EBAY-API-DEV-NAME": dev_id,
        "X-EBAY-API-CERT-NAME": cert_id,
    }

    url = trading_endpoint()
    req = urllib.request.Request(
        url,
        data=xml_body.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return e.code, err_body


def parse_ack_and_time(xml_text: str) -> tuple[str, Optional[str], list[str]]:
    """Returns (ack, timestamp_or_none, errors)."""
    errors: list[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return "ParseError", None, [xml_text[:500]]

    ack = ""
    ts: Optional[str] = None
    for el in root.iter():
        tag = _local_tag(el.tag)
        if tag == "Ack" and el.text:
            ack = el.text.strip()
        if tag == "Timestamp" and el.text:
            ts = el.text.strip()
        if tag == "Errors":
            parts = []
            for ch in el:
                ct = _local_tag(ch.tag)
                if ch.text and ct in ("ErrorCode", "ErrorClassification", "ShortMessage", "LongMessage"):
                    parts.append(ch.text.strip())
            if parts:
                errors.append(" | ".join(parts))
    return ack, ts, errors


def main() -> int:
    ap = argparse.ArgumentParser(
        description="eBay Trading API key smoke test (GeteBayOfficialTime, no user OAuth)"
    )
    ap.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV,
        help=f"Dotenv-style file (default: {DEFAULT_ENV})",
    )
    args = ap.parse_args()

    load_env_file(args.env_file.resolve())

    print("Endpoint:", trading_endpoint(), flush=True)
    print(
        "Env file:",
        args.env_file.resolve(),
        "(exists)" if args.env_file.is_file() else "(missing; using process env only)",
        flush=True,
    )

    xml_body = build_geteBayOfficial_time_xml()
    try:
        status, response = post_trading(xml_body=xml_body, call_name="GeteBayOfficialTime")
    except ValueError:
        return 1

    print("HTTP status:", status, flush=True)
    print("Response (first 800 chars):", flush=True)
    print(response[:800], flush=True)

    ack, ts, errors = parse_ack_and_time(response)
    print("\nParsed Ack:", ack, flush=True)
    if ts:
        print("Official eBay time (Timestamp):", ts, flush=True)
    if errors:
        print("\nAPI errors:", flush=True)
        for e in errors:
            print(" ", e, flush=True)

    if ack.upper() == "SUCCESS" or ack.upper() == "WARNING":
        print("\nOK — Trading API accepted the application keyset (no user token).", flush=True)
        return 0

    print(
        textwrap.dedent(
            """

            If you see credential errors:
              - Confirm EBAY_APP_ID, EBAY_DEV_ID, EBAY_CERT_ID match production (or set EBAY_USE_SANDBOX=true for sandbox keys).
              - Keys are only used here for GeteBayOfficialTime. Public listing search uses a different API (Browse + application token).

            Docs: https://developer.ebay.com/api-docs/user-guides/static/make-a-call/using-xml.html
            """
        ).strip(),
        flush=True,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
