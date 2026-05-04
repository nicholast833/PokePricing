"""Run sync_pokemon_wizard.py with --only-set-codes from a newline-separated list (e.g. sets_missing_wizard_price_history_codes.txt)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    p = ROOT / "sets_missing_wizard_price_history_codes.txt"
    if not p.is_file():
        print(f"Missing {p}", file=sys.stderr)
        return 2
    codes = [ln.strip().lower() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not codes:
        print("No set codes in file", file=sys.stderr)
        return 2
    s = ",".join(sorted(set(codes)))
    cmd = [sys.executable, str(ROOT / "scrape" / "sync_pokemon_wizard.py"), "--sleep", "0.12", "--only-set-codes", s]
    print(f"Syncing {len(codes)} set_codes ...", flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


if __name__ == "__main__":
    raise SystemExit(main())
