"""Atomic JSON writes with retries for Windows (file locked by editor, indexer, or AV)."""

from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import Any, Optional


def write_json_atomic(
    path: Path,
    data: Any,
    *,
    indent: int = 4,
    retries: int = 18,
    base_delay: float = 0.35,
) -> None:
    path = path.resolve()
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, indent=indent, ensure_ascii=False) + "\n"
    legacy_tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        if legacy_tmp.is_file():
            legacy_tmp.unlink()
    except OSError:
        pass

    last: Optional[BaseException] = None
    pid = os.getpid()
    for attempt in range(retries):
        tmp = parent / f".{path.stem}.{pid}.{random.randint(1, 999_999_999)}.tmp"
        try:
            tmp.write_text(text, encoding="utf-8")
            os.replace(str(tmp), str(path))
            return
        except (OSError, PermissionError) as e:
            last = e
            try:
                if tmp.is_file():
                    tmp.unlink()
            except OSError:
                pass
            time.sleep(base_delay * (1.0 + attempt * 0.12))
    assert last is not None
    raise last
