"""Central paths for dataset sidecar reports and sync logs (kept out of repo root)."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = REPO_ROOT / "reports"
DATASET_SIDECAR_REPORT_DIR = REPORTS_DIR / "dataset-sidecars"
PRICECHARTING_SKIPS_LOG = REPORTS_DIR / "pricecharting_sync_skips.json"
WIZARD_SYNC_SKIPS_LOG = REPORTS_DIR / "pokemon_wizard_sync_skips.json"
GEMRATE_SCRAPE_SKIPPED_TXT = REPORTS_DIR / "gemrate_scrape_skipped.txt"
PRICECHARTING_SEGMENT_PROBE_REPORT = REPORTS_DIR / "pricecharting_segment_probe_report.json"


def dataset_sidecar_report_path(dataset_json: Path, suffix: str) -> Path:
    """
    Sidecar JSON next to a dataset export name, stored under reports/dataset-sidecars/.

    `suffix` must include the leading dot, e.g. '.merge_report.json' or '.ebay_sold_sync_report.json'.
    """
    name = Path(dataset_json).name
    return DATASET_SIDECAR_REPORT_DIR / (name + suffix)
