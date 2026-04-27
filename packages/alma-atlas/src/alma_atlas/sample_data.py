"""Bundled sample-data SQLite snapshot for Atlas first-touch experience.

Per design doc Track 3b: ships a pre-populated Atlas graph (mock Snowflake +
dbt + Looker) so a cold dev can `pip install alma-atlas && alma-atlas sample install`
and have Cursor / Claude Desktop seeing a real-shaped data stack in 60 seconds —
no warehouse credentials, no scan, no network.

The snapshot is built by `scripts/generate_sample_data.py` and committed at
`data/atlas-sample.db.gz` next to this module. Refreshed only on major
releases per the design doc spec (target <10MB compressed, ~20 assets across
3 mock sources).
"""

from __future__ import annotations

import gzip
import shutil
from pathlib import Path

_BUNDLED_DATA_PATH = Path(__file__).parent / "data" / "atlas-sample.db.gz"


def bundled_snapshot_path() -> Path:
    """Return the bundled `atlas-sample.db.gz` path inside the wheel."""
    return _BUNDLED_DATA_PATH


def install_sample(target_db_path: Path, *, overwrite: bool = False) -> Path:
    """Install the bundled sample SQLite snapshot at the given target path.

    Decompresses `atlas-sample.db.gz` from the package into `target_db_path`.
    Creates the parent directory if missing.

    Args:
        target_db_path: Absolute path where the SQLite file should be written.
            Typically `~/.alma-atlas/atlas.db`.
        overwrite: If False (default), raises FileExistsError when the target
            already exists. If True, replaces the existing file.

    Returns:
        The target path written.

    Raises:
        FileNotFoundError: bundled snapshot is missing (build issue).
        FileExistsError: target exists and overwrite=False.
    """
    if not _BUNDLED_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Bundled sample snapshot missing at {_BUNDLED_DATA_PATH}. "
            "Did the wheel build skip the data/ directory?"
        )
    if target_db_path.exists() and not overwrite:
        raise FileExistsError(
            f"Target {target_db_path} already exists. "
            "Pass overwrite=True (or `--force` on the CLI) to replace."
        )

    target_db_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(_BUNDLED_DATA_PATH, "rb") as src, open(target_db_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return target_db_path
