"""
Bronze-layer ingestion: WITS/TRAINS tariff data (preferential + MFN, HS6 level).

NOTE ON DATA SOURCE: this reads manually exported CSVs from the WITS web
portal (https://wits.worldbank.org/tariff/trains/) rather than calling the
WITS API, because the public `world_trade_data` Python wrapper returned
Invalid_Reporter errors for every tested country -- including known-good
reporters like the USA -- indicating a broken/stale client library rather
than a genuine data gap. This is a documented, deliberate scoping decision,
not a workaround being hidden.

Expects one CSV per country at data/bronze/wits_tariffs_<ISO3>.csv,
exported from the WITS "Tariff and Trade Analysis" advanced query tool.
"""

import glob
import os
import pandas as pd
import yaml
from datetime import datetime, timezone

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)

BRONZE_DIR = "data/bronze"


def ingest_bronze():
    """Read all manually exported WITS tariff CSVs and tag with metadata."""
    frames = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    expected_isos = {c["iso3"] for c in CONFIG["countries"]}
    found_isos = set()

    for path in glob.glob(os.path.join(BRONZE_DIR, "wits_tariffs_*.csv")):
        iso3 = os.path.basename(path).replace("wits_tariffs_", "").replace(".csv", "").upper()
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="cp1252")
        df["_ingested_at"] = ingested_at
        df["_source"] = "wits_manual_export"
        df["_reporter_iso3"] = iso3
        frames.append(df)
        found_isos.add(iso3)
        print(f"{iso3}: {len(df)} tariff rows (from {path})")

    missing = expected_isos - found_isos
    if missing:
        print(f"WARNING: no CSV found for {sorted(missing)} -- "
              f"export these from WITS before running the silver transform")

    return frames


if __name__ == "__main__":
    results = ingest_bronze()
    total_rows = sum(len(df) for df in results)
    print(f"Total rows across {len(results)} country file(s): {total_rows}")
