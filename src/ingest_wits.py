"""
Bronze-layer ingestion: WITS/TRAINS tariff data (preferential + MFN, HS6 level).

Uses the `world_trade_data` Python wrapper around the WITS REST API.
No API key required.

Note: AfCFTA-specific preferential rates may be sparsely reported for some
country pairs, since AfCFTA preferential trade reporting is still maturing.
This is logged explicitly rather than silently backfilled -- see README.
"""

import yaml
import world_trade_data as wits
from datetime import datetime, timezone

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)


def fetch_tariffs(reporter_iso3: str, year: int):
    """Fetch reported tariff data (MFN + preferential where available)."""
    try:
        df = wits.get_tariff_reported(
            reporter=reporter_iso3,
            partner="wld",
            product="all",
            year=year,
        )
        return df
    except Exception as e:
        print(f"  WARNING: no tariff data for {reporter_iso3} / {year}: {e}")
        return None


def ingest_bronze():
    """Pull tariff data for all configured countries/years and tag with metadata."""
    frames = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for country in CONFIG["countries"]:
        for year in CONFIG["years"]:
            df = fetch_tariffs(country["iso3"], year)
            if df is not None and not df.empty:
                df["_ingested_at"] = ingested_at
                df["_source"] = "wits_trains"
                frames.append(df)
                print(f"{country['iso3']} / {year}: {len(df)} tariff rows")
            else:
                print(f"{country['iso3']} / {year}: NO DATA -- flag for gold-layer documentation")

    # TODO: concat frames, filter to configured HS chapters, write to
    #       spark table afcfta_trade.bronze.wits_tariffs_raw
    return frames


if __name__ == "__main__":
    results = ingest_bronze()
    print(f"Total country/year slices with data: {len(results)}")
