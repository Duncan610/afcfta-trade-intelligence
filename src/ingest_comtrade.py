"""
Bronze-layer ingestion: UN Comtrade bilateral trade flow data.

Pulls annual trade data for the countries / HS chapters / years defined in
config.yaml and lands it as a raw Delta table with an ingestion timestamp.

Requires a free API key from https://comtradedeveloper.un.org/
set as the COMTRADE_API_KEY environment variable. Without a key, calls fall
back to the keyless preview endpoint (capped at 500 records/call).
"""

import os
import time
import requests
import yaml
from datetime import datetime, timezone

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)

API_KEY = os.environ.get("COMTRADE_API_KEY")
BASE_URL = CONFIG["comtrade"]["base_url"]

ISO3_TO_M49 = {
    "KEN": "404",
    "NGA": "566",
    "EGY": "818",
    "ZAF": "710",
    "GHA": "288",
}


def fetch_comtrade(reporter_iso3: str, hs_code: str, year: int) -> list[dict]:
    """Fetch one reporter/HS-chapter/year slice from UN Comtrade."""
    reporter_code = ISO3_TO_M49[reporter_iso3]
    type_code = CONFIG["comtrade"]["type"]
    freq_code = CONFIG["comtrade"]["freq"]
    cl_code = CONFIG["comtrade"]["clCode"]

    url = f"{BASE_URL}/{type_code}/{freq_code}/{cl_code}"
    params = {
        "reporterCode": reporter_code,
        "period": year,
        "cmdCode": hs_code,
        "flowCode": "M,X",  # imports and exports
        "partnerCode": "0",  # World; refine to specific partners later
    }
    headers = {"Ocp-Apim-Subscription-Key": API_KEY} if API_KEY else {}

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code == 429:
        # Free tier rate limit -- back off and retry once
        time.sleep(5)
        resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", [])


def ingest_bronze():
    """Pull all configured slices and write raw records to bronze."""
    all_records = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for country in CONFIG["countries"]:
        for chapter in CONFIG["hs_chapters"]:
            for year in CONFIG["years"]:
                records = fetch_comtrade(country["iso3"], chapter["code"], year)
                for r in records:
                    r["_ingested_at"] = ingested_at
                    r["_source"] = "un_comtrade"
                all_records.extend(records)
                print(f"{country['iso3']} / HS{chapter['code']} / {year}: {len(records)} rows")

    # TODO: replace with spark.createDataFrame(all_records).write.format("delta")
    #       .mode("append").saveAsTable("afcfta_trade.bronze.comtrade_raw")
    return all_records


if __name__ == "__main__":
    rows = ingest_bronze()
    print(f"Total rows pulled: {len(rows)}")
