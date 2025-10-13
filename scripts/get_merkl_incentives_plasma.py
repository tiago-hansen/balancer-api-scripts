import requests
import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv

import os


def main(spreadsheet_name, worksheet_name):
    API_URL = "https://api-v3.balancer.fi"
    QUERY = """
    {
      poolGetPools(
        where: {
          chainIn: PLASMA
        }
      ) {
        poolTokens {
          symbol
        }
        dynamicData {
          aprItems {
            type
            apr
          }
          totalLiquidity
        }
        address
      }
    }
    """

    # Service account file for Google Sheets
    load_dotenv()
    SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")

    # Fetch
    resp = requests.post(API_URL, json={"query": QUERY})
    if resp.status_code != 200:
        logging.error(
            f"Query failed with code {resp.status_code}. {resp.text}")
        return
    logging.info("Data fetched successfully from the API.")

    raw = (resp.json().get("data") or {}).get("poolGetPools") or []

    rows = []
    for pool in raw:
        tokens = pool.get("poolTokens") or []
        apr_items = ((pool.get("dynamicData") or {}).get("aprItems")) or []
        total_liquidity_raw = (pool.get("dynamicData")
                               or {}).get("totalLiquidity")
        address = pool.get("address") or ""

        # Build pool name: "Token A / Token B / Token ..."
        pool_name = " / ".join([(t.get("symbol") or "")
                               for t in tokens if t.get("symbol")])

        # Sum MERKL APRs (if there are multiple entries, combine them)
        merkl_apr = 0.0
        for item in apr_items:
            if item.get("type") == "MERKL":
                try:
                    merkl_apr += float(item.get("apr") or 0.0)
                except (TypeError, ValueError):
                    continue

        # Only include pools with a positive Merkl APR
        if merkl_apr and merkl_apr > 0.0:
            # TVL as float (may come as string)
            try:
                tvl = float(total_liquidity_raw or 0.0)
            except (TypeError, ValueError):
                tvl = 0.0

            rows.append(
                {
                    "Pool": pool_name,
                    "APR (MERKL)": f'{merkl_apr:.3f}',
                    "TVL": tvl,
                    "Pool url": f"https://balancer.fi/pools/plasma/v3/{address}",
                }
            )

    df = pd.DataFrame(rows)

    # Optional: sort by APR desc then TVL desc
    if not df.empty:
        df = df.sort_values(["APR (MERKL)", "TVL"], ascending=[
                            False, False]).reset_index(drop=True)

    # Google Sheets
    client = pygsheets.authorize(service_account_file=SERVICE_FILE)
    logging.info("Authorized with Google Sheets API.")

    sh = client.open(spreadsheet_name)
    wks = sh.worksheet_by_title(worksheet_name)
    wks.clear()
    wks.set_dataframe(df, (1, 1))
    logging.info("Data written to Google Sheets successfully.")


if __name__ == "__main__":
    main(
        spreadsheet_name="Plasma pool proposal",
        worksheet_name="Merkl incentives",
    )
