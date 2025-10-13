import requests
import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv

import os


def main(spreadsheet_name, worksheet_name):
    # API parameters
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
          balanceUSD
        }
        dynamicData {
          totalLiquidity
          aprItems {
            type
            rewardTokenSymbol
            apr
          }
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

    # fetch
    response = requests.post(API_URL, json={"query": QUERY})
    if response.status_code != 200:
        logging.error(
            f"Query failed with code {response.status_code}. {response.text}"
        )
        return
    logging.info("Data fetched successfully from the API.")

    raw = response.json()["data"]["poolGetPools"]

    rows = []
    for pool in raw:
        tokens = pool.get("poolTokens", [])
        # Ensure we have at least two tokens to build a pair row
        if len(tokens) < 2:
            continue

        # Take the first two entries as Token1/Token2
        t1, t2 = tokens[0], tokens[1]
        sym1 = t1.get("symbol") or ""
        sym2 = t2.get("symbol") or ""

        # Convert balances (strings) to floats
        try:
            b1 = float(t1.get("balanceUSD") or 0.0)
        except ValueError:
            b1 = 0.0
        try:
            b2 = float(t2.get("balanceUSD") or 0.0)
        except ValueError:
            b2 = 0.0

        # TVL from dynamicData.totalLiquidity
        try:
            tvl = float(pool["dynamicData"].get("totalLiquidity") or 0.0)
        except (TypeError, ValueError, KeyError):
            tvl = 0.0

        # Balance percentage for each token
        pct1 = (b1 / tvl) if tvl else 0.0
        pct2 = (b2 / tvl) if tvl else 0.0

        # --- APR: avoid double-counting swap APR ---
        apr_items = (pool.get("dynamicData") or {}).get("aprItems") or []

        # Choose exactly one swap APR (prefer dynamic)
        swap_apr = None
        for typ in ("DYNAMIC_SWAP_FEE_24H", "SWAP_FEE_24H"):
            for it in apr_items:
                if it.get("type") == typ:
                    try:
                        swap_apr = float(it.get("apr") or 0.0)
                    except (TypeError, ValueError):
                        swap_apr = 0.0
                    break
            if swap_apr is not None:
                break

        # Sum all non-swap APR components
        non_swap_apr = 0.0
        for it in apr_items:
            t = it.get("type")
            if t in ("DYNAMIC_SWAP_FEE_24H", "SWAP_FEE_24H"):
                continue  # already accounted via swap_apr
            try:
                non_swap_apr += float(it.get("apr") or 0.0)
            except (TypeError, ValueError):
                pass

        apr_sum = (swap_apr or 0.0) + non_swap_apr

        address = pool.get("address") or ""
        pool_url = f"https://balancer.fi/pools/plasma/v3/{address}"

        rows.append(
            {
                "Pool pair": f"{sym1} / {sym2}",
                "Token1": sym1,
                "Token2": sym2,
                "Token1 [%]": f'{pct1:.3f}',
                "Token2 [%]": f'{pct2:.3f}',
                "Current TVL": f'{tvl:.0f}',
                "Current APR": f'{apr_sum:.3f}',
                "Pool URL": pool_url,
            }
        )

    # Build final DataFrame; optional sort by TVL desc
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(
            "Current TVL", ascending=False).reset_index(drop=True)

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
        worksheet_name="Incentives plan",
    )
