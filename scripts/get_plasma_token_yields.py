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
          address
        }
        dynamicData {
          aprItems {
            type
            rewardTokenAddress
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

    # Fetch
    response = requests.post(API_URL, json={"query": QUERY})
    if response.status_code != 200:
        logging.error(
            f"Query failed with code {response.status_code}. {response.text}"
        )
        return
    logging.info("Data fetched successfully from the API.")

    data = response.json()
    pools = (data.get("data") or {}).get("poolGetPools") or []

    rows = []

    for pool in pools:
        pool_tokens = pool.get("poolTokens") or []
        apr_items = ((pool.get("dynamicData") or {}).get("aprItems")) or []
        address = pool.get("address") or ""
        pool_url = f"https://balancer.fi/pools/plasma/v3/{address}"

        # Build pool display name: "Token 1 / Token 2 / Token ..."
        token_symbols = []
        for t in pool_tokens:
            sym = t.get("symbol") or ""
            token_symbols.append(sym)
        pool_display = " / ".join(token_symbols) if token_symbols else ""

        # Index APRs by rewardTokenAddress (lowercased) and sum in case of multiples
        apr_by_reward = {}
        for item in apr_items:
            raddr = item.get("rewardTokenAddress")
            if not raddr:
                continue
            try:
                apr_val = float(item.get("apr") or 0.0)
            except (TypeError, ValueError):
                apr_val = 0.0
            if apr_val == 0.0:
                continue
            key = raddr.lower()
            apr_by_reward[key] = apr_by_reward.get(key, 0.0) + apr_val

        # For each token in the pool, emit a row if it has a yield (reward address match)
        for t in pool_tokens:
            token_symbol = t.get("symbol") or ""
            token_addr = (t.get("address") or "").lower()
            if not token_addr:
                continue

            token_yield = apr_by_reward.get(token_addr, 0.0)
            if token_yield and token_yield != 0.0:
                rows.append(
                    {
                        "Token symbol": token_symbol,
                        "Token yield": f'{token_yield:.3f}',
                        "Pool": pool_display,
                        "Pool url": pool_url,
                    }
                )

    # Build final DataFrame (only tokens with yield appear)
    df = pd.DataFrame(rows)

    # Optional: sort by highest yield first, then token symbol
    if not df.empty:
        df = df.sort_values(["Token yield", "Token symbol"], ascending=[
                            False, True]).reset_index(drop=True)

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
        worksheet_name="Token yields",
    )
