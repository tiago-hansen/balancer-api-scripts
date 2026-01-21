import requests
import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv
import os
import datetime

from utils import make_api_request


def main(spreadsheet_name, worksheet_name):
    # API parameters
    API_URL = "https://api-v3.balancer.fi"
    QUERY = """
    {
        poolGetPools(
            orderBy: totalLiquidity,
            where: {
                minTvl: 50000,
                protocolVersionIn: [3]
            }
        ) {
            chain
            address
            dynamicData {
                totalLiquidity
                volume24h
                swapFee
            }
            address
            hook {
                type
            }
            poolTokens {
                symbol
            }
            createTime
            type
        }
    }
    """

    # Service account file for Google Sheets
    load_dotenv()
    SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")

    response = make_api_request(API_URL, QUERY)
    if response.status_code != 200:
        logging.error(
            f"Query failed with code {response.status_code}. {response.text}"
        )
        return
    logging.info("Data fetched successfully from the API.")

    raw = response.json()["data"]["poolGetPools"]

    rows = []
    for pool in raw:
        chain = pool.get("chain", "")
        chain_lower = chain.lower() if chain != "MAINNET" else "ethereum"
        pool_type = pool.get("type", "")
        hook = pool.get("hook")
        is_stable_surge = hook.get("type") == "STABLE_SURGE" if hook else False
        address = pool.get("address", "")
        pool_url = f"https://balancer.fi/pools/{chain_lower}/v3/{address}"
        tvl = float(pool["dynamicData"].get("totalLiquidity", 0.0))
        swap_fee = float(pool["dynamicData"].get("swapFee", 0.0))
        volume_24h = float(pool["dynamicData"].get("volume24h", 0.0))
        pool_tokens = pool.get("poolTokens", [])
        pool_pair = " / ".join([t.get("symbol", "") for t in pool_tokens])
        create_time = pool.get("createTime", 0)
        if create_time:
            create_date = datetime.datetime.fromtimestamp(create_time).strftime(
                "%Y-%m-%d"
            )
        else:
            create_date = ""

        # Get data from pool snapshots
        subquery = f"""
            {{
                poolGetSnapshots(
                    id: "{address}"
                    chain: {chain}
                    range: THIRTY_DAYS
                ) {{
                    timestamp
                    totalLiquidity
                    volume24h
                }}
            }}
        """
        sub_response = make_api_request(API_URL, subquery)
        if sub_response.status_code != 200:
            logging.warning(
                f"Query failed with code {sub_response.status_code}. {sub_response.text}"
            )
            continue
        snapshots = sub_response.json().get("data", {}).get("poolGetSnapshots", [])
        if len(snapshots) >= 30:
            tvl_30d_ago = float(snapshots[-30].get("totalLiquidity", 0.0))
        else:
            tvl_30d_ago = 0.0

        # Calculate 7d Volume and 30d Volume by summing the volume24h fields from the last 7/30 days of snapshots
        # If there are fewer than 7 or 30 snapshots, sum all available snapshots
        if len(snapshots) >= 7:
            volume_7d = sum(float(s.get("volume24h", 0.0))
                            for s in snapshots[-7:])
        else:
            volume_7d = sum(float(s.get("volume24h", 0.0))
                            for s in snapshots)
        if len(snapshots) >= 30:
            volume_30d = sum(float(s.get("volume24h", 0.0))
                             for s in snapshots[-30:])
        else:
            volume_30d = sum(float(s.get("volume24h", 0.0))
                             for s in snapshots)

        rows.append(
            {
                "Chain": chain.lower().capitalize(),
                "Pool pair": pool_pair,
                "Type": pool_type,
                "Creation date": create_date,
                "TVL": tvl,
                "TVL 30d ago": tvl_30d_ago,
                "24h Volume": volume_24h,
                "7d Volume": volume_7d,
                "30d Volume": volume_30d,
                "Swap Fee": swap_fee,
                "Is Stable Surge": "Yes" if is_stable_surge else "No",
                "Pool URL": pool_url,
            }
        )

        print(f"Processed pool {pool_pair} - {len(rows)}/{len(raw)} pools")

    df = pd.DataFrame(rows)

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
        spreadsheet_name="BD Monthly data",
        worksheet_name="16.01.2026",
    )
