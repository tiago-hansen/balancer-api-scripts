import requests
import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv

import os
import datetime


def main(spreadsheet_name, worksheet_name):
    # API parameters
    API_URL = "https://api-v3.balancer.fi"
    QUERY = """
    {
      poolGetPools(
        orderBy: totalLiquidity,
        where: {
          minTvl: 10000
        }
      ) {
        chain
        poolTokens {
          symbol
        }
        type
        createTime
        dynamicData {
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
        chain = pool.get("chain", "")
        type = pool.get("type", "")
        address = pool.get("address") or ""
        pool_url = f"https://balancer.fi/pools/plasma/v3/{address}"

        # Define "pool pair" as a sring in the format "SYM1 / SYM2 / SYM3 ..."
        tokens = pool.get("poolTokens", [])
        pool_pair = " / ".join([t.get("symbol", "") for t in tokens])

        # Define creation date from createTime
        # createTime is a Unix timestamp in seconds
        create_time = pool.get("createTime", 0)
        if create_time:
            create_date = datetime.datetime.fromtimestamp(create_time).strftime(
                "%Y-%m-%d"
            )

        # TVL from dynamicData.totalLiquidity
        try:
            tvl = float(pool["dynamicData"].get("totalLiquidity") or 0.0)
        except (TypeError, ValueError, KeyError):
            tvl = 0.0

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
                    totalSwapVolume
                    totalSwapFee
                }}
            }}
        """
        sub_response = requests.post(API_URL, json={"query": subquery})
        snapshots = sub_response.json()["data"]["poolGetSnapshots"]
        if len(snapshots) >= 30:
            # Get 7d and 30d TVL change in %
            tvl_30d_ago = float(snapshots[-30]["totalLiquidity"] or 0.0)

            tvl_30d_change = ((tvl - tvl_30d_ago) /
                              tvl_30d_ago) if tvl_30d_ago else 0.0
            # Get 7d and 30d volume in USD
            volume_30d = sum(
                float(s["totalSwapVolume"] or 0.0) for s in snapshots[-30:])
            # Get 7d and 30d fees in USD
            fees_30d = sum(
                float(s["totalSwapFee"] or 0.0) for s in snapshots[-30:])
        elif len(snapshots) >= 7:
            tvl_7d_ago = float(snapshots[-7]["totalLiquidity"] or 0.0)
            tvl_7d_change = ((tvl - tvl_7d_ago) /
                             tvl_7d_ago) if tvl_7d_ago else 0.0
            volume_7d = sum(
                float(s["totalSwapVolume"] or 0.0) for s in snapshots[-7:])
            fees_7d = sum(float(s["totalSwapFee"] or 0.0)
                          for s in snapshots[-7:])
        else:
            tvl_7d_change = 0.0
            tvl_30d_change = 0.0
            volume_7d = 0.0
            volume_30d = 0.0
            fees_7d = 0.0
            fees_30d = 0.0

        rows.append(
            {
                "Pool pair": pool_pair,
                "Type": type,
                "Creation date": create_date,
                "TVL [$]": tvl,
                "7d TVL change [%]": tvl_7d_change,
                "30d TVL change [%]": tvl_30d_change,
                "7d Volume [$]": volume_7d,
                "30d Volume [$]": volume_30d,
                "7d Fees [$]": fees_7d,
                "30d Fees [$]": fees_30d,
                "Pool URL": pool_url,
            }
        )

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
        worksheet_name="17.10.2025",
    )
