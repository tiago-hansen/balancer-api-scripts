import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv
import os

from utils import make_api_request


def main(spreadsheet_name, worksheet_name):
    API_URL = "https://api-v3.balancer.fi"

    # Google Sheets
    load_dotenv()
    SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")
    logging.info("Fetching Stable V3 pools with TVL >= 10k")

    # Query: Get all Stable V3 pools with minTvl >= 10k
    QUERY = """
    {
      poolGetPools(
        where: {
          chainIn: [ARBITRUM, AVALANCHE, BASE, GNOSIS, HYPEREVM, MAINNET, OPTIMISM, PLASMA, POLYGON]
          poolTypeIn: [STABLE]
          protocolVersionIn: [3]
          minTvl: 10000
        }
        orderBy: totalLiquidity
      ) {
        address
        chain
        hasErc4626
        poolTokens {
          symbol
          balanceUSD
        }
        dynamicData {
          totalLiquidity
        }
      }
    }
    """

    response = make_api_request(API_URL, QUERY)
    if response.status_code != 200:
        logging.error(
            f"Query failed with code {response.status_code}. {response.text}"
        )
        return
    logging.info("Data fetched successfully from the API.")

    pools = response.json()["data"]["poolGetPools"]
    logging.info(f"Found {len(pools)} Stable V3 pools with TVL >= 10k")

    # Filter for boosted pools (hasErc4626 = true)
    boosted_pools = [pool for pool in pools if pool.get("hasErc4626") is True]
    logging.info(f"Filtered to {len(boosted_pools)} boosted pools")

    rows = []

    for idx, pool in enumerate(boosted_pools):
        pool_address = pool.get("address", "")
        chain = pool.get("chain", "")
        pool_tokens = pool.get("poolTokens", [])
        dynamic_data = pool.get("dynamicData", {})
        tvl = float(dynamic_data.get("totalLiquidity", 0) or 0.0)

        logging.info(
            f"Processing pool {idx + 1}/{len(boosted_pools)}: {pool_address} ({chain})")

        # Extract tokens with balances from poolTokens (balanceUSD is already available)
        tokens_with_balances = []
        for token in pool_tokens:
            balance_usd = float(token.get("balanceUSD", 0) or 0.0)
            tokens_with_balances.append({
                "symbol": token.get("symbol", ""),
                "balanceUSD": balance_usd
            })

        # Sort tokens by balanceUSD descending
        tokens_data_sorted = sorted(
            tokens_with_balances,
            key=lambda x: float(x.get("balanceUSD", 0) or 0.0),
            reverse=True
        )

        # Extract up to 3 tokens with their percentages
        token_1 = ""
        token_1_pct = 0.0
        token_2 = ""
        token_2_pct = 0.0
        token_3 = ""
        token_3_pct = 0.0

        if len(tokens_data_sorted) > 0:
            token_1 = tokens_data_sorted[0].get("symbol", "")
            balance_1 = float(
                tokens_data_sorted[0].get("balanceUSD", 0) or 0.0)
            token_1_pct = (balance_1 / tvl) if tvl > 0 else 0.0

        if len(tokens_data_sorted) > 1:
            token_2 = tokens_data_sorted[1].get("symbol", "")
            balance_2 = float(
                tokens_data_sorted[1].get("balanceUSD", 0) or 0.0)
            token_2_pct = (balance_2 / tvl) if tvl > 0 else 0.0

        if len(tokens_data_sorted) > 2:
            token_3 = tokens_data_sorted[2].get("symbol", "")
            balance_3 = float(
                tokens_data_sorted[2].get("balanceUSD", 0) or 0.0)
            token_3_pct = (balance_3 / tvl) if tvl > 0 else 0.0

        # Build pool URL
        chain_lower = chain.lower() if chain else ""
        if chain_lower == "mainnet":
            chain_lower = "ethereum"
        pool_url = f"https://balancer.fi/pools/{chain_lower}/v3/{pool_address}"

        rows.append({
            "pool url": pool_url,
            "chain": chain,
            "token 1": token_1,
            "% token 1": token_1_pct,
            "token 2": token_2,
            "% token 2": token_2_pct,
            "token 3": token_3,
            "% token 3": token_3_pct,
            "TVL": tvl,
        })

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
        worksheet_name="Boosted 26.01.2026"
    )
