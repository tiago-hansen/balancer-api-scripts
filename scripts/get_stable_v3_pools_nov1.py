import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import time

from utils import make_api_request


def get_timestamp_for_date(date_str):
    """Convert date string (YYYY-MM-DD) to Unix timestamp."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def get_tvl_for_date(snapshots, target_timestamp):
    """Get TVL from snapshots closest to target timestamp."""
    if not snapshots:
        return 0.0

    # Find the snapshot closest to the target timestamp
    closest = min(snapshots, key=lambda x: abs(
        int(x["timestamp"]) - int(target_timestamp)))
    return float(closest.get("totalLiquidity", 0) or 0.0)


def get_snapshot_for_date(snapshots, target_timestamp):
    """Get snapshot closest to target timestamp."""
    if not snapshots:
        return None

    # Find the snapshot closest to the target timestamp
    closest = min(snapshots, key=lambda x: abs(
        int(x["timestamp"]) - int(target_timestamp)))
    return closest


def get_price_for_date(price_history, target_timestamp):
    """Get price from price history closest to target timestamp."""
    if not price_history or not price_history.get("prices"):
        return None

    prices = price_history["prices"]
    if not prices:
        return None

    # Find the price closest to the target timestamp
    closest = min(prices, key=lambda x: abs(
        int(x["timestamp"]) - int(target_timestamp)))
    return float(closest.get("price", 0) or 0.0)


def main(spreadsheet_name, worksheet_name, nov_1st_date="2025-11-01"):
    API_URL = "https://api-v3.balancer.fi"

    nov_1st = datetime.strptime(nov_1st_date, "%Y-%m-%d")
    nov_2nd = nov_1st + timedelta(days=1)

    # Convert to timestamp (use start of day)
    nov_1st_start = datetime.combine(nov_1st.date(), datetime.min.time())
    nov_1st_ts = int(nov_1st_start.timestamp())

    # Nov 2nd timestamp (start of day) - pools created before this
    nov_2nd_start = datetime.combine(nov_2nd.date(), datetime.min.time())
    nov_2nd_ts = int(nov_2nd_start.timestamp())

    # Google Sheets
    load_dotenv()
    SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")
    logging.info(
        f"Target date: Nov 1st={nov_1st.date()}, Filter: pools created before Nov 2nd")

    # Query: Get all Stable V3 pools created before Nov 2nd with tokens
    QUERY = f"""
    {{
      poolGetPools(
        where: {{
          chainIn: [ARBITRUM, AVALANCHE, BASE, GNOSIS, HYPEREVM, MAINNET, OPTIMISM, PLASMA, POLYGON]
          poolTypeIn: [STABLE]
          protocolVersionIn: [3]
          createTime: {{
            lt: {nov_2nd_ts}
          }}
        }}
        orderBy: totalLiquidity
      ) {{
        address
        chain
        hasErc4626
        poolTokens {{
          symbol
          address
        }}
      }}
    }}
    """

    response = make_api_request(API_URL, QUERY)
    if response.status_code != 200:
        logging.error(
            f"Query failed with code {response.status_code}. {response.text}"
        )
        return
    logging.info("Data fetched successfully from the API.")

    pools = response.json()["data"]["poolGetPools"]
    logging.info(f"Found {len(pools)} Stable V3 pools created before Nov 2nd")

    # Filter for boosted pools (hasErc4626 = true)
    boosted_pools = [pool for pool in pools if pool.get("hasErc4626") is True]
    logging.info(f"Filtered to {len(boosted_pools)} boosted pools")

    # Collect all unique token addresses grouped by chain
    tokens_by_chain = {}
    for pool in boosted_pools:
        chain = pool.get("chain", "")
        tokens = pool.get("poolTokens", [])
        if chain not in tokens_by_chain:
            tokens_by_chain[chain] = set()
        for token in tokens:
            token_address = token.get("address", "")
            if token_address:
                tokens_by_chain[chain].add(token_address)

    # Build token info map: (chain, address) -> {symbol, address}
    token_info_map = {}
    for pool in boosted_pools:
        chain = pool.get("chain", "")
        tokens = pool.get("poolTokens", [])
        for token in tokens:
            token_address = token.get("address", "")
            if token_address:
                key = (chain, token_address)
                if key not in token_info_map:
                    token_info_map[key] = {
                        "symbol": token.get("symbol", ""),
                        "address": token_address
                    }

    # Batch fetch historical prices for all tokens, grouped by chain
    logging.info("Fetching historical prices for all tokens...")
    total_tokens = sum(len(addrs) for addrs in tokens_by_chain.values())
    logging.info(f"Total unique tokens across all chains: {total_tokens}")

    price_cache = {}  # (chain, address) -> price for Nov 1st

    # Batch size to avoid query limits (adjust if needed)
    BATCH_SIZE = 50

    for chain, addresses in tokens_by_chain.items():
        if not addresses:
            continue

        addresses_list = list(addresses)
        logging.info(
            f"Fetching prices for {len(addresses_list)} tokens on {chain}")

        # Process in batches
        for i in range(0, len(addresses_list), BATCH_SIZE):
            batch = addresses_list[i:i + BATCH_SIZE]
            addresses_str = '", "'.join(batch)

            prices_query = f"""
            {{
              tokenGetHistoricalPrices(
                addresses: ["{addresses_str}"]
                chain: {chain}
                range: NINETY_DAY
              ) {{
                address
                chain
                prices {{
                  price
                  timestamp
                }}
              }}
            }}
            """

            prices_response = make_api_request(API_URL, prices_query)
            if prices_response.status_code == 200:
                response_json = prices_response.json()
                # Check for GraphQL errors
                if "errors" in response_json:
                    logging.warning(
                        f"GraphQL errors for chain {chain} batch {i//BATCH_SIZE + 1}: {response_json['errors']}")
                    continue

                prices_data = response_json.get(
                    "data", {}).get("tokenGetHistoricalPrices", [])
                logging.debug(
                    f"Received {len(prices_data)} price histories for chain {chain} batch {i//BATCH_SIZE + 1}")

                for price_history in prices_data:
                    token_address = price_history.get("address", "")
                    price = get_price_for_date(price_history, nov_1st_ts)
                    if price is not None and price > 0:
                        price_cache[(chain, token_address)] = price
                    else:
                        logging.debug(
                            f"No valid price found for token {token_address} on {chain} (price: {price})")
            else:
                logging.warning(
                    f"Failed to fetch prices for chain {chain} batch {i//BATCH_SIZE + 1}: {prices_response.status_code}")
                if prices_response.status_code == 400:
                    logging.warning(f"Response: {prices_response.text}")
                # Log the actual query for debugging
                logging.debug(f"Query was: {prices_query}")

    logging.info(f"Cached prices for {len(price_cache)} tokens")

    rows = []

    for idx, pool in enumerate(boosted_pools):
        pool_address = pool.get("address", "")
        chain = pool.get("chain", "")
        pool_tokens = pool.get("poolTokens", [])

        logging.info(
            f"Processing pool {idx + 1}/{len(boosted_pools)}: {pool_address} ({chain})")

        # Query snapshots to get TVL and token amounts for Nov 1st
        snapshots_query = f"""
        {{
          poolGetSnapshots(
            id: "{pool_address}"
            range: NINETY_DAYS
            chain: {chain}
          ) {{
            totalLiquidity
            timestamp
            amounts
          }}
        }}
        """

        snapshots_response = make_api_request(API_URL, snapshots_query)
        snapshots = []
        if snapshots_response.status_code == 200:
            snapshots_data = snapshots_response.json().get(
                "data", {}).get("poolGetSnapshots", [])
            snapshots = snapshots_data
        else:
            logging.warning(
                f"Failed to fetch snapshots for pool {pool_address}: {snapshots_response.status_code}")

        # Get snapshot for Nov 1st
        snapshot_nov_1st = get_snapshot_for_date(snapshots, nov_1st_ts)
        tvl_nov_1st = get_tvl_for_date(snapshots, nov_1st_ts)

        # Skip pool if TVL on Nov 1st was less than 10k
        if tvl_nov_1st < 10000:
            logging.info(
                f"Skipping pool {pool_address} ({chain}): TVL on Nov 1st was {tvl_nov_1st:.2f} (< 10k)")
            continue

        # Match amounts with tokens and calculate USD balances
        tokens_with_balances = []
        if snapshot_nov_1st and snapshot_nov_1st.get("amounts"):
            amounts = snapshot_nov_1st.get("amounts", [])
            # Match amounts[i] with pool_tokens[i]
            for i, token in enumerate(pool_tokens):
                if i < len(amounts):
                    token_address = token.get("address", "")
                    # Handle amounts as strings (they might be in wei format)
                    try:
                        amount = float(amounts[i] or 0.0)
                    except (ValueError, TypeError):
                        amount = 0.0

                    # Get price from cache
                    price = price_cache.get((chain, token_address), 0.0)
                    balance_usd = amount * price

                    tokens_with_balances.append({
                        "symbol": token.get("symbol", ""),
                        "balanceUSD": balance_usd
                    })
        else:
            logging.warning(
                f"No amounts data available in snapshot for {pool_address} on Nov 1st")

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
            token_1_pct = (balance_1 / tvl_nov_1st) if tvl_nov_1st > 0 else 0.0

        if len(tokens_data_sorted) > 1:
            token_2 = tokens_data_sorted[1].get("symbol", "")
            balance_2 = float(
                tokens_data_sorted[1].get("balanceUSD", 0) or 0.0)
            token_2_pct = (balance_2 / tvl_nov_1st) if tvl_nov_1st > 0 else 0.0

        if len(tokens_data_sorted) > 2:
            token_3 = tokens_data_sorted[2].get("symbol", "")
            balance_3 = float(
                tokens_data_sorted[2].get("balanceUSD", 0) or 0.0)
            token_3_pct = (balance_3 / tvl_nov_1st) if tvl_nov_1st > 0 else 0.0

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
            "TVL on Nov 1st": tvl_nov_1st,
        })

        # Add small buffer between pools
        time.sleep(0.1)

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
        worksheet_name="Boosted Nov 1st"
    )
