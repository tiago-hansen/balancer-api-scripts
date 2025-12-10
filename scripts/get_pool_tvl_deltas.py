import requests
import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

import os


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
        x["timestamp"] - target_timestamp))
    return float(closest.get("totalLiquidity", 0) or 0.0)


def make_api_request(api_url, query, max_retries=3, retry_delay=1):
    """Make an API request with retry logic for rate limiting."""
    for attempt in range(max_retries):
        try:
            response = requests.post(api_url, json={"query": query})

            # If rate limited, wait and retry
            if response.status_code == 429:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logging.warning(
                    f"Rate limited (429), waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
                continue

            # If successful, return response
            if response.status_code == 200:
                return response

            # For other errors, log and return
            logging.warning(
                f"API request failed with status {response.status_code}")
            return response

        except Exception as e:
            logging.warning(
                f"API request exception: {e}, retry {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (2 ** attempt))
            else:
                raise

    # If all retries failed, return the last response
    return response


def calculate_delta(events, start_timestamp, end_timestamp):
    """Calculate delta (ADD - REMOVE) for a time period.

    Delta = sum of all ADDs - sum of all REMOVEs in the time period.
    """
    adds = 0.0
    removes = 0.0

    if not events:
        return 0.0

    for event in events:
        event_timestamp = event.get("timestamp", 0)

        # Handle both seconds and milliseconds timestamps
        # Timestamps in seconds for 2024-2025 are around 1.7-1.8 billion
        # Timestamps in milliseconds would be around 1.7-1.8 trillion
        # If timestamp > 1e12 (1 trillion), it's likely in milliseconds
        if event_timestamp > 1e12:
            event_timestamp = int(event_timestamp / 1000)
        else:
            event_timestamp = int(event_timestamp)

        # Include events within the time range (inclusive on both ends)
        if start_timestamp <= event_timestamp <= end_timestamp:
            value_usd = float(event.get("valueUSD", 0) or 0.0)
            event_type = event.get("type", "")

            # Only process ADD and REMOVE events
            if event_type == "ADD":
                adds += value_usd
            elif event_type == "REMOVE":
                removes += value_usd

    return adds - removes


def calculate_withdrawal_analysis(events, start_timestamp, end_timestamp):
    """Calculate number of addresses responsible for 70% of withdrawals and the address if count = 1.

    Args:
        events: List of events already filtered by time range (from filter_events_by_range)
        start_timestamp: Start timestamp for the period (for reference, events are already filtered)
        end_timestamp: End timestamp for the period (for reference, events are already filtered)

    Returns:
        tuple: (count_of_addresses, address_if_count_is_1)
    """
    # Filter only REMOVE events (events are already filtered by time range)
    remove_events = [event for event in events if event.get(
        "type", "") == "REMOVE"]

    if not remove_events:
        return (0, "")

    # Group by userAddress and sum valueUSD
    user_totals = {}
    for event in remove_events:
        user_address = event.get("userAddress", "")
        value_usd = float(event.get("valueUSD", 0) or 0.0)

        if user_address:
            if user_address not in user_totals:
                user_totals[user_address] = 0.0
            user_totals[user_address] += value_usd

    if not user_totals:
        return (0, "")

    # Calculate total withdrawals
    total_withdrawals = sum(user_totals.values())

    if total_withdrawals == 0:
        return (0, "")

    # Sort users by total value descending
    sorted_users = sorted(user_totals.items(),
                          key=lambda x: x[1], reverse=True)

    # Calculate cumulative percentage until reaching 70%
    target_percentage = 0.70
    cumulative_value = 0.0
    address_count = 0

    for user_address, user_total in sorted_users:
        cumulative_value += user_total
        address_count += 1

        if cumulative_value / total_withdrawals >= target_percentage:
            # If only one address is needed, return that address
            address_if_one = user_address if address_count == 1 else ""
            return (address_count, address_if_one)

    # If we've gone through all addresses and still haven't reached 70%,
    # return the count of all addresses
    address_if_one = sorted_users[0][0] if address_count == 1 else ""
    return (address_count, address_if_one)


def main(spreadsheet_name, worksheet_name, nov_2nd_date="2025-11-02", nov_5th_date="2025-11-05"):
    # API parameters
    API_URL = "https://api-v3.balancer.fi"

    # Calculate dates
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)

    nov_2nd = datetime.strptime(nov_2nd_date, "%Y-%m-%d")
    nov_5th = datetime.strptime(nov_5th_date, "%Y-%m-%d")

    # Convert to timestamps
    # Use start of day for start dates and end of day for end dates
    nov_2nd_start = datetime.combine(nov_2nd.date(), datetime.min.time())
    nov_5th_start = datetime.combine(nov_5th.date(), datetime.min.time())
    nov_5th_end = datetime.combine(nov_5th.date(), datetime.max.time())
    seven_days_ago_start = datetime.combine(
        seven_days_ago.date(), datetime.min.time())
    today_end = datetime.combine(today.date(), datetime.max.time())

    nov_2nd_ts = int(nov_2nd_start.timestamp())
    nov_5th_ts_start = int(nov_5th_start.timestamp())
    nov_5th_ts_end = int(nov_5th_end.timestamp())
    seven_days_ago_ts = int(seven_days_ago_start.timestamp())
    today_ts = int(today_end.timestamp())

    # Service account file for Google Sheets
    load_dotenv()
    SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")
    logging.info(
        f"Date ranges: Nov 2nd={nov_2nd.date()}, Nov 5th={nov_5th.date()}, 7d ago={seven_days_ago.date()}, Today={today.date()}")

    # Query 1: Get all pools with at least 10k TVL
    QUERY = """
    {
      poolGetPools(
        where: {
          chainIn: [ARBITRUM, AVALANCHE, BASE, GNOSIS, HYPEREVM, MAINNET, OPTIMISM, PLASMA, POLYGON],
          minTvl: 100000
        }
        orderBy: totalLiquidity
      ) {
        dynamicData {
          totalLiquidity
          volume24h
          swapFee
        }
        symbol
        type
        chain
        id
        protocolVersion
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
    logging.info(f"Found {len(pools)} pools with TVL >= 100k")

    rows = []

    for idx, pool in enumerate(pools):
        pool_id = pool.get("id", "")
        chain = pool.get("chain", "")
        pool_symbol = pool.get("symbol", "")
        pool_type = pool.get("type", "")
        protocol_version = pool.get("protocolVersion", "")

        # Get current data
        dynamic_data = pool.get("dynamicData", {})
        tvl_today = float(dynamic_data.get("totalLiquidity", 0) or 0.0)
        volume_24h = float(dynamic_data.get("volume24h", 0) or 0.0)
        swap_fee = float(dynamic_data.get("swapFee", 0) or 0.0)

        # Build pool URL
        chain_lower = chain.lower() if chain else ""
        if chain_lower == "mainnet":
            chain_lower = "ethereum"
        pool_url = f"https://balancer.fi/pools/{chain_lower}/v{protocol_version}/{pool_id}"

        logging.info(
            f"Processing pool {idx + 1}/{len(pools)}: {pool_symbol} ({chain})")

        # Query events with pagination to get all events, then filter by date range
        # The API limits results to 1000 events per query, so we need to paginate
        def fetch_all_events():
            """Fetch all events for a pool using pagination."""
            all_events = []
            skip = 0
            page_size = 1000

            while True:
                events_query = f"""
                {{
                  poolEvents(
                    where: {{
                      poolId: "{pool_id}",
                      chainIn: [{chain}]
                    }}
                    first: {page_size}
                    skip: {skip}
                  ) {{
                    poolId
                    timestamp
                    valueUSD
                    type
                    userAddress
                  }}
                }}
                """

                response = make_api_request(API_URL, events_query)
                if response.status_code != 200:
                    logging.warning(
                        f"Failed to fetch events for pool {pool_id} at skip {skip}: {response.status_code}")
                    # If rate limited, wait longer before continuing
                    if response.status_code == 429:
                        time.sleep(5)
                    break

                # Add small delay between pagination requests to avoid rate limiting
                time.sleep(0.1)

                result = response.json()
                errors = result.get("errors", [])
                if errors:
                    logging.warning(
                        f"GraphQL errors for pool {pool_id}: {errors}")
                    break

                events_data = result.get("data", {}).get("poolEvents", [])
                if not events_data:
                    break

                all_events.extend(events_data)

                # If we got fewer than page_size, we've reached the end
                if len(events_data) < page_size:
                    break

                skip += page_size

                # Safety limit to avoid infinite loops (increase for pools with many events)
                if skip > 200000:  # Max 200k events (200 pages)
                    logging.warning(
                        f"Reached safety limit for pool {pool_id} at {skip} events")
                    break

            return all_events

        # Filter events by date range and type (ADD/REMOVE only)
        def filter_events_by_range(events, start_ts, end_ts):
            """Filter events by timestamp range and type."""
            filtered = []
            for event in events:
                event_timestamp = int(event.get("timestamp", 0))
                event_type = event.get("type", "")

                # Only include ADD and REMOVE events within the time range
                if (start_ts <= event_timestamp <= end_ts and
                        event_type in ["ADD", "REMOVE"]):
                    filtered.append(event)
            return filtered

        # Fetch all events once
        all_pool_events = fetch_all_events()

        # Filter events for each date range
        events_nov_2_to_5 = filter_events_by_range(
            all_pool_events, nov_2nd_ts, nov_5th_ts_end)

        # For Nov 5th to 7d ago, determine the correct range
        if seven_days_ago_ts < nov_5th_ts_start:
            events_nov_5_to_7d = filter_events_by_range(
                all_pool_events, seven_days_ago_ts, nov_5th_ts_end)
        else:
            events_nov_5_to_7d = filter_events_by_range(
                all_pool_events, nov_5th_ts_start, seven_days_ago_ts)

        events_7d_to_today = filter_events_by_range(
            all_pool_events, seven_days_ago_ts, today_ts)

        # Check if we got events and log sample for debugging
        if idx < 3:
            logging.info(
                f"Pool {pool_symbol}: Total events fetched={len(all_pool_events)}")
            if all_pool_events:
                sample_event = all_pool_events[0]
                logging.info(
                    f"Sample event: type={sample_event.get('type')}, "
                    f"timestamp={sample_event.get('timestamp')}, "
                    f"valueUSD={sample_event.get('valueUSD')}"
                )
            logging.info(
                f"Filtered events - Nov 2-5: {len(events_nov_2_to_5)}, "
                f"Nov 5-7d: {len(events_nov_5_to_7d)}, "
                f"7d-Today: {len(events_7d_to_today)}"
            )

        # Query 3: Get pool snapshots for last 90 days
        snapshots_query = f"""
        {{
          poolGetSnapshots(
            id: "{pool_id}"
            range: NINETY_DAYS
            chain: {chain}
          ) {{
            totalLiquidity
            timestamp
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
                f"Failed to fetch snapshots for pool {pool_id}: {snapshots_response.status_code}")

        # Add delay between pools to avoid rate limiting
        time.sleep(0.2)

        # Get TVL for specific dates (use start of day timestamps)
        tvl_nov_2nd = get_tvl_for_date(snapshots, nov_2nd_ts)
        tvl_nov_5th = get_tvl_for_date(snapshots, nov_5th_ts_start)
        tvl_7d_ago = get_tvl_for_date(snapshots, seven_days_ago_ts)

        # Calculate deltas using the pre-filtered events for each range
        # Nov 2nd to Nov 5th: from Nov 2nd 00:00:00 to Nov 5th 23:59:59
        delta_nov_2_to_5 = calculate_delta(
            events_nov_2_to_5, nov_2nd_ts, nov_5th_ts_end)

        # Calculate withdrawal analysis for Nov 2nd to 5th
        count_nov_2_to_5, address_nov_2_to_5 = calculate_withdrawal_analysis(
            events_nov_2_to_5, nov_2nd_ts, nov_5th_ts_end)

        # Nov 5th to 7d ago: from Nov 5th 00:00:00 to 7d ago 23:59:59
        delta_nov_5_to_7d = calculate_delta(
            events_nov_5_to_7d, nov_5th_ts_start, seven_days_ago_ts)

        # Calculate withdrawal analysis for Nov 5th to 7d ago
        count_nov_5_to_7d, address_nov_5_to_7d = calculate_withdrawal_analysis(
            events_nov_5_to_7d, nov_5th_ts_start, seven_days_ago_ts)

        # 7d ago to Today: from 7d ago 00:00:00 to Today 23:59:59
        delta_7d_to_today = calculate_delta(
            events_7d_to_today, seven_days_ago_ts, today_ts)

        # Calculate withdrawal analysis for 7d ago to Today
        count_7d_to_today, address_7d_to_today = calculate_withdrawal_analysis(
            events_7d_to_today, seven_days_ago_ts, today_ts)

        # Debug logging for first few pools
        if idx < 3:
            logging.info(
                f"Pool {pool_symbol}: "
                f"Delta Nov 2-5={delta_nov_2_to_5:.2f}, "
                f"Delta Nov 5-7d={delta_nov_5_to_7d:.2f}, "
                f"Delta 7d-Today={delta_7d_to_today:.2f}"
            )

        rows.append({
            "Pool": pool_symbol,
            "TVL (Nov 2nd)": tvl_nov_2nd,
            "Delta Remove-Add (Nov 2nd - 5th)": delta_nov_2_to_5,
            "Addresses (70% removes Nov 2nd - 5th)": count_nov_2_to_5,
            "Most remover (if 1 user Nov 2nd - 5th)": address_nov_2_to_5,
            "TVL (Nov 5th)": tvl_nov_5th,
            "Delta Remove-Add (Nov 5th - 7d ago)": delta_nov_5_to_7d,
            "Addresses (70% removes Nov 5th - 7d ago)": count_nov_5_to_7d,
            "Most remover (if 1 user Nov 5th - 7d ago)": address_nov_5_to_7d,
            "TVL (7d ago)": tvl_7d_ago,
            "Delta Remove-Add (7d ago - Today)": delta_7d_to_today,
            "Addresses (70% removes 7d ago - Today)": count_7d_to_today,
            "Most remover (if 1 user 7d ago - Today)": address_7d_to_today,
            "TVL (Today)": tvl_today,
            "Volume (24h)": volume_24h,
            "Swap Fee": swap_fee,
            "Pool Type": pool_type,
            "Chain": chain,
            "Version": protocol_version,
            "Url": pool_url,
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
        spreadsheet_name="Exploit analysis",
        worksheet_name="TVL"
    )
