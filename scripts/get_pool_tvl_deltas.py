import requests
import pandas as pd
import pygsheets
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from collections import defaultdict, deque

import os


class RateLimiter:
    """Rate limiter that enforces a maximum of N requests per T seconds."""

    def __init__(self, max_requests=30, time_window=10):
        """
        Args:
            max_requests: Maximum number of requests allowed
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = deque()
        self.lock = False

    def wait_if_needed(self):
        """Wait if we've reached the rate limit."""
        now = time.time()

        # Remove timestamps outside the time window
        while self.request_times and self.request_times[0] < now - self.time_window:
            self.request_times.popleft()

        # If we've hit the limit, wait until the oldest request expires
        if len(self.request_times) >= self.max_requests:
            oldest_request_time = self.request_times[0]
            wait_time = self.time_window - \
                (now - oldest_request_time) + 0.1  # Add small buffer
            if wait_time > 0:
                logging.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                # Clean up again after waiting
                now = time.time()
                while self.request_times and self.request_times[0] < now - self.time_window:
                    self.request_times.popleft()

    def record_request(self):
        """Record that a request was made."""
        self.request_times.append(time.time())


# Global rate limiter instance
_rate_limiter = RateLimiter(max_requests=30, time_window=10)


def normalize_timestamp(ts):
    """Normalize timestamp to seconds (handle milliseconds)."""
    if ts > 1e12:
        return int(ts / 1000)
    return int(ts)


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
    """Make an API request with retry logic for rate limiting.

    Enforces rate limit of 30 requests per 10 seconds.
    """
    global _rate_limiter

    for attempt in range(max_retries):
        try:
            # Check and wait if we're approaching rate limit
            _rate_limiter.wait_if_needed()

            # Make the request
            response = requests.post(api_url, json={"query": query})

            # Record the request
            _rate_limiter.record_request()

            # Handle 429 (Too Many Requests)
            if response.status_code == 429:
                # Try to extract retry-after header if available
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                        logging.warning(
                            f"Rate limited (429), Retry-After header: {wait_time}s")
                    except ValueError:
                        wait_time = retry_delay * (2 ** attempt)
                else:
                    # Exponential backoff with jitter
                    wait_time = retry_delay * \
                        (2 ** attempt) + (time.time() % 1)
                    logging.warning(
                        f"Rate limited (429), waiting {wait_time:.2f}s before retry {attempt + 1}/{max_retries}")

                time.sleep(wait_time)

                # Reset rate limiter state after 429 to be more conservative
                _rate_limiter.request_times.clear()
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


def process_events_for_ranges(events, ranges):
    """Process events in a single pass for all date ranges.

    Args:
        events: List of all events (already normalized)
        ranges: Dict mapping range_name -> (start_ts, end_ts)

    Returns:
        Dict mapping range_name -> {
            'delta': float,
            'remove_by_user': dict,
            'total_removes': float
        }
    """
    results = {}
    for range_name, (start_ts, end_ts) in ranges.items():
        results[range_name] = {
            'adds': 0.0,
            'removes': 0.0,
            'remove_by_user': defaultdict(float)
        }

    # Single pass through all events
    for event in events:
        event_timestamp = normalize_timestamp(event.get("timestamp", 0))
        event_type = event.get("type", "")
        value_usd = float(event.get("valueUSD", 0) or 0.0)

        # Skip non-ADD/REMOVE events early
        if event_type not in ["ADD", "REMOVE"]:
            continue

        # Check which ranges this event belongs to
        for range_name, (start_ts, end_ts) in ranges.items():
            if start_ts <= event_timestamp <= end_ts:
                if event_type == "ADD":
                    results[range_name]['adds'] += value_usd
                elif event_type == "REMOVE":
                    results[range_name]['removes'] += value_usd
                    user_address = event.get("userAddress", "")
                    if user_address:
                        results[range_name]['remove_by_user'][user_address] += value_usd

    # Calculate deltas and withdrawal analysis
    for range_name in results:
        results[range_name]['delta'] = (
            results[range_name]['adds'] - results[range_name]['removes']
        )
        results[range_name]['total_removes'] = results[range_name]['removes']

    return results


def calculate_withdrawal_analysis_from_results(remove_by_user, total_removes):
    """Calculate withdrawal analysis from pre-computed user totals.

    Args:
        remove_by_user: Dict mapping user_address -> total_value
        total_removes: Total removal value

    Returns:
        tuple: (count_of_addresses, address_if_count_is_1)
    """
    if not remove_by_user or total_removes == 0:
        return (0, "")

    # Sort users by total value descending
    sorted_users = sorted(remove_by_user.items(),
                          key=lambda x: x[1], reverse=True)

    # Calculate cumulative percentage until reaching 70%
    target_percentage = 0.70
    cumulative_value = 0.0
    address_count = 0

    for user_address, user_total in sorted_users:
        cumulative_value += user_total
        address_count += 1

        if cumulative_value / total_removes >= target_percentage:
            # If only one address is needed, return that address
            most_remover_address = user_address
            return (address_count, most_remover_address)

    # If we've gone through all addresses and still haven't reached 70%,
    # return the count of all addresses
    most_remover_address = sorted_users[0][0]
    return (address_count, most_remover_address)


def main(spreadsheet_name, worksheet_name, nov_2nd_date="2025-11-02", nov_5th_date="2025-11-05"):
    API_URL = "https://api-v3.balancer.fi"

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

    ranges = {
        'nov_2_to_5': (nov_2nd_ts, nov_5th_ts_end),
        'nov_5_to_7d': (nov_5th_ts_start, seven_days_ago_ts) if seven_days_ago_ts >= nov_5th_ts_start else (seven_days_ago_ts, nov_5th_ts_end),
        '7d_to_today': (seven_days_ago_ts, today_ts)
    }

    # Google Sheets
    load_dotenv()
    SERVICE_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")
    logging.info(
        f"Date ranges: Nov 2nd={nov_2nd.date()}, Nov 5th={nov_5th.date()}, 7d ago={seven_days_ago.date()}, Today={today.date()}")

    # Query 1: Get all pools
    QUERY = """
    {
      poolGetPools(
        where: {
          chainIn: [ARBITRUM, AVALANCHE, BASE, GNOSIS, HYPEREVM, MAINNET, OPTIMISM, PLASMA, POLYGON]
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
    logging.info(f"Found {len(pools)} pools")

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

        # Query snapshots first to check TVL on Nov 2nd before processing events
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

        # Get TVL for Nov 2nd to check if we should skip this pool
        tvl_nov_2nd = get_tvl_for_date(snapshots, nov_2nd_ts)

        # Skip pool if TVL on Nov 2nd was less than 300k
        if tvl_nov_2nd < 300000:
            logging.info(
                f"Skipping pool {pool_symbol} ({chain}): TVL on Nov 2nd was {tvl_nov_2nd:.2f} (< 300k)")
            continue

        # Query events with pagination, stopping early when we reach events older than Nov 2nd
        def fetch_events_until_nov_2nd():
            """Fetch events for a pool using pagination, stopping when we reach events older than Nov 2nd.

            Filters events to only include those >= Nov 2nd timestamp.
            Stops pagination early when ALL events in a page are older than Nov 2nd,
            assuming pagination generally returns newer events first.
            """
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
                    break

                result = response.json()
                errors = result.get("errors", [])
                if errors:
                    logging.warning(
                        f"GraphQL errors for pool {pool_id}: {errors}")
                    break

                events_data = result.get("data", {}).get("poolEvents", [])
                if not events_data:
                    break

                # Check if we've reached events older than Nov 2nd
                # Process events and filter by timestamp
                # Note: We assume events are generally returned newest-first, but we check all events
                # to be safe since explicit ordering isn't supported by the API
                events_in_range = []
                all_events_older = True

                for event in events_data:
                    event_timestamp = normalize_timestamp(
                        event.get("timestamp", 0))

                    # Only add events that are >= Nov 2nd
                    if event_timestamp >= nov_2nd_ts:
                        events_in_range.append(event)
                        all_events_older = False

                # Add events that are in range
                all_events.extend(events_in_range)

                # If ALL events in this page are older than Nov 2nd, stop pagination
                # (assuming pagination generally goes from newer to older)
                if all_events_older:
                    logging.debug(
                        f"All events in page are older than Nov 2nd for pool {pool_id}, stopping pagination at skip {skip}")
                    break

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

        # Fetch events until we reach Nov 2nd (early termination optimization)
        all_pool_events = fetch_events_until_nov_2nd()

        # Process events in a single pass for all ranges
        range_results = process_events_for_ranges(all_pool_events, ranges)

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
                f"Processed events - Nov 2-5: delta={range_results['nov_2_to_5']['delta']:.2f}, "
                f"Nov 5-7d: delta={range_results['nov_5_to_7d']['delta']:.2f}, "
                f"7d-Today: delta={range_results['7d_to_today']['delta']:.2f}"
            )

        # Add small buffer between pools
        time.sleep(0.1)

        # Get TVL for the other dates
        tvl_nov_5th = get_tvl_for_date(snapshots, nov_5th_ts_start)
        tvl_7d_ago = get_tvl_for_date(snapshots, seven_days_ago_ts)

        # Calculate withdrawal analysis for each range
        count_nov_2_to_5, address_nov_2_to_5 = calculate_withdrawal_analysis_from_results(
            range_results['nov_2_to_5']['remove_by_user'],
            range_results['nov_2_to_5']['total_removes']
        )

        count_nov_5_to_7d, address_nov_5_to_7d = calculate_withdrawal_analysis_from_results(
            range_results['nov_5_to_7d']['remove_by_user'],
            range_results['nov_5_to_7d']['total_removes']
        )

        count_7d_to_today, address_7d_to_today = calculate_withdrawal_analysis_from_results(
            range_results['7d_to_today']['remove_by_user'],
            range_results['7d_to_today']['total_removes']
        )

        # Debug logging for first few pools
        if idx < 3:
            logging.info(
                f"Pool {pool_symbol}: "
                f"Delta Nov 2-5={range_results['nov_2_to_5']['delta']:.2f}, "
                f"Delta Nov 5-7d={range_results['nov_5_to_7d']['delta']:.2f}, "
                f"Delta 7d-Today={range_results['7d_to_today']['delta']:.2f}"
            )

        rows.append({
            "Pool": pool_symbol,
            "TVL (Nov 2nd)": tvl_nov_2nd,
            "Delta Remove-Add (Nov 2nd - 5th)": range_results['nov_2_to_5']['delta'],
            "Addresses (70% removes Nov 2nd - 5th)": count_nov_2_to_5,
            "Most remover (Nov 2nd - 5th)": address_nov_2_to_5,
            "TVL (Nov 5th)": tvl_nov_5th,
            "Delta Remove-Add (Nov 5th - 7d ago)": range_results['nov_5_to_7d']['delta'],
            "Addresses (70% removes Nov 5th - 7d ago)": count_nov_5_to_7d,
            "Most remover (Nov 5th - 7d ago)": address_nov_5_to_7d,
            "TVL (7d ago)": tvl_7d_ago,
            "Delta Remove-Add (7d ago - Today)": range_results['7d_to_today']['delta'],
            "Addresses (70% removes 7d ago - Today)": count_7d_to_today,
            "Most remover (7d ago - Today)": address_7d_to_today,
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
