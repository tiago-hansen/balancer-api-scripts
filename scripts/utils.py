import time
from collections import deque
import logging
import requests


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


# Default rate limiter instance for make_api_request
_rate_limiter = RateLimiter(max_requests=30, time_window=10)


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
