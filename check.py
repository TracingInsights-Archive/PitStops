import json
import logging
import time
from datetime import datetime

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Output to console
        logging.FileHandler("monitor.log"),  # Output to file
    ],
)
logger = logging.getLogger(__name__)

# Configuration
URL = "https://inmotion.dhl/api/f1-award-element-data/6365?event=1094"
CHECK_INTERVAL_SECONDS = 60  # Check every 60 seconds
MAX_RETRIES_ON_ERROR = 3  # Max retries if a network error occurs before waiting longer
RETRY_DELAY_SECONDS = 10  # Delay between retries for transient errors

# It's good practice to set a User-Agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 F1UpdateChecker/1.0"
}


def fetch_data(url, headers, timeout=10):
    """Fetches data from the URL and returns the JSON response."""
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
    return response.json()


def main():
    logger.info(f"Monitoring URL: {URL}")
    logger.info(
        f"Checking every {CHECK_INTERVAL_SECONDS} seconds for updates in 'data.chart'."
    )
    logger.info("-" * 30)

    retries = 0
    while True:
        try:
            logger.info("Checking for updates...")
            api_data = fetch_data(URL, HEADERS)
            retries = 0  # Reset retries on successful fetch

            # The key distinguishing factor is the 'chart' list within 'data'
            # Safely access nested keys
            chart_data = api_data.get("data", {}).get("chart", None)

            if chart_data is None:
                logger.warning(
                    "Response format unexpected: 'data' or 'chart' key missing."
                )
                logger.debug(f"Raw response snippet: {str(api_data)[:200]}")
            elif isinstance(chart_data, list) and len(chart_data) > 0:
                logger.info(">>> UPDATE DETECTED! <<<")
                logger.info(f"Chart Data:\n{json.dumps(chart_data, indent=2)}")
                logger.info("Script will now exit as data has been found.")
                break  # Exit the loop once an update is found
            else:
                # chart_data is an empty list or not a list
                logger.info(
                    "No update yet. 'chart' data is empty or not in the expected populated format."
                )
                if isinstance(chart_data, list):
                    logger.debug(f"Current 'chart' length: {len(chart_data)}")
                else:
                    logger.debug(
                        f"Current 'chart' type: {type(chart_data)}, value: {str(chart_data)[:50]}"
                    )

        except requests.exceptions.HTTPError as http_err:
            logger.error(
                f"HTTP error occurred: {http_err} - Status Code: {http_err.response.status_code}"
            )
            if retries < MAX_RETRIES_ON_ERROR:
                retries += 1
                logger.info(
                    f"Retrying in {RETRY_DELAY_SECONDS} seconds... (Attempt {retries}/{MAX_RETRIES_ON_ERROR})"
                )
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            else:
                logger.warning("Max retries reached. Will wait for the full interval.")

        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Connection error occurred: {conn_err}")
            if retries < MAX_RETRIES_ON_ERROR:
                retries += 1
                logger.info(
                    f"Retrying in {RETRY_DELAY_SECONDS} seconds... (Attempt {retries}/{MAX_RETRIES_ON_ERROR})"
                )
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            else:
                logger.warning("Max retries reached. Will wait for the full interval.")

        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Timeout error occurred: {timeout_err}")
            if retries < MAX_RETRIES_ON_ERROR:
                retries += 1
                logger.info(
                    f"Retrying in {RETRY_DELAY_SECONDS} seconds... (Attempt {retries}/{MAX_RETRIES_ON_ERROR})"
                )
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            else:
                logger.warning("Max retries reached. Will wait for the full interval.")

        except requests.exceptions.RequestException as req_err:
            logger.error(f"An error occurred during the request: {req_err}")
            # For generic request exceptions, you might not want to retry as aggressively

        except json.JSONDecodeError as json_err:
            logger.error(f"Failed to decode JSON response: {json_err}")
            # It's unlikely retrying immediately will help if the server sends malformed JSON

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)

        logger.info(
            f"Waiting for {CHECK_INTERVAL_SECONDS} seconds before next check..."
        )
        logger.info("-" * 30)
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user.")
