import io
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests

# Configuration constants
DEFAULT_TIMEOUT = 10
# URLs by year
F1_URLS = {
    2018: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6664",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6662",
    },
    2019: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6674",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6672",
    },
    2020: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6679",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6677",
    },
    2021: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6284",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6282",
    },
    2022: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6284",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6282",
    },
    2023: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6284",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6282",
    },
    2024: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6276",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6273",
    },
    2025: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6367",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6365",
    },
}
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
}
DELAY_BETWEEN_REQUESTS = 0.5  # seconds

# F1 race names by year
F1_RACES = {
    2025: {
        "AUSTRALIAN": "Australian Grand Prix",
        "CHINESE": "Chinese Grand Prix",
        "JAPANESE": "Japanese Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",
        "MIAMI": "Miami Grand Prix",
        "EMILIA-ROMAGNA": "Emilia Romagna Grand Prix",
        "MONACO": "Monaco Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "CANADA": "Canadian Grand Prix",
        "AUSTRIAN": "Austrian Grand Prix",
        "BRITISH": "British Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "HUNGARIAN": "Hungarian Grand Prix",
        "DUTCH": "Dutch Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "SINGAPORE": "Singapore Grand Prix",
        "UNITED STATES": "United States Grand Prix",
        "MÉXICO": "Mexico City Grand Prix",
        "SÃO PAULO": "São Paulo Grand Prix",
        "LAS VEGAS": "Las Vegas Grand Prix",
        "QATAR": "Qatar Grand Prix",
        "ABU DHABI": "Abu Dhabi Grand Prix",
    },
    2024: {
        "AUSTRALIAN": "Australian Grand Prix",
        "CHINESE": "Chinese Grand Prix",
        "JAPANESE": "Japanese Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",
        "MIAMI": "Miami Grand Prix",
        "EMILIA-ROMAGNA": "Emilia Romagna Grand Prix",
        "MONACO": "Monaco Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "CANADA": "Canadian Grand Prix",
        "AUSTRIAN": "Austrian Grand Prix",
        "BRITISH": "British Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "HUNGARIAN": "Hungarian Grand Prix",
        "DUTCH": "Dutch Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "SINGAPORE": "Singapore Grand Prix",
        "UNITED STATES": "United States Grand Prix",
        "MEXICO": "Mexico City Grand Prix",
        "SÃO PAOLO": "São Paulo Grand Prix",
        "LAS VEGAS": "Las Vegas Grand Prix",
        "QATAR": "Qatar Grand Prix",
        "ABU DHABI": "Abu Dhabi Grand Prix",
    },
    2023: {
        "ABU DHABI": "Abu Dhabi Grand Prix",
        "LAS VEGAS": "Las Vegas Grand Prix",
        "SÃO PAULO": "São Paulo Grand Prix",
        "MÉXICO": "Mexico City Grand Prix",
        "UNITED STATES": "United States Grand Prix",
        "QATAR": "Qatar Grand Prix",
        "JAPANESE": "Japanese Grand Prix",
        "SINGAPORE": "Singapore Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "DUTCH": "Dutch Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "HUNGARIAN": "Hungarian Grand Prix",
        "BRITISH": "British Grand Prix",
        "ÖSTERREICH": "Austrian Grand Prix",
        "CANADA": "Canadian Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "MONACO": "Monaco Grand Prix",
        "MIAMI": "Miami Grand Prix",
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "AUSTRALIAN": "Australian Grand Prix",
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
    },
    2020: {
        "AUSTRALIAN": "Australian Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "CHINESE": "Chinese Grand Prix",
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "MONACO": "Monaco Grand Prix",
        "CANADA": "Canadian Grand Prix",
        "France": "French Grand Prix",
        "ÖSTERREICH": "Austrian Grand Prix",
        "BRITISH": "British Grand Prix",
        "DEUTSCHLAND": "German Grand Prix",
        "NAGYDÍJ": "Hungarian Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "SINGAPORE": "Singapore Grand Prix",
        "RUSSIAN": "Russian Grand Prix",
        "JAPANESE": "Japanese Grand Prix",
        "UNITED STATES": "United States Grand Prix",
        "MÉXICO": "Mexican Grand Prix",
        "BRASIL": "Brazilian Grand Prix",
        "ABU DHABI": "Abu Dhabi Grand Prix",
    },
    2019: {
        "AUSTRALIAN": "Australian Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "CHINESE": "Chinese Grand Prix",
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "MONACO": "Monaco Grand Prix",
        "CANADA": "Canadian Grand Prix",
        "France": "French Grand Prix",
        "ÖSTERREICH": "Austrian Grand Prix",
        "BRITISH": "British Grand Prix",
        "DEUTSCHLAND": "German Grand Prix",
        "NAGYDÍJ": "Hungarian Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "SINGAPORE": "Singapore Grand Prix",
        "RUSSIAN": "Russian Grand Prix",
        "JAPANESE": "Japanese Grand Prix",
        "UNITED STATES": "United States Grand Prix",
        "MÉXICO": "Mexican Grand Prix",
        "BRASIL": "Brazilian Grand Prix",
        "ABU DHABI": "Abu Dhabi Grand Prix",
    },
    2018: {
        "AUSTRALIAN": "Australian Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "CHINESE": "Chinese Grand Prix",
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "MONACO": "Monaco Grand Prix",
        "CANADA": "Canadian Grand Prix",
        "France": "French Grand Prix",
        "ÖSTERREICH": "Austrian Grand Prix",
        "BRITISH": "British Grand Prix",
        "DEUTSCHLAND": "German Grand Prix",
        "NAGYDÍJ": "Hungarian Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "SINGAPORE": "Singapore Grand Prix",
        "RUSSIAN": "Russian Grand Prix",
        "JAPANESE": "Japanese Grand Prix",
        "UNITED STATES": "United States Grand Prix",
        "MÉXICO": "Mexican Grand Prix",
        "BRASIL": "Brazilian Grand Prix",
        "ABU DHABI": "Abu Dhabi Grand Prix",
    },
}


class F1DataFetcher:
    """Class for fetching and processing Formula 1 data."""

    def __init__(
        self, year: int = 2025, timeout: int = DEFAULT_TIMEOUT, headers: Dict = None
    ):
        """
        Initialize the F1DataFetcher.

        Args:
            year: Year for which to fetch F1 data
            timeout: Request timeout in seconds
            headers: HTTP headers for requests
        """
        self.year = year
        self.timeout = timeout
        self.headers = headers or DEFAULT_HEADERS
        self.event_data_cache = {}
        self.event_specific_data_cache = {}

        # Set URLs based on year
        self.set_year(year)

    def set_year(self, year: int) -> None:
        """
        Set the year and update URLs accordingly.

        Args:
            year: Year for which to fetch F1 data
        """
        self.year = year
        if year in F1_URLS:
            self.event_data_url = F1_URLS[year]["EVENT_DATA_URL"]
            self.event_specific_url = F1_URLS[year]["EVENT_SPECIFIC_URL"]
        else:
            # Default to latest year if requested year is not available
            latest_year = max(F1_URLS.keys())
            print(
                f"Warning: Data for year {year} not available. Using {latest_year} instead."
            )
            self.year = latest_year
            self.event_data_url = F1_URLS[latest_year]["EVENT_DATA_URL"]
            self.event_specific_url = F1_URLS[latest_year]["EVENT_SPECIFIC_URL"]

    def fetch_events_data(self, url: str = None) -> Dict:
        """
        Fetch events data from the specified URL.

        Args:
            url: URL to fetch data from (defaults to year-specific URL)

        Returns:
            Dictionary containing events data

        Raises:
            requests.exceptions.RequestException: If request fails
            json.JSONDecodeError: If response is not valid JSON
        """
        if url is None:
            url = self.event_data_url

        print(f"Attempting to fetch data from: {url}")

        try:
            response = self._make_request(url)
            parsed_data = response.json()

            # Extract events list from the nested structure
            data_section = parsed_data.get("data", {})
            chart_section = data_section.get("chart", {})
            events_data = chart_section.get("events", [])

            if events_data:
                print("\nSuccessfully extracted events data")
                return events_data
            else:
                print("\nError: Could not find the 'events' data at the expected path")
                return []

        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"\nError fetching events data: {e}")
            return []

    def fetch_event_specific_data(
        self, events_data: List[Dict], base_url: str = None
    ) -> Dict[str, Any]:
        """
        Fetch specific data for each event.

        Args:
            events_data: List of event dictionaries
            base_url: Base URL for event-specific data (defaults to year-specific URL)

        Returns:
            Dictionary mapping event IDs to their specific data
        """
        if base_url is None:
            base_url = self.event_specific_url

        all_event_specific_data = {}

        print(f"Found {len(events_data)} events to process.")

        for event in events_data:
            event_id = event.get("id")
            event_title = event.get("title", "Unknown Title")

            if not event_id:
                print(f"Warning: Skipping event with missing ID: {event_title}")
                continue

            # Check cache first
            if event_id in self.event_specific_data_cache:
                all_event_specific_data[event_id] = self.event_specific_data_cache[
                    event_id
                ]
                print(f"Using cached data for Event ID: {event_id} ({event_title})")
                continue

            specific_url = f"{base_url}?event={event_id}"
            print(
                f"\nAttempting to fetch data for Event ID: {event_id} ({event_title})"
            )
            print(f"URL: {specific_url}")

            try:
                response = self._make_request(specific_url)
                event_specific_data = response.json()

                # Cache the result
                self.event_specific_data_cache[event_id] = event_specific_data
                all_event_specific_data[event_id] = event_specific_data

                print(f"Successfully fetched and parsed data for Event ID: {event_id}")

            except requests.exceptions.RequestException as e:
                error_info = {"error": str(e)}
                all_event_specific_data[event_id] = error_info
                print(f"Error during request for Event ID: {event_id}: {e}")

            except json.JSONDecodeError:
                error_info = {
                    "error": "JSONDecodeError",
                    "response_text": (
                        response.text[:500]
                        if hasattr(response, "text")
                        else "No response text"
                    ),
                }
                all_event_specific_data[event_id] = error_info
                print(f"Error: Failed to decode JSON for Event ID: {event_id}")

            # Add delay between requests
            if DELAY_BETWEEN_REQUESTS > 0:
                time.sleep(DELAY_BETWEEN_REQUESTS)

        # Print summary
        self._print_fetch_summary(events_data, all_event_specific_data)
        return all_event_specific_data

    def _make_request(self, url: str) -> requests.Response:
        """
        Make an HTTP request with error handling.

        Args:
            url: URL to request

        Returns:
            Response object

        Raises:
            requests.exceptions.RequestException: If request fails
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            print(f"Error: The request to {url} timed out.")
            raise
        except requests.exceptions.RequestException:
            print(f"Error during request to {url}")
            raise

    def _print_fetch_summary(
        self, events_data: List[Dict], all_event_specific_data: Dict[str, Any]
    ) -> None:
        """Print a summary of the fetch operation."""
        print("\n--- Processing Complete ---")
        successful_fetches = sum(
            1
            for data in all_event_specific_data.values()
            if isinstance(data, dict) and "error" not in data
        )
        failed_fetches = len(events_data) - successful_fetches
        print(f"Successfully fetched data for {successful_fetches} events.")
        print(f"Failed to fetch data for {failed_fetches} events.")


class DataProcessor:
    """Class for processing F1 data."""

    @staticmethod
    def html_table_to_dataframe(event_json_data: Dict) -> Optional[pd.DataFrame]:
        """
        Extract HTML table from event JSON data and convert to DataFrame.

        Args:
            event_json_data: JSON data dictionary for a specific event

        Returns:
            DataFrame created from HTML table, or None if extraction fails
        """
        if not isinstance(event_json_data, dict):
            print("Error: Input must be a dictionary.")
            return None

        # Extract HTML table string
        html_table_str = event_json_data.get("htmlList", {}).get("table")

        if not html_table_str:
            print(
                "Error: Could not find 'htmlList' -> 'table' in the provided JSON data or it's empty."
            )
            return None

        if not isinstance(html_table_str, str):
            print("Error: The value at ['htmlList']['table'] is not a string.")
            return None

        print("Found HTML table string. Attempting to parse...")
        try:
            # Parse HTML table into DataFrame
            list_of_dfs = pd.read_html(io.StringIO(html_table_str))

            if list_of_dfs:
                print("Successfully parsed HTML table into DataFrame.")
                return list_of_dfs[0]
            else:
                print(
                    "Warning: No tables found by pd.read_html, although HTML string was present."
                )
                return None

        except ValueError as ve:
            print(f"Error parsing HTML with pandas (ValueError): {ve}")
            print("Check if the HTML string actually contains a <table> tag.")
            return None
        except ImportError:
            print(
                "Error: The 'lxml' library might be required by pd.read_html. Please install it (`pip install lxml`)."
            )
            return None
        except Exception as e:
            print(f"An unexpected error occurred during HTML parsing: {e}")
            return None

    @staticmethod
    def save_dataframe_to_json(
        df: pd.DataFrame, event_title: str, year: int, output_dir: str = None
    ) -> str:
        """
        Save DataFrame to a JSON file with a standardized filename.

        Args:
            df: DataFrame to save
            event_title: Title of the event (e.g., "FORMULA 1 LOUIS VUITTON AUSTRALIAN GRAND PRIX 2025")
            year: Year of the event
            output_dir: Directory to save the JSON file (defaults to year folder)

        Returns:
            Path to the saved JSON file
        """
        if df is None:
            print(f"Error: Cannot save None DataFrame for {event_title}")
            return ""

        # If no output directory is specified, use the year as the directory name
        if output_dir is None:
            output_dir = str(year)

        # Find the proper race name from the event title
        race_name = None

        # First, try to match with our predefined race names for the specific year
        if year in F1_RACES:
            year_races = F1_RACES[year]
            for key, proper_name in year_races.items():
                if key in event_title.upper():
                    race_name = proper_name
                    break
        else:
            # If year not in F1_RACES, use the latest year's race names
            latest_year = max(F1_RACES.keys())
            year_races = F1_RACES[latest_year]
            for key, proper_name in year_races.items():
                if key in event_title.upper():
                    race_name = proper_name
                    break

        # If no match found, try to extract from the title
        if race_name is None:
            # Try to extract Grand Prix name using regex
            match = re.search(
                r"([A-Z]+(?:\s+[A-Z]+)*)\s+GRAND\s+PRIX", event_title, re.IGNORECASE
            )
            if match:
                location = match.group(1).strip()
                race_name = f"{location} Grand Prix"
            else:
                # Fallback to a generic name with the event ID
                race_name = "Unknown Grand Prix"
                print(f"Warning: Could not determine race name for: {event_title}")

        # Clean up the filename
        filename = race_name

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save the DataFrame to a JSON file
        file_path = os.path.join(output_dir, f"{filename}.json")
        df.to_json(file_path, orient="records", indent=4)

        print(f"Saved data to {file_path}")
        return file_path


def main(year: int = 2025):
    """
    Main function to fetch and process F1 data for a specific year.

    Args:
        year: Year to fetch data for (default: 2025)
    """
    print(f"Fetching F1 data for year: {year}")

    # Initialize the data fetcher with the specified year
    fetcher = F1DataFetcher(year=year)

    # Fetch events data
    events_data = fetcher.fetch_events_data()
    if not events_data:
        print("No events data found. Exiting.")
        return

    # Fetch specific data for each event
    all_event_specific_data = fetcher.fetch_event_specific_data(events_data)

    # Create a DataProcessor instance
    processor = DataProcessor()

    # Process each event and save to JSON
    saved_files = []
    output_dir = str(year)  # Use year as directory name

    for event in events_data:
        event_id = event.get("id")
        event_title = event.get("title", "Unknown Title")

        if not event_id or event_id not in all_event_specific_data:
            print(f"Skipping event {event_title}: No data available")
            continue

        event_data = all_event_specific_data[event_id]

        # Check if there was an error fetching this event's data
        if isinstance(event_data, dict) and "error" in event_data:
            print(
                f"Skipping event {event_title}: Error in data - {event_data.get('error')}"
            )
            continue

        # Convert HTML table to DataFrame
        print(f"\nProcessing event: {event_title}")
        event_dataframe = processor.html_table_to_dataframe(event_data)

        if event_dataframe is not None:
            # Save DataFrame to JSON
            file_path = processor.save_dataframe_to_json(
                event_dataframe, event_title, year, output_dir
            )

            if file_path:
                saved_files.append(file_path)
        else:
            print(f"Failed to create DataFrame for {event_title}")

    # Print summary
    print(f"\n--- JSON Export Complete ---")
    print(
        f"Successfully saved {len(saved_files)} event data files to the '{output_dir}' directory."
    )
    if saved_files:
        print("Files saved:")
        for file_path in saved_files:
            print(f"  - {file_path}")


if __name__ == "__main__":
    main(2019)
