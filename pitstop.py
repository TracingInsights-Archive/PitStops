import io
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests

# Configuration constants
#  https://aistudio.google.com/prompts/1p-i2TSn-3uPdbqqMzZ9sFfZfih_iUw4e - for F!_RACES conversion
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
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6684",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6682",
    },
    2022: {
        "EVENT_DATA_URL": "https://inmotion.dhl/api/f1-award-element-data/6689",
        "EVENT_SPECIFIC_URL": "https://inmotion.dhl/api/f1-award-element-data/6687",
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
DELAY_BETWEEN_REQUESTS = 0  # seconds

# F1 race names by year
F1_RACES = {
    2025: {
        "AUSTRALIAN": "Australian Grand Prix",  # From "FORMULA 1 LOUIS VUITTON AUSTRALIAN GRAND PRIX 2025"
        "CHINESE": "Chinese Grand Prix",  # From "FORMULA 1 HEINEKEN CHINESE GRAND PRIX 2025"
        "JAPANESE": "Japanese Grand Prix",  # From "FORMULA 1 LENOVO JAPANESE GRAND PRIX 2025"
        "BAHRAIN": "Bahrain Grand Prix",  # From "FORMULA 1 GULF AIR BAHRAIN GRAND PRIX 2025"
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",  # From "FORMULA 1 STC SAUDI ARABIAN GRAND PRIX 2025"
        "MIAMI": "Miami Grand Prix",  # From "FORMULA 1 CRYPTO.COM MIAMI GRAND PRIX 2025"
        "EMILIA-ROMAGNA": "Emilia Romagna Grand Prix",  # From "FORMULA 1 AWS GRAN PREMIO DEL MADE IN ITALY E DELL'EMILIA-ROMAGNA 2025"
        "MONACO": "Monaco Grand Prix",  # From "FORMULA 1 TAG HEUER GRAND PRIX DE MONACO 2025"
        "ESPAÑA": "Spanish Grand Prix",  # From "FORMULA 1 ARAMCO GRAN PREMIO DE ESPAÑA 2025"
        "CANADA": "Canadian Grand Prix",  # From "FORMULA 1 PIRELLI GRAND PRIX DU CANADA 2025"
        "AUSTRIAN": "Austrian Grand Prix",  # From "FORMULA 1 MSC CRUISES AUSTRIAN GRAND PRIX 2025"
        "BRITISH": "British Grand Prix",  # From "FORMULA 1 QATAR AIRWAYS BRITISH GRAND PRIX 2025"
        "BELGIAN": "Belgian Grand Prix",  # From "FORMULA 1 MOËT & CHANDON BELGIAN GRAND PRIX 2025"
        "HUNGARIAN": "Hungarian Grand Prix",  # From "FORMULA 1 LENOVO HUNGARIAN GRAND PRIX 2025"
        "DUTCH": "Dutch Grand Prix",  # From "FORMULA 1 HEINEKEN DUTCH GRAND PRIX 2025"
        "ITALIA": "Italian Grand Prix",  # From "FORMULA 1 PIRELLI GRAN PREMIO D’ITALIA 2025"
        "AZERBAIJAN": "Azerbaijan Grand Prix",  # From "FORMULA 1 QATAR AIRWAYS AZERBAIJAN GRAND PRIX 2025"
        "SINGAPORE": "Singapore Grand Prix",  # From "FORMULA 1 SINGAPORE AIRLINES SINGAPORE GRAND PRIX 2025"
        "UNITED STATES": "United States Grand Prix",  # From "FORMULA 1 MSC CRUISES UNITED STATES GRAND PRIX 2025"
        "MÉXICO": "Mexico City Grand Prix",  # From "FORMULA 1 GRAN PREMIO DE LA CIUDAD DE MÉXICO 2025"
        "SÃO PAULO": "São Paulo Grand Prix",  # From "FORMULA 1 MSC CRUISES GRANDE PRÊMIO DE SÃO PAULO 2025"
        "LAS VEGAS": "Las Vegas Grand Prix",  # From "FORMULA 1 HEINEKEN LAS VEGAS GRAND PRIX 2025"
        "QATAR": "Qatar Grand Prix",  # From "FORMULA 1 QATAR AIRWAYS QATAR GRAND PRIX 2025"
        "ABU DHABI": "Abu Dhabi Grand Prix",  # From "FORMULA 1 ETIHAD AIRWAYS ABU DHABI GRAND PRIX 2025"
    },
    2024: {
        "BAHRAIN": "Bahrain Grand Prix",  # From "Formula 1 Gulf Air Bahrain Grand Prix 2024"
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",  # From "Formula 1 stc Saudi Arabian Grand Prix 2024"
        "AUSTRALIAN": "Australian Grand Prix",  # From "Formula 1 Rolex Australian Grand Prix 2024"
        "JAPANESE": "Japanese Grand Prix",  # From "Formula 1 MSC Cruises Japanese Grand Prix 2024"
        "CHINESE": "Chinese Grand Prix",  # From "Formula 1 Lenovo Chinese Grand Prix 2024"
        "MIAMI": "Miami Grand Prix",  # From "Formula 1 Crypto.com Miami Grand Prix 2024"
        "EMILIA-ROMAGNA": "Emilia Romagna Grand Prix",  # From "Formula 1 MSC Cruises Gran Premio dell'Emilia-Romagna 2024"
        "MONACO": "Monaco Grand Prix",  # From "Formula 1 Grand Prix de Monaco 2024"
        "CANADA": "Canadian Grand Prix",  # From "Formula 1 AWS Grand Prix du Canada 2024"
        "ESPAÑA": "Spanish Grand Prix",  # From "Formula 1 Aramco Grand Premio de España" (Note: API title ends abruptly, using "ESPAÑA")
        "AUSTRIAN": "Austrian Grand Prix",  # From "Formula 1 Qatar Airways Austrian Grand Prix 2024"
        "BRITISH": "British Grand Prix",  # From "Formula 1 Qatar Airways British Grand Prix 2024"
        "HUNGARIAN": "Hungarian Grand Prix",  # From "Formula 1 Hungarian Grand Prix 2024"
        "BELGIAN": "Belgian Grand Prix",  # From "Formula 1 Rolex Belgian Grand Prix 2024"
        "DUTCH": "Dutch Grand Prix",  # From "Formula 1 Heineken Dutch Grand Prix 2024"
        "ITALIA": "Italian Grand Prix",  # From "Formula 1 Pirelli Gran Premio d'Italia 2024"
        "AZERBAIJAN": "Azerbaijan Grand Prix",  # From "Formula 1 Qatar Airways Azerbaijan Grand Prix 2024"
        "SINGAPORE": "Singapore Grand Prix",  # From "Formula 1 Singapore Airlines Singapore Grand Prix 2024"
        "UNITED STATES": "United States Grand Prix",  # From "Formula 1 Pirelli United States Grand Prix 2024"
        "MEXICO": "Mexico City Grand Prix",  # From "Formula 1 Grand Premio de la Ciudad de Mexico 2024" (Using "MEXICO" from "Ciudad de Mexico")
        "SÃO PAOLO": "São Paulo Grand Prix",  # From "Formula 1 Lenovo Grande Prêmio de São Paolo 2024" (Using "SÃO PAOLO" as per API)
        "LAS VEGAS": "Las Vegas Grand Prix",  # From "Formula 1 Heineken Silver Las Vegas Grand Prix 2024"
        "QATAR": "Qatar Grand Prix",  # From "Formula 1 Qatar Airways Qatar Grand Prix 2024"
        "ABU DHABI": "Abu Dhabi Grand Prix",  # From "Formula 1 Etihad Airways Abu Dhabi Grand Prix 2024"
    },
    2023: {
        "BAHRAIN": "Bahrain Grand Prix",  # From "Formula 1 Gulf Air Bahrain Grand Prix 2023"
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",  # From "Formula 1 stc Saudi Arabian Grand Prix 2023"
        "AUSTRALIAN": "Australian Grand Prix",  # From "Formula 1 Rolex Australian Grand Prix 2023"
        "AZERBAIJAN": "Azerbaijan Grand Prix",  # From "Formula 1 Azerbaijan Grand Prix 2023"
        "MIAMI": "Miami Grand Prix",  # From "Formula 1 Crypto.com Miami Grand Prix 2023"
        "MONACO": "Monaco Grand Prix",  # From "Formula 1 Grand Prix de Monaco 2023"
        "ESPAÑA": "Spanish Grand Prix",  # From "Formula 1 Pirelli Gran Premio De España 2023"
        "CANADA": "Canadian Grand Prix",  # From "Formula 1 Grand Prix Du Canada 2023"
        "ÖSTERREICH": "Austrian Grand Prix",  # From "Formula 1 Grosser Preis von Österreich 2023"
        "BRITISH": "British Grand Prix",  # From "Formula 1 Aramco British Grand Prix 2023"
        "HUNGARIAN": "Hungarian Grand Prix",  # From "Formula 1 Hungarian Grand Prix 2023" (API title is direct enough)
        "BELGIAN": "Belgian Grand Prix",  # From "Formula 1 Belgian Grand Prix 2023" (API title is direct enough)
        "DUTCH": "Dutch Grand Prix",  # From "Formula 1 Heineken Dutch Grand Prix 2023"
        "ITALIA": "Italian Grand Prix",  # From "Formula 1 Gran Premio D’italia 2023"
        "SINGAPORE": "Singapore Grand Prix",  # From "Formula 1 Singapore Airlines Singapore Grand Prix 2023"
        "JAPANESE": "Japanese Grand Prix",  # From "Formula 1 Lenovo Japanese Grand Prix 2023"
        "QATAR": "Qatar Grand Prix",  # From "Formula 1 Qatar Grand Prix 2023" (API title is direct enough)
        "UNITED STATES": "United States Grand Prix",  # From "Formula 1 Lenovo United States Grand Prix 2023"
        "MÉXICO": "Mexico City Grand Prix",  # From "Formula 1 Gran Premio De La Ciudad De México 2023"
        "SÃO PAULO": "São Paulo Grand Prix",  # From "Formula 1 Rolex Grande Prêmio De São Paulo 2023"
        "LAS VEGAS": "Las Vegas Grand Prix",  # From "Formula 1 Heineken Silver Las Vegas Grand Prix 2023"
        "ABU DHABI": "Abu Dhabi Grand Prix",  # From "Formula 1 Etihad Airways Abu Dhabi Grand Prix 2023"
    },
    2022: {
        "BAHRAIN": "Bahrain Grand Prix",  # From "Formula 1 Gulf Air Bahrain Grand Prix 2022"
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",  # From "Formula 1 stc Saudi Arabian Grand Prix 2022"
        "AUSTRALIAN": "Australian Grand Prix",  # From "Formula 1 Heineken Australian Grand Prix 2022"
        "EMILIA ROMAGNA": "Emilia Romagna Grand Prix",  # From "Formula 1 Rolex Gran Premio del Made in Italy E dell’Emilia Romagna 2022"
        "MIAMI": "Miami Grand Prix",  # From "Formula 1 Crypto.com Miami Grand Prix 2022"
        "ESPAÑA": "Spanish Grand Prix",  # From "Formula 1 Pirelli Gran Premio De España 2022"
        "MONACO": "Monaco Grand Prix",  # From "Formula 1 Grand Prix De Monaco 2022"
        "AZERBAIJAN": "Azerbaijan Grand Prix",  # From "Formula 1 Azerbaijan Grand Prix 2022"
        "CANADA": "Canadian Grand Prix",  # From "Formula 1 AWS Grand Prix Du Canada 2022"
        "BRITISH": "British Grand Prix",  # From "Formula 1 Lenovo British Grand Prix 2022"
        "ÖSTERREICH": "Austrian Grand Prix",  # From "Formula 1 Rolex Grosser Preis von Österreich 2022"
        "FRANCE": "French Grand Prix",  # From "Formula 1 Lenovo Grand Prix De France 2022"
        "NAGYDÍJ": "Hungarian Grand Prix",  # From "Formula 1 Aramco Magyar Nagydíj 2022"
        "BELGIAN": "Belgian Grand Prix",  # From "Formula 1 Rolex Belgian Grand Prix 2022"
        "DUTCH": "Dutch Grand Prix",  # From "Formula 1 Heineken Dutch Grand Prix 2022"
        "ITALIA": "Italian Grand Prix",  # From "Formula 1 Pirelli Gran Premio D’italia 2022"
        "SINGAPORE": "Singapore Grand Prix",  # From "Formula 1 Singapore Airlines Singapore Grand Prix 2022"
        "JAPANESE": "Japanese Grand Prix",  # From "Formula 1 Honda Japanese Grand Prix 2022"
        "UNITED STATES": "United States Grand Prix",  # From "Formula 1 Aramco United States Grand Prix 2022"
        "MÉXICO": "Mexico City Grand Prix",  # From "Formula 1 Gran Premio De La Ciudad De México 2022"
        "SÃO PAULO": "São Paulo Grand Prix",  # From "Formula 1 Heineken Grande Prêemio De São Paulo 2022"
        "ABU DHABI": "Abu Dhabi Grand Prix",  # From "Formula 1 Etihad Airways Abu Dhabi Grand Prix 2022"
    },
    2021: {
        "ABU DHABI": "Abu Dhabi Grand Prix",
        "ÖSTERREICH": "Austrian Grand Prix",  # From "Preis Von Österreich"
        "AZERBAIJAN": "Azerbaijan Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "BRITISH": "British Grand Prix",
        "DUTCH": "Dutch Grand Prix",
        "EMILIA ROMAGNA": "Emilia Romagna Grand Prix",  # From "dell’Emilia Romagna"
        "FRANCE": "French Grand Prix",
        "NAGYDÍJ": "Hungarian Grand Prix",  # From "Magyar Nagydíj"
        "ITALIA": "Italian Grand Prix",  # From "D’italia"
        "MÉXICO": "Mexico City Grand Prix",  # From "Ciudad De México"
        "MONACO": "Monaco Grand Prix",
        "PORTUGUESE": "Portuguese Grand Prix",
        "QATAR": "Qatar Grand Prix",
        "RUSSIAN": "Russian Grand Prix",
        "SAUDI ARABIAN": "Saudi Arabian Grand Prix",
        "ESPAÑA": "Spanish Grand Prix",
        "STEIERMARK": "Styrian Grand Prix",  # From "Der Steiermark"
        "SÃO PAULO": "São Paulo Grand Prix",
        "TURKISH": "Turkish Grand Prix",
        "UNITED STATES": "United States Grand Prix",
    },
    2020: {
        "ÖSTERREICH": "Austrian Grand Prix",
        "STEIERMARK": "Styrian Grand Prix",
        "NAGYDÍJ": "Hungarian Grand Prix",
        "BRITISH": "British Grand Prix",
        "70TH ANNIVERSARY": "70th Anniversary Grand Prix",  # Key "ANNIVERSARY" in API title often
        "ESPAÑA": "Spanish Grand Prix",
        "BELGIAN": "Belgian Grand Prix",
        "ITALIA": "Italian Grand Prix",
        "TOSCANA": "Tuscan Grand Prix",
        "RUSSIAN": "Russian Grand Prix",
        "EIFEL": "Eifel Grand Prix",
        "PORTUGUESE": "Portuguese Grand Prix",
        "EMILIA ROMAGNA": "Emilia Romagna Grand Prix",  # Often "ROMAGNA" is enough from API title part "DELL'EMILIA ROMAGNA"
        "TURKISH": "Turkish Grand Prix",
        "BAHRAIN": "Bahrain Grand Prix",
        "SAKHIR": "Sakhir Grand Prix",
        "ABU DHABI": "Abu Dhabi Grand Prix",
    },
    2019: {
        "AUSTRALIAN": "Australian Grand Prix",  # From "Formula 1 Rolex Australian Grand Prix"
        "BAHRAIN": "Bahrain Grand Prix",  # From "Formula 1 Gulf Air Bahrain Grand Prix"
        "CHINESE": "Chinese Grand Prix",  # From "Formula 1 Heineken Chinese Grand Prix"
        "AZERBAIJAN": "Azerbaijan Grand Prix",  # From "Formula 1 Socar Azerbaijan Grand Prix"
        "ESPAÑA": "Spanish Grand Prix",  # From "Formula 1 Emirates Gran Premio de España"
        "MONACO": "Monaco Grand Prix",  # From "Formula 1 Grand Prix de Monaco"
        "CANADA": "Canadian Grand Prix",  # From "Formula 1 Pirelli Grand Prix du Canada"
        "FRANCE": "French Grand Prix",  # From "Formula 1 Grand Prix de France"
        "ÖSTERREICH": "Austrian Grand Prix",  # From "Formula 1 myWorld Grosser Preis von Österreich"
        "BRITISH": "British Grand Prix",  # From "Formula 1 Rolex British Grand Prix"
        "DEUTSCHLAND": "German Grand Prix",  # From "Formula 1 Mercedes-Benz Grosser Preis von Deutschland"
        "NAGYDÍJ": "Hungarian Grand Prix",  # From "Formula 1 Magyar Nagydíj"
        "BELGIAN": "Belgian Grand Prix",  # From "Formula 1 Johnnie Walker Belgian Grand Prix"
        "ITALIA": "Italian Grand Prix",  # From "Formula 1 Gran Premio Heineken D'Italia"
        "SINGAPORE": "Singapore Grand Prix",  # From "Formula 1 Singapore Airlines Singapore Grand Prix"
        "RUSSIAN": "Russian Grand Prix",  # From "Formula 1 VTB Russian Grand Prix"
        "JAPANESE": "Japanese Grand Prix",  # From "Formula 1 Japanese Grand Prix"
        "MÉXICO": "Mexican Grand Prix",  # From "Formula 1 Gran Premio de México 2019"
        "UNITED STATES": "United States Grand Prix",  # From "Formula 1 United States Grand Prix"
        "BRASIL": "Brazilian Grand Prix",  # From "Formula 1 Heineken Grande Prêmio do Brasil"
        "ABU DHABI": "Abu Dhabi Grand Prix",  # From "Formula 1 Etihad Airways Abu Dhabi Grand Prix"
    },
    2018: {
        "AUSTRALIAN": "Australian Grand Prix",  # From "FORMULA 1 2018 ROLEX AUSTRALIAN GRAND PRIX"
        "BAHRAIN": "Bahrain Grand Prix",  # From "FORMULA 1 2018 GULF AIR BAHRAIN GRAND PRIX"
        "CHINESE": "Chinese Grand Prix",  # From "FORMULA 1 2018 HEINEKEN CHINESE GRAND PRIX"
        "AZERBAIJAN": "Azerbaijan Grand Prix",  # From "FORMULA 1 2018 AZERBAIJAN GRAND PRIX"
        "ESPAÑA": "Spanish Grand Prix",  # From "FORMULA 1 GRAN PREMIO DE ESPAÑA EMIRATES 2018"
        "MONACO": "Monaco Grand Prix",  # From "FORMULA 1 GRAND PRIX DE MONACO 2018"
        "CANADA": "Canadian Grand Prix",  # From "FORMULA 1 GRAND PRIX HEINEKEN DU CANADA 2018"
        "FRANCE": "French Grand Prix",  # From "FORMULA 1 PIRELLI GRAND PRIX DE FRANCE 2018"
        "ÖSTERREICH": "Austrian Grand Prix",  # From "FORMULA 1 EYETIME GROSSER PREIS VON ÖSTERREICH 2018"
        "BRITISH": "British Grand Prix",  # From "FORMULA 1 2018 ROLEX BRITISH GRAND PRIX"
        "DEUTSCHLAND": "German Grand Prix",  # From "FORMULA 1 EMIRATES GROSSER PREIS VON DEUTSCHLAND 2018"
        "NAGYDÍJ": "Hungarian Grand Prix",  # From "FORMULA 1 ROLEX MAGYAR NAGYDÍJ 2018"
        "BELGIAN": "Belgian Grand Prix",  # From "FORMULA 1 2018 JOHNNIE WALKER BELGIAN GRAND PRIX"
        "ITALIA": "Italian Grand Prix",  # From "FORMULA 1 GRAN PREMIO HEINEKEN D'ITALIA 2018"
        "SINGAPORE": "Singapore Grand Prix",  # From "FORMULA 1 2018 SINGAPORE AIRLINES SINGAPORE GRAND PRIX"
        "RUSSIAN": "Russian Grand Prix",  # From "FORMULA 1 2018 VTB RUSSIAN GRAND PRIX"
        "JAPANESE": "Japanese Grand Prix",  # From "FORMULA 1 2018 HONDA JAPANESE GRAND PRIX"
        "UNITED STATES": "United States Grand Prix",  # From "FORMULA 1 PIRELLI 2018 UNITED STATES GRAND PRIX"
        "MÉXICO": "Mexican Grand Prix",  # From "FORMULA 1 GRAN PREMIO DE MÉXICO 2018"
        "BRASIL": "Brazilian Grand Prix",  # From "FORMULA 1 GRANDE PRÊMIO HEINEKEN DO BRASIL 2018"
        "ABU DHABI": "Abu Dhabi Grand Prix",  # From "FORMULA 1 2018 ETIHAD AIRWAYS ABU DHABI GRAND PRIX"
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
    main(2025)
