import requests
import json

# Define the URL to fetch data from
url = "https://inmotion.dhl/api/f1-award-element-data/6367"

print(f"Attempting to fetch data from: {url}")

try:
    # Make the GET request
    response = requests.get(url, timeout=10)  # Added a timeout

    # Check for HTTP errors (like 404 Not Found, 500 Internal Server Error)
    response.raise_for_status()
    print(f"Successfully fetched data (Status Code: {response.status_code})")

    # Parse the JSON response
    try:
        parsed_data = response.json()

        # Navigate through the dictionary to extract the 'events' list
        # Path: root -> 'data' -> 'chart' -> 'events'
        data_section = parsed_data.get("data")
        chart_section = data_section.get("chart") if data_section else None
        events_data = chart_section.get("events") if chart_section else None

        # Check if the data was found and print it
        if events_data is not None:
            print("\nSuccessfully extracted events data:")
            # Pretty print the extracted list
            print(json.dumps(events_data, indent=4))
        else:
            print(
                "\nError: Could not find the 'events' data at the expected path ('data' -> 'chart' -> 'events') in the JSON response."
            )
            # Optional: Print the structure if keys are missing
            # print("\nReceived JSON structure:")
            # print(json.dumps(parsed_data, indent=4))

    except json.JSONDecodeError:
        print("\nError: Failed to decode JSON from the response.")
        print("Response text was:")
        print(
            response.text[:500] + "..." if len(response.text) > 500 else response.text
        )  # Print beginning of text

except requests.exceptions.Timeout:
    print(f"\nError: The request to {url} timed out.")
except requests.exceptions.RequestException as e:
    print(f"\nError during request to {url}: {e}")
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}")


import requests
import json
import time  # Import time for potential delays

# --- 1. Sample events_data (Replace with your actual data) ---
# Since the tool environment couldn't fetch the initial list,
# we'll use the sample data from your first prompt here.


# --- 2. Configuration ---
base_event_url = "https://inmotion.dhl/api/f1-award-element-data/6365"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
}
timeout_seconds = 15
# Optional: Add a small delay between requests to avoid overwhelming the server
delay_between_requests = 0.5  # seconds

# --- 3. Data Fetching Loop ---
all_event_specific_data = {}  # Dictionary to store results {event_id: data}

print(f"Found {len(events_data)} events to process.")

for event in events_data:
    event_id = event.get("id")
    event_title = event.get("title", "Unknown Title")  # Get title for logging

    if not event_id:
        print(f"Warning: Skipping event with missing ID: {event_title}")
        continue

    # Construct the specific URL for this event
    specific_url = f"{base_event_url}?event={event_id}"
    print(f"\nAttempting to fetch data for Event ID: {event_id} ({event_title})")
    print(f"URL: {specific_url}")

    try:
        # Make the GET request for the specific event
        response = requests.get(specific_url, headers=headers, timeout=timeout_seconds)
        response.raise_for_status()  # Check for HTTP errors

        # Parse the JSON response
        try:
            event_specific_data = response.json()
            all_event_specific_data[event_id] = event_specific_data  # Store the data
            print(f"Successfully fetched and parsed data for Event ID: {event_id}")
            # Optional: Print a snippet of the fetched data
            # print(json.dumps(event_specific_data, indent=2)[:200] + "...")

        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON for Event ID: {event_id}")
            print(f"Response text (first 500 chars): {response.text[:500]}")
            all_event_specific_data[event_id] = {
                "error": "JSONDecodeError",
                "response_text": response.text[:500],
            }

    except requests.exceptions.Timeout:
        print(f"Error: Request timed out for Event ID: {event_id} at {specific_url}")
        all_event_specific_data[event_id] = {"error": "Timeout"}
    except requests.exceptions.RequestException as e:
        print(f"Error during request for Event ID: {event_id} at {specific_url}: {e}")
        all_event_specific_data[event_id] = {"error": str(e)}
    except Exception as e:
        print(f"An unexpected error occurred for Event ID: {event_id}: {e}")
        all_event_specific_data[event_id] = {"error": f"Unexpected: {str(e)}"}

    # Optional delay
    if delay_between_requests > 0:
        time.sleep(delay_between_requests)

# --- 4. Final Output ---
print("\n--- Processing Complete ---")
successful_fetches = sum(
    1
    for data in all_event_specific_data.values()
    if isinstance(data, dict) and "error" not in data
)
failed_fetches = len(events_data) - successful_fetches
print(f"Successfully fetched data for {successful_fetches} events.")
print(f"Failed to fetch data for {failed_fetches} events.")

# Optional: Print all collected data (can be very large)
# print("\n--- Collected Data ---")
# print(json.dumps(all_event_specific_data, indent=4))

# Example: Print data for a specific event ID if it exists and wasn't an error
target_id_to_show = 1086
if (
    target_id_to_show in all_event_specific_data
    and isinstance(all_event_specific_data[target_id_to_show], dict)
    and "error" not in all_event_specific_data[target_id_to_show]
):
    print(f"\n--- Sample Data for Event ID {target_id_to_show} ---")
    print(json.dumps(all_event_specific_data[target_id_to_show], indent=4))
elif target_id_to_show in all_event_specific_data:
    print(f"\n--- Error Data for Event ID {target_id_to_show} ---")
    print(json.dumps(all_event_specific_data[target_id_to_show], indent=4))


import pandas as pd
import io  # Needed to treat the string as a file for read_html
import json

# --- Sample Data (as provided in the prompt) ---
# In a real scenario, this would come from your 'all_event_specific_data[1086]'
sample_event_data = all_event_specific_data[target_id_to_show]


def html_table_to_dataframe(event_json_data):
    """
    Extracts an HTML table string from event JSON data and converts it to a Pandas DataFrame.

    Args:
        event_json_data (dict): The JSON data dictionary for a specific event,
                                expected to contain ['htmlList']['table'].

    Returns:
        pandas.DataFrame: The DataFrame created from the HTML table, or None if an error occurs.
    """
    if not isinstance(event_json_data, dict):
        print("Error: Input must be a dictionary.")
        return None

    # Safely extract the HTML table string
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
        # pd.read_html returns a list of DataFrames. We expect only one table.
        list_of_dfs = pd.read_html(io.StringIO(html_table_str))

        if list_of_dfs:
            print("Successfully parsed HTML table into DataFrame.")
            return list_of_dfs[0]  # Return the first DataFrame found
        else:
            # This case is unlikely if the input contains a <table> tag,
            # pd.read_html usually raises ValueError if no tables are found.
            print(
                "Warning: No tables found by pd.read_html, although HTML string was present."
            )
            return None

    except ValueError as ve:
        # This error often means no tables were found in the string
        print(f"Error parsing HTML with pandas (ValueError): {ve}")
        print("Check if the HTML string actually contains a <table> tag.")
        return None
    except ImportError:
        print(
            "Error: The 'lxml' library might be required by pd.read_html. Please install it (`pip install lxml`)."
        )
        # Note: The tool environment usually has common libraries like lxml.
        return None
    except Exception as e:
        print(f"An unexpected error occurred during HTML parsing: {e}")
        return None


# --- Example Usage ---
# Assuming 'sample_event_data' holds the JSON for Event ID 1086
event_dataframe = html_table_to_dataframe(sample_event_data)

if event_dataframe is not None:
    print("\n--- DataFrame for Event ID 1086 ---")
    # Display the DataFrame. print() works, but display() might be nicer in some environments.
    # Using print() for compatibility here.
    print(event_dataframe.to_string())  # .to_string() ensures all rows/cols are printed

    # You can also print specific info, e.g., the first 5 rows:
    # print("\n--- First 5 Rows ---")
    # print(event_dataframe.head())
else:
    print("\nFailed to create DataFrame.")
