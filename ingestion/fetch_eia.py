# import requests
# import os
# from dotenv import load_dotenv

# load_dotenv()

# API_KEY = os.environ.get("EIA_API_KEY")

# BASE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

# params = {
#     "api_key": API_KEY,
#     "frequency": "daily",
#     "data[0]": "value",
#     "facets[product][]": "EPCWTI",
#     "facets[duoarea][]": "YCUOK",
#     "sort[0][column]": "period",
#     "sort[0][direction]": "desc",
#     "sort[1][column]": "value",
#     "sort[1][direction]": "asc",
#     "length": 5
# }

# response = requests.get(BASE_URL, params=params)
# data = response.json()
# records = data['response']['data']

# for r in records:
#     print(r)

import requests
import os
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("EIA_API_KEY")
if not API_KEY:
    raise ValueError("EIA_API_KEY not found. Check your .env file.")

BASE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"


def fetch_wti_prices(start_date: str, end_date: str) -> dict:
    """
    Fetch WTI crude oil spot prices from EIA API.
    start_date and end_date format: 'YYYY-MM-DD'
    Returns the full raw API response as a dictionary.
    """
    params = {
        "api_key": API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[product][]": "EPCWTI",
        "facets[duoarea][]": "YCUOK",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        'start': start_date,
        'end': end_date
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    # call response.raise_for_status() to handle errors
    if not data['response']['data']: 
        raise ValueError(f"API returned empty data for range {start_date} to {end_date}")
    return data


def save_raw_response(data: dict, fetch_date: str) -> str:
    """
    Save raw API response to local bronze folder.
    Returns the file path where it was saved.
    """
    folder_path = Path("data/bronze/eia_oil_prices")
    folder_path.mkdir(parents=True, exist_ok=True)
    filename = f"raw_{fetch_date}.json"
    file_path = folder_path / filename
    
    # Remove API key from saved file
    if 'request' in data and 'params' in data['request']:
        data['request']['params'].pop('api_key', None)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)
    return str(file_path)


def main():
    today = datetime.today().strftime("%Y-%m-%d")
    start = "2025-01-01"

    print(f"Fetching WTI prices from {start} to {today}...")
    
    raw_data = fetch_wti_prices(start_date=start, end_date=today)
    
    record_count = len(raw_data['response']['data'])
    print(f"Records fetched: {record_count}")
    
    file_path = save_raw_response(raw_data, today)
    print(f"Saved to: {file_path}")


if __name__ == "__main__":
    main()