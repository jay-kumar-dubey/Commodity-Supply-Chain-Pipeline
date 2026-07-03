import requests
import os
import json
from pathlib import Path
from datetime import datetime , timedelta
from dotenv import load_dotenv
import boto3

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
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        error_msg = str(e).replace(API_KEY, "***")
        raise ConnectionError(f"Network error connecting to EIA API: {error_msg}")
    except requests.exceptions.HTTPError as e:
        error_msg = str(e).replace(API_KEY, "***")
        raise RuntimeError(f"EIA API returned an error: {error_msg}")
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

def upload_to_s3(local_file_path: str, s3_key: str) -> None:

    s3 = boto3.client(
        's3',
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name = os.environ.get('AWS_REGION')
    )

    bucket = os.environ.get('AWS_BUCKET_NAME')
    s3.upload_file(local_file_path, bucket, s3_key)
    print(f"Uploaded to S3: //{bucket} / {s3_key}")

def main():
    today = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=40)).strftime("%Y-%m-%d")

    print(f"Fetching WTI prices from {start} to {today}...")
    
    raw_data = fetch_wti_prices(start_date=start, end_date=today)
    
    record_count = len(raw_data['response']['data'])
    print(f"Records fetched: {record_count}")
    
    file_path = save_raw_response(raw_data, today)
    print(f"Saved to: {file_path}")

    year = datetime.today().strftime("%Y")
    month = datetime.today().strftime("%m")
    s3_key = f"bronze/eia_oil_prices/year={year}/month={month}/raw_{today}.json"
    upload_to_s3(file_path, s3_key)


if __name__ == "__main__":
    main()