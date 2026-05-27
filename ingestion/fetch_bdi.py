import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
import json
import boto3

load_dotenv()


API_KEY = os.environ.get("FRED_API_KEY")
if not API_KEY:
    raise ValueError("FRED_API_KEY not found. Check your .env file.")

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def bdi_data(start_date: str, end_date: str) -> dict:
    """
    Fetch FRED data from the EIA API.
    """
    params = {
    "series_id": "PPIFIS",
    "api_key": API_KEY,
    "file_type": "json",
    "sort_order": "desc",
    "observation_start": start_date,
    "observation_end": end_date
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if not data['observations']: 
        raise ValueError(f"API returned empty data for range {start_date} to {end_date}")
    if response.status_code == 200:
        return data

def save_raw_response(data: dict, fetch_month: str) -> str:
    folder_path = Path("data/bronze/baltic_dry_index")
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / f"raw_{fetch_month}.json"
    
    if file_path.exists():
        print(f"Data for {fetch_month} already exists. Skipping save.")
        return str(file_path)
    
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)
    return str(file_path)

def upload_to_s3(local_file_path:str, s3_key: str) -> None:

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION')
    )

    bucket = os.environ.get('AWS_BUCKET_NAME')
    s3.upload_file(local_file_path, bucket , s3_key)
    print(f"Upload to s3: // {bucket}/ {s3_key}")

def main():
    start = "2025-01-01"
    today = datetime.today().strftime("%Y-%m-%d")
    fetch_month = datetime.today().strftime("%Y-%m")
    
    print(f"Fetching shipping index from {start} to {today}...")
    data = bdi_data(start_date=start, end_date=today)
    
    record_count = len(data['observations'])
    print(f"Records fetched: {record_count}")
    
    file_path = save_raw_response(data, fetch_month)
    print(f"Saved to: {file_path}")

    year = datetime.today().strftime("%Y")
    month = datetime.today().strftime("%m")

    s3_key = f"bronze/baltic_dry_index/year={year}/month={month}/raw_{fetch_month}.json"
    upload_to_s3(file_path, s3_key)

if __name__ == "__main__":
    main()  
