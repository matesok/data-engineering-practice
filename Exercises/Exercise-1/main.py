import requests
import os
from zipfile import ZipFile
import io
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = Path("downloads")

def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(extract_zip, download_uris)
    pass

def extract_zip(url: str) -> None:
    try:
        zip_path = DOWNLOAD_DIR / url.split("/")[-1]
        r = requests.get(url)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return
    with open(zip_path, 'wb') as f:
        f.write(r.content)

    try:
        with ZipFile(zip_path) as z:
            for member in z.namelist():
                if member.endswith(".csv") and not member.startswith("__MACOSX/"):
                    z.extract(member, DOWNLOAD_DIR)
    except Exception as e:
        print(f"Failed to extract {zip_path}: {e}")
        return
    finally:
        zip_path.unlink(missing_ok=True)

if __name__ == "__main__":
    main()
