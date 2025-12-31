import requests
import pandas
from bs4 import BeautifulSoup


url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'

def extract_zip(url: str) -> str:
    found = False
    res = requests.get(url)
    if res.status_code != 200:
        print(f"Failed to retrieve data from {url}")
        return
    soup = BeautifulSoup(res.text, 'html.parser')
    table = soup.find('table')
    rows = table.find("tbody").find_all("tr")[3:-1]
    for tr in rows:
        tds = tr.find_all("td")
        if('2024-01-19 14:48' in tds[1].text) and not found:
            found = True
            return tds[0].text
    return None

def fetch_csv(file_name: str) -> None:
    final_url = url+file_name
    res = requests.get(final_url)
    with open(file_name, 'wb') as f:
        f.write(res.content)

def extract_hourly_max_temperature(file_name):
    df = pandas.read_csv(file_name)
    print(df.head())
    return df[df['HourlyDryBulbTemperature']  == df['HourlyDryBulbTemperature'].max()]

def main():
    file_name = extract_zip(url)
    fetch_csv(file_name)
    result = extract_hourly_max_temperature(file_name)
    print(result)

if __name__ == "__main__":
    main()
