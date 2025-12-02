import os
import requests

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"  # NYC 311 service requests [web:21]

def download_nyc_311_csv(year: int = 2023, limit: int = 200000, output_file: str | None = None) -> str | bool:
    if output_file is None:
        output_file = f"./data/nyc_311_{year}.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    soql_query = (
        f"?$where=created_date between '{year}-01-01T00:00:00' and "
        f"'{year}-12-31T23:59:59'&$limit={limit}"
    )
    download_url = BASE_URL + soql_query

    print(f"[⬇] Downloading NYC 311 data for {year} from:\n{download_url}")

    resp = requests.get(download_url, stream=True, timeout=60)

    if resp.status_code != 200:
        print(f"[❌] Failed to download dataset. Status code: {resp.status_code}")
        return False

    with open(output_file, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"[✅] Download complete. Saved to {output_file}")
    return output_file


if __name__ == "__main__":
    year = int(os.getenv("NYC311_YEAR", "2023"))
    limit = int(os.getenv("NYC311_LIMIT", "200000"))
    download_nyc_311_csv(year=year, limit=limit)
