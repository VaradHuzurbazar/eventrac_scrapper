import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from typing import Optional


import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlencode

BASE = "https://www.eventrac.co.uk/e/hibernal-hills-10870/entrants"

PARAMS_BASE = {
    "sort": "registration_category_id",
    "direction": "asc",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; entrants-scraper/1.0; +https://example.com)"
}

def get_total_pages(html: str) -> Optional[int]:
    # Looks like: "Page 2 of 10"
    m = re.search(r"Page\s+\d+\s+of\s+(\d+)", html)
    return int(m.group(1)) if m else None

def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    # Get headers (First Name, Last Name, Club, Age, Category, Race)
    header_row = table.find("tr")
    if not header_row:
        return []

    headers = [th.get_text(" ", strip=True) for th in header_row.find_all(["th", "td"])]
    # Normalize headers
    headers = [h.strip() for h in headers]

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        cells = [td.get_text(" ", strip=True) for td in tds]
        if len(cells) != len(headers):
            # If structure changes, skip row rather than corrupt data
            continue
        row = dict(zip(headers, cells))
        rows.append(row)

    return rows

def fetch_page(page: int) -> str:
    params = dict(PARAMS_BASE)
    params["page"] = page
    url = f"{BASE}?{urlencode(params)}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def main():
    html1 = fetch_page(1)
    total_pages = get_total_pages(html1) or 1

    all_rows = []
    for page in range(1, total_pages + 1):
        html = html1 if page == 1 else fetch_page(page)
        rows = parse_table(html)
        if not rows:
            break
        all_rows.extend(rows)

        # be polite
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("No rows parsed. The page structure may have changed.")

    df = pd.DataFrame(all_rows)

    # Keep just what you asked for (plus keep Club/Category if you want)
    keep = [c for c in ["First Name", "Last Name", "Race"] if c in df.columns]
    df_out = df[keep].copy()

    out_path = "hibernal_hills_entrants.csv"
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(df_out)} rows to {out_path}")

if __name__ == "__main__":
    main()

