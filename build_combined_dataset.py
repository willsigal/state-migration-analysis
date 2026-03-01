import os
import re
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

PAGE_URL = 'https://www.census.gov/data/tables/time-series/demo/geographic-mobility/state-to-state-migration.html'
BASE_DIR = r'C:\Users\user\Desktop\Python Projects\State Migration data anlysis'
RAW_DIR = os.path.join(BASE_DIR, 'raw_files')
OUTPUT_CSV = os.path.join(BASE_DIR, 'state_to_state_migration_all_cells.csv')
MANIFEST_CSV = os.path.join(BASE_DIR, 'download_manifest.csv')


def normalize_url(href: str) -> str:
    if href.startswith('//'):
        return 'https:' + href
    if href.startswith('/'):
        return urljoin(PAGE_URL, href)
    return href


def get_download_links() -> list[tuple[str, str]]:
    html = requests.get(PAGE_URL, timeout=30).text
    soup = BeautifulSoup(html, 'html.parser')
    items = []

    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        text = ' '.join(a.get_text(' ', strip=True).split())
        if not re.search(r'\.(xlsx?|csv|zip)$', href.lower()):
            continue
        full_url = normalize_url(href)
        if 'www2.census.gov/programs-surveys/demo/tables/geographic-mobility' not in full_url:
            continue
        filename = os.path.basename(urlparse(full_url).path)
        if not filename:
            continue
        items.append((filename, full_url, text))

    # Deduplicate by filename while preserving first-seen link.
    seen = set()
    deduped = []
    for filename, url, text in items:
        if filename in seen:
            continue
        seen.add(filename)
        deduped.append((filename, url, text))
    return deduped


def extract_years(filename: str) -> tuple[int | None, int | None]:
    years = [int(y) for y in re.findall(r'(?:19|20)\d{2}', filename)]
    if not years:
        return None, None
    return min(years), max(years)


def parse_numeric(value: str):
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip()
    if cleaned in {'X', 'N', 'N/A', '(X)'}:
        return None
    # Convert values like "+/- 8,215" and "(1,234)" to numeric.
    cleaned = cleaned.replace('+/-', '').replace(',', '').replace('%', '').strip()
    if cleaned.startswith('(') and cleaned.endswith(')'):
        cleaned = '-' + cleaned[1:-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def combine_all_files(download_links: list[tuple[str, str, str]]) -> pd.DataFrame:
    url_by_file = {filename: url for filename, url, _ in download_links}
    title_by_file = {filename: text for filename, _, text in download_links}
    frames = []

    for filename in sorted(os.listdir(RAW_DIR)):
        if not filename.lower().endswith(('.xls', '.xlsx')):
            continue

        path = os.path.join(RAW_DIR, filename)
        engine = 'openpyxl' if filename.lower().endswith('.xlsx') else 'xlrd'
        year_start, year_end = extract_years(filename)

        try:
            workbook = pd.ExcelFile(path, engine=engine)
        except Exception as exc:
            print(f'Skipping {filename}: failed to open workbook ({exc})')
            continue

        for sheet in workbook.sheet_names:
            try:
                df = pd.read_excel(path, sheet_name=sheet, header=None, engine=engine, dtype=object)
            except Exception as exc:
                print(f'Skipping {filename} / {sheet}: read error ({exc})')
                continue

            stacked = df.stack().reset_index()
            if stacked.empty:
                continue

            stacked.columns = ['row_index_0based', 'column_index_0based', 'cell_value_raw']
            stacked = stacked[stacked['cell_value_raw'].notna()]
            stacked['cell_value_raw'] = stacked['cell_value_raw'].astype(str).str.strip()
            stacked = stacked[stacked['cell_value_raw'] != '']
            if stacked.empty:
                continue

            stacked['source_file'] = filename
            stacked['source_url'] = url_by_file.get(filename)
            stacked['source_link_text'] = title_by_file.get(filename)
            stacked['sheet_name'] = sheet
            stacked['year_start'] = year_start
            stacked['year_end'] = year_end
            stacked['row_number'] = stacked['row_index_0based'] + 1
            stacked['column_number'] = stacked['column_index_0based'] + 1
            stacked['cell_value_numeric'] = stacked['cell_value_raw'].map(parse_numeric)

            frames.append(
                stacked[
                    [
                        'source_file',
                        'source_url',
                        'source_link_text',
                        'sheet_name',
                        'year_start',
                        'year_end',
                        'row_number',
                        'column_number',
                        'cell_value_raw',
                        'cell_value_numeric',
                    ]
                ]
            )

    if not frames:
        return pd.DataFrame(
            columns=[
                'source_file',
                'source_url',
                'source_link_text',
                'sheet_name',
                'year_start',
                'year_end',
                'row_number',
                'column_number',
                'cell_value_raw',
                'cell_value_numeric',
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(
        by=['year_start', 'source_file', 'sheet_name', 'row_number', 'column_number'],
        na_position='last',
    ).reset_index(drop=True)
    return combined


def write_manifest(download_links: list[tuple[str, str, str]]) -> None:
    rows = []
    for filename, url, text in download_links:
        local_path = os.path.join(RAW_DIR, filename)
        rows.append(
            {
                'filename': filename,
                'url': url,
                'link_text': text,
                'downloaded': os.path.exists(local_path),
                'bytes': os.path.getsize(local_path) if os.path.exists(local_path) else None,
            }
        )
    manifest = pd.DataFrame(rows).sort_values('filename').reset_index(drop=True)
    manifest.to_csv(MANIFEST_CSV, index=False)


def main() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    links = get_download_links()
    write_manifest(links)
    combined = combine_all_files(links)
    combined.to_csv(OUTPUT_CSV, index=False)

    print(f'Wrote {len(combined):,} rows to {OUTPUT_CSV}')
    print(f'Wrote manifest: {MANIFEST_CSV}')


if __name__ == '__main__':
    main()
