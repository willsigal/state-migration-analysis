import os
import re
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

PAGE_URL = 'https://www.census.gov/data/tables/time-series/demo/geographic-mobility/state-to-state-migration.html'
BASE_DIR = r'C:\Users\user\Desktop\Python Projects\State Migration data anlysis'
RAW_DIR = os.path.join(BASE_DIR, 'raw_files')
OUTPUT_CSV = os.path.join(BASE_DIR, 'state_to_state_migration_normalized.csv')
OUTPUT_PARQUET = os.path.join(BASE_DIR, 'state_to_state_migration_normalized.parquet')

STATE_NAMES = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
    'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
    'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
    'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
    'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
    'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah',
    'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'Puerto Rico'
]
STATE_SET = set(STATE_NAMES)


def normalize_text(value) -> str:
    if value is None:
        return ''
    if isinstance(value, float) and pd.isna(value):
        return ''
    text = str(value)
    text = text.replace('\n', ' ')
    text = text.replace('\xa0', ' ')
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    return text


def normalize_state(value) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    text = text.lstrip('*').strip()
    text = re.sub(r'\d+$', '', text).strip()  # remove footnote suffixes like "Alabama2"

    # Normalize case variants.
    if text.lower() == 'district of columbia':
        text = 'District of Columbia'

    if text in STATE_SET:
        return text
    return None


def parse_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = normalize_text(value)
    if not text:
        return None
    if text in {'X', 'N', 'N/A', '(X)'}:
        return None

    text = text.replace('+/-', '').replace(',', '').replace('%', '').strip()
    if text.startswith('(') and text.endswith(')'):
        text = '-' + text[1:-1]

    try:
        return float(text)
    except ValueError:
        return None


def get_download_links() -> dict[str, str]:
    html = requests.get(PAGE_URL, timeout=30).text
    soup = BeautifulSoup(html, 'html.parser')
    mapping = {}

    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if not re.search(r'\.(xlsx?|csv|zip)$', href.lower()):
            continue
        if href.startswith('//'):
            full_url = 'https:' + href
        elif href.startswith('/'):
            full_url = urljoin(PAGE_URL, href)
        else:
            full_url = href

        if 'www2.census.gov/programs-surveys/demo/tables/geographic-mobility' not in full_url:
            continue
        filename = os.path.basename(urlparse(full_url).path)
        if filename and filename not in mapping:
            mapping[filename] = full_url

    return mapping


def extract_year_range(filename: str) -> tuple[int | None, int | None]:
    years = [int(y) for y in re.findall(r'(?:19|20)\d{2}', filename)]
    if not years:
        return None, None
    return min(years), max(years)


def find_estimate_row(df: pd.DataFrame) -> int | None:
    best_idx = None
    best_score = 0
    for idx in range(min(len(df), 25)):
        row = df.iloc[idx]
        score = 0
        for val in row.tolist():
            text = normalize_text(val).lower()
            if text == 'estimate':
                score += 1
        if score > best_score:
            best_score = score
            best_idx = idx

    if best_score == 0:
        return None
    return best_idx


def parse_long_table(df: pd.DataFrame, filename: str, source_url: str) -> pd.DataFrame:
    records = []
    year_start, year_end = extract_year_range(filename)

    for r in range(len(df)):
        to_state = normalize_state(df.iat[r, 0] if df.shape[1] > 0 else None)
        from_state = normalize_state(df.iat[r, 1] if df.shape[1] > 1 else None)
        if not to_state or not from_state:
            continue

        estimate_raw = normalize_text(df.iat[r, 2] if df.shape[1] > 2 else None)
        moe_raw = normalize_text(df.iat[r, 3] if df.shape[1] > 3 else None)

        records.append(
            {
                'year_start': year_start,
                'year_end': year_end,
                'period_label': f'{year_start}' if year_start == year_end else f'{year_start}-{year_end}',
                'source_file': filename,
                'source_url': source_url,
                'source_sheet': 'Table',
                'from_state': from_state,
                'to_state': to_state,
                'estimate': parse_number(estimate_raw),
                'moe': parse_number(moe_raw),
                'estimate_raw': estimate_raw,
                'moe_raw': moe_raw,
            }
        )

    return pd.DataFrame(records)


def parse_matrix_table(df: pd.DataFrame, filename: str, source_url: str, sheet_name: str) -> pd.DataFrame:
    row_est = find_estimate_row(df)
    if row_est is None or row_est < 1:
        return pd.DataFrame()

    row_origin = row_est - 1

    # Identify estimate columns from header row.
    est_cols = []
    for c in range(df.shape[1]):
        text = normalize_text(df.iat[row_est, c]).lower()
        if text == 'estimate':
            est_cols.append(c)

    if not est_cols:
        return pd.DataFrame()

    # Destination/state row-label columns are columns that carry many recognized state names.
    data_start = row_est + 1
    dest_cols = []
    for c in range(df.shape[1]):
        state_hits = 0
        for r in range(data_start, df.shape[0]):
            if normalize_state(df.iat[r, c]):
                state_hits += 1
        if state_hits >= 10:
            dest_cols.append(c)

    if not dest_cols:
        return pd.DataFrame()

    records = []
    year_start, year_end = extract_year_range(filename)

    for est_col in est_cols:
        # MOE is expected to the right of estimate in these matrix files.
        moe_col = est_col + 1 if est_col + 1 < df.shape[1] else None
        moe_header = normalize_text(df.iat[row_est, moe_col]).lower() if moe_col is not None else ''
        if 'moe' not in moe_header and 'margin' not in moe_header:
            # Skip non-flow estimate columns such as population/same-house estimates.
            continue

        from_state = normalize_state(df.iat[row_origin, est_col])
        if not from_state:
            continue

        # Map each estimate column to the nearest destination-label column to its left.
        possible_dest_cols = [d for d in dest_cols if d <= est_col]
        if not possible_dest_cols:
            continue
        dest_col = max(possible_dest_cols)

        for r in range(data_start, df.shape[0]):
            to_state = normalize_state(df.iat[r, dest_col])
            if not to_state:
                continue

            estimate_raw = normalize_text(df.iat[r, est_col])
            moe_raw = normalize_text(df.iat[r, moe_col]) if moe_col is not None else ''

            if not estimate_raw and not moe_raw:
                continue

            records.append(
                {
                    'year_start': year_start,
                    'year_end': year_end,
                    'period_label': f'{year_start}' if year_start == year_end else f'{year_start}-{year_end}',
                    'source_file': filename,
                    'source_url': source_url,
                    'source_sheet': sheet_name,
                    'from_state': from_state,
                    'to_state': to_state,
                    'estimate': parse_number(estimate_raw),
                    'moe': parse_number(moe_raw),
                    'estimate_raw': estimate_raw,
                    'moe_raw': moe_raw,
                }
            )

    if not records:
        return pd.DataFrame()

    out = pd.DataFrame(records)
    # Deduplicate repeated rows introduced by mirrored/continued blocks.
    out = out.drop_duplicates(
        subset=['source_file', 'source_sheet', 'from_state', 'to_state', 'estimate_raw', 'moe_raw']
    )
    return out


def parse_state_migration_appendix(path: str, filename: str, source_url: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name='Appendix A', header=None, engine='xlrd', dtype=object)
    except Exception:
        return pd.DataFrame()
    return parse_matrix_table(df, filename, source_url, 'Appendix A')


def build_normalized_dataset() -> pd.DataFrame:
    links = get_download_links()
    all_parts = []

    for filename in sorted(os.listdir(RAW_DIR)):
        if not filename.lower().endswith(('.xls', '.xlsx')):
            continue

        path = os.path.join(RAW_DIR, filename)
        source_url = links.get(filename)

        # Dedicated handling for the special multi-table workbook.
        if filename.lower() == 'state_migration_flows_tables.xls':
            part = parse_state_migration_appendix(path, filename, source_url)
            if not part.empty:
                all_parts.append(part)
            continue

        engine = 'openpyxl' if filename.lower().endswith('.xlsx') else 'xlrd'
        sheet_name = 'Table'

        try:
            df = pd.read_excel(path, sheet_name=sheet_name, header=None, engine=engine, dtype=object)
        except Exception:
            try:
                df = pd.read_excel(path, sheet_name=0, header=None, engine=engine, dtype=object)
                sheet_name = 'Sheet1'
            except Exception:
                continue

        # 2024 and future long-format tables use a compact 4-column layout.
        long_candidate = parse_long_table(df, filename, source_url)
        if len(long_candidate) >= 1000:
            all_parts.append(long_candidate)
            continue

        matrix_candidate = parse_matrix_table(df, filename, source_url, sheet_name)
        if not matrix_candidate.empty:
            all_parts.append(matrix_candidate)

    if not all_parts:
        return pd.DataFrame(
            columns=[
                'year_start', 'year_end', 'period_label', 'source_file', 'source_url', 'source_sheet',
                'from_state', 'to_state', 'estimate', 'moe', 'estimate_raw', 'moe_raw'
            ]
        )

    combined = pd.concat(all_parts, ignore_index=True)
    combined = combined.sort_values(
        by=['year_start', 'year_end', 'source_file', 'from_state', 'to_state'],
        na_position='last'
    ).reset_index(drop=True)
    return combined


def main() -> None:
    os.makedirs(BASE_DIR, exist_ok=True)
    df = build_normalized_dataset()
    df.to_csv(OUTPUT_CSV, index=False)

    # Parquet is optional; skip if engine unavailable.
    try:
        df.to_parquet(OUTPUT_PARQUET, index=False)
        parquet_note = f' and {OUTPUT_PARQUET}'
    except Exception:
        parquet_note = ''

    print(f'Wrote {len(df):,} normalized flow rows to {OUTPUT_CSV}{parquet_note}')
    if not df.empty:
        print('Year range:', int(df['year_start'].min()), 'to', int(df['year_end'].max()))
        print('Distinct source files used:', df['source_file'].nunique())


if __name__ == '__main__':
    main()
