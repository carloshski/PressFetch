# NHS England Data Fetching Methodology

This document describes the exact methodology used to discover, download, and process provider-level healthcare statistics from NHS England and NHS Digital. It covers all 8 datasets and can be used to replicate the approach in any application.

---

## Table of Contents

1. [General Architecture](#general-architecture)
2. [Trust Resolution via ODS API](#trust-resolution-via-ods-api)
3. [Shared Utilities](#shared-utilities)
4. [Dataset 1: RTT Waiting Times](#dataset-1-rtt-waiting-times)
5. [Dataset 2: A&E Attendances](#dataset-2-ae-attendances)
6. [Dataset 3: Cancer Waiting Times](#dataset-3-cancer-waiting-times)
7. [Dataset 4: Diagnostics (DM01)](#dataset-4-diagnostics-dm01)
8. [Dataset 5: Ambulance Response Times (AmbSYS)](#dataset-5-ambulance-response-times-ambsys)
9. [Dataset 6: NHS Workforce Statistics](#dataset-6-nhs-workforce-statistics)
10. [Dataset 7: Community Services](#dataset-7-community-services)
11. [Dataset 8: Maternity Services](#dataset-8-maternity-services)
12. [Column Standardisation](#column-standardisation)
13. [Common Pitfalls and Notes](#common-pitfalls-and-notes)

---

## General Architecture

Every dataset follows the same four-step pipeline:

```
1. DISCOVER  ->  Scrape NHS web page(s) to find the latest download link
2. DOWNLOAD  ->  Download the raw file (CSV, ZIP, XLS, XLSX) to data/raw/
3. PROCESS   ->  Load into Pandas, standardise columns, filter by ODS codes
4. SAVE      ->  Write filtered provider-level CSV to data/processed/
```

**Libraries used:** `requests`, `beautifulsoup4`, `pandas`, `openpyxl` (for .xlsx), `zipfile` (for .zip)

**Key principle:** NHS England and NHS Digital do NOT provide stable API endpoints for statistical data. The download links change every month as new publications are added. The system must scrape the publication pages to discover the current links dynamically.

---

## Trust Resolution via ODS API

Before filtering any dataset, you need to resolve NHS Trust ODS codes (e.g. `RD1`, `RA7`) to their full names.

### API Endpoint

```
GET https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/{ODS_CODE}
```

### Example

```
GET https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/RD1
```

Returns JSON with `Organisation.Name` containing the full trust name.

### Search Endpoint (for user trust search)

```
GET https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?Name={QUERY}&Status=Active&PrimaryRoleId=RO197&Limit=20
```

- `PrimaryRoleId=RO197` filters to NHS Trusts only
- `Status=Active` excludes closed/merged trusts
- Returns a list of matching organisations with their ODS codes and names

### Caching Strategy

ODS lookups are cached for 24 hours in a local JSON file (`data/processed/ods_cache.json`). The cache stores the timestamp, and re-resolves if expired or if any trust name contains "Unknown".

### Filtering Logic

When filtering a dataset by ODS codes, the system checks multiple possible column names for the provider/trust code (see [Column Standardisation](#column-standardisation)) and performs a case-insensitive match against the user's selected ODS codes.

---

## Shared Utilities

### File Download (`download_file`)

```python
response = requests.get(url, timeout=timeout, stream=True)
response.raise_for_status()
with open(save_path, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)
```

- Uses streaming downloads for large files
- Calculates SHA-256 hash of downloaded file for integrity verification
- Returns metadata dict with `success`, `file_path`, `size_bytes`, `hash_sha256`

### ZIP Extraction (`extract_zip_file`)

For ZIP files (used by RTT), extracts all files matching a given extension (e.g. `.csv`) to a temporary directory.

### Filename Generation (`generate_filename`)

Creates timestamped filenames: `{prefix}_{YYYYMMDD_HHMMSS}.{ext}`

---

## Dataset 1: RTT Waiting Times

### Source

NHS England Statistical Work Area: Referral to Treatment Waiting Times

### Discovery Method (Two-Level Navigation)

**Step 1: Find the latest year sub-page**

```
Entry URL: https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/
```

Scrape all `<a>` links. Find the first link whose `href` matches the regex OR whose link text matches a secondary pattern:

```regex
# Primary: match in the href
rtt-data-\d{4}-\d{2}

# Fallback: match in the link text (case-insensitive)
\d{4}-\d{2}\s+rtt\s+waiting\s+times\s+data
```

The code checks both in a single pass (`or` condition). This gives the year sub-page URL, e.g.:
```
https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2025-26/
```

**Step 2: Find the Full CSV ZIP on the year sub-page**

Scrape the year sub-page. Look for a link where:
- The link text (case-insensitive) contains `"full csv data file"`
- AND the `href` ends with `.zip`

Fallback: if no text match, look for a link where `href` ends with `.zip` and contains `full-csv` in the URL path.

### File Format

- **Container:** ZIP archive (~4MB compressed, ~80MB uncompressed)
- **Content:** One or more CSV files inside the ZIP
- **Selection logic:** If no file has "provider" or "trust" in its name, use the largest CSV file (the full extract)
- **Typical filename:** `20251231-RTT-December-2025-full-extract.csv`

### Data Structure

| Column | Description |
|--------|-------------|
| Period | Date period (e.g. `December 2025`) |
| Provider Org Code | ODS code of the provider Trust |
| Provider Org Name | Name of the provider Trust |
| RTT Part Type | Type of pathway (admitted/non-admitted/incomplete) |
| Treatment Function Code | Specialty code |
| Total number of incomplete pathways | Count of patients waiting |
| Patients with unknown clock start date | Data quality metric |
| *...various wait band columns...* | Counts by weeks waiting (0-1, 1-2, ... 52+) |

### Filtering

Filter rows where `Provider Org Code` (standardised to `provider_code`) matches any of the user's selected ODS codes.

---

## Dataset 2: A&E Attendances

### Source

NHS England Statistical Work Area: A&E Waiting Times and Activity

### Discovery Method (Two-Level Navigation)

**Step 1: Find the latest year sub-page**

```
Entry URL: https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/
```

Scrape all links. Find the first link whose `href` matches:

```regex
ae-attendances-and-emergency-admissions-\d{4}-\d{2}
```

**Step 2: Find the monthly CSV on the year sub-page**

Scrape the year sub-page. Look for a link where:
- `href` ends with `.csv`
- AND link text contains `csv`, `a&e`, or `ae` (case-insensitive)

Fallback: if no CSV found, look for XLS/XLSX files with `ae` or `a&e` in the text and `provider` in the URL.

### File Format

- **Type:** CSV (typically ~26KB) or occasionally XLS
- **Typical filename:** `January-2026-CSV-S6H81b.csv`

### Data Structure

| Column | Description |
|--------|-------------|
| Period | Month/year |
| Org Code | ODS code |
| Org name | Trust name |
| A&E attendances Type 1 | Major A&E attendances |
| A&E attendances Type 2 | Single-specialty attendances |
| A&E attendances Other A&E Department | Other department attendances |
| A&E attendances Booked Appointments Type 1/2/Other | Booked appointment counts |
| Emergency Admissions via Type 1/2/Other | Emergency admission counts |
| Number of patients spending >4 hours... | 4-hour wait breaches |
| Number of patients spending >12 hours... | 12-hour trolley waits |
| Percentage in 4 hours or less (type 1) | 4-hour performance % |

### Post-Processing

- Column names containing "attendances" + "total" are renamed to `total_attendances`
- Columns with "4" + "hour" + "percent/%" are renamed to `pct_4_hours`
- Columns with "breach" + "12" are renamed to `breaches_12_hour`
- Columns with "emergency" + "admission" are renamed to `emergency_admissions`
- If `pct_4_hours` values are > 1, they are divided by 100 (converting from percentage to decimal)

---

## Dataset 3: Cancer Waiting Times

### Source

NHS England Statistical Work Area: Cancer Waiting Times

### Discovery Method (Single-Level - Direct Links)

```
Entry URL: https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/
```

Unlike RTT and A&E, the Cancer page has direct download links on the main page. No sub-page navigation needed.

Scrape all links. Categorise them into two groups:

**Provider Extracts (preferred):**
- Link text contains `"data extract"` AND `"provider"` (case-insensitive)
- `href` ends with `.xlsx` or `.xls`
- Select the first one found (most recent)

**Combined CSVs (fallback):**
- Link text contains `"combined csv"` (case-insensitive)
- `href` ends with `.csv`

Priority: Provider Extracts are preferred over Combined CSVs because they are smaller and already provider-level.

### File Format - Provider Extract (XLSX)

- **Type:** Excel workbook (~1-5MB)
- **Sheets:** May contain multiple sheets for different metrics
- **Skip sheets:** Any sheet whose name contains `note`, `info`, `content`, `read me`, `readme`, `index`, or `cover`
- **Header detection (two-pass):**
  1. **Primary:** Scan first 20 rows. Count how many cells match keywords: `provider`, `org`, `trust`, `code`, `standard`, `total`, `numerator`, `denominator`. The first row with 2+ matches is the header.
  2. **Fallback:** If no row has 2+ keyword matches, scan again for any row where at least one cell contains `provider`, `org`, or `trust`.

### File Format - Combined CSV (CSV)

- **Type:** Large CSV (~25-60MB)
- **Contains:** All providers, all metrics combined
- **Metric classification:** If a column named `Standard` or `Measure` exists, data is split into sub-DataFrames by metric type

### Metric Classification

The system classifies cancer metrics by scanning text for keywords:

| Metric Key | Keywords in text |
|------------|-----------------|
| `28d` | "28", "faster diagnosis", "fds" |
| `31d` | "31", "first treatment", "decision to treat" |
| `62d` | "62", "urgent" + "referral" |

### Output

Cancer data produces multiple output files, one per metric (e.g. `cwt_28d.csv`, `cwt_62d.csv`, `cwt_cwt_crs_provider_extract.csv`).

---

## Dataset 4: Diagnostics (DM01)

### Source

NHS England Statistical Work Area: Diagnostics Waiting Times and Activity

### Discovery Method (Two-Level Navigation)

**Step 1: Find the latest year sub-page**

```
Entry URL: https://www.england.nhs.uk/statistics/statistical-work-areas/diagnostics-waiting-times-and-activity/monthly-diagnostics-waiting-times-and-activity/
```

Scrape all links. Find the first link whose `href` matches:

```regex
monthly-diagnostics-data-\d{4}-\d{2}
```

**Step 2: Find the provider XLS on the year sub-page**

Scrape the year sub-page. Look for a link where:
- Link text (or URL) contains `"provider"` (case-insensitive)
- AND `href` ends with `.xls` or `.xlsx`

Fallback: any XLS/XLSX link with `diagnostics` in the URL.

### File Format

- **Type:** Excel workbook (XLS or XLSX, ~1.6MB)
- **Engine:** Opens with `pd.ExcelFile(path, engine='openpyxl')` first; if that fails (e.g. for older `.xls` files), falls back to `pd.ExcelFile(path)` which uses the default engine
- **Skip sheets:** Sheets whose name contains `note`, `info`, `content`, `read me`, `readme`, `index`, or `cover` are skipped
- **Header detection (two-pass):**
  1. **Primary:** Scan first 20 rows. Count how many cells in each row match any of these keywords: `provider`, `org`, `trust`, `code`, `diagnostic`, `waiting`, `median`. The first row with 2+ matches is used as the header row.
  2. **Fallback:** If no row has 2+ keyword matches, scan again looking for any row where at least one cell contains `provider`, `org`, or `trust`.
- **Sheet selection:** If a sheet has `"provider"` in its name AND contains valid provider data, it is selected immediately. Otherwise the first sheet with valid provider data is used.

### Data Structure

Varies by publication but typically includes:

| Column | Description |
|--------|-------------|
| Provider Code | ODS code |
| Provider Name | Trust name |
| Diagnostic Test | Type of diagnostic (e.g. MRI, CT, Endoscopy) |
| Waiting List Size | Number of patients on waiting list |
| Median Weeks Wait | Median waiting time in weeks |
| 95th Percentile Weeks Wait | 95th percentile wait |
| *...wait band columns...* | Counts by weeks waiting |

---

## Dataset 5: Ambulance Response Times (AmbSYS)

### Source

NHS England Statistical Work Area: Ambulance Quality Indicators

### Discovery Method (Single-Level - Direct Links)

```
Entry URL: https://www.england.nhs.uk/statistics/statistical-work-areas/ambulance-quality-indicators/
```

Scrape all links on the main page. Look for:

1. **Primary:** Link text contains `"ambsys"` AND `href` ends with `.csv`
2. **Fallback 1:** URL contains `"ambsys"` AND ends with `.csv`
3. **Fallback 2:** Any CSV link with `"ambulance"` in text or URL

### File Format

- **Type:** CSV (contains all ambulance trusts, all months)
- **Typical content:** ~2000 rows covering all ambulance trusts across multiple months

### Data Structure

| Column | Description |
|--------|-------------|
| Year | Year |
| Month | Month |
| Region | NHS region |
| Org Code | ODS code of the ambulance trust |
| Org Name | Trust name |
| A0 | Total calls answered |
| A1 | Category 1 incidents |
| A2 | Category 2 incidents |
| A3 | Category 3 incidents |
| A4 | Category 4 incidents |
| A5-A56 | Various response time metrics |

### Important Note

Ambulance trusts have different ODS codes from acute hospital trusts. If you search for an acute hospital trust code (e.g. `RD1`), you will get 0 rows. Ambulance trust codes are typically like `RYA` (East of England Ambulance), `RX9` (South Western Ambulance), etc.

---

## Dataset 6: NHS Workforce Statistics

### Source

NHS Digital: NHS Workforce Statistics

### Discovery Method (Two-Level Navigation)

**Step 1: Find the latest publication page**

```
Entry URL: https://digital.nhs.uk/data-and-information/publications/statistical/nhs-workforce-statistics
```

Scrape all links. Find the first link where:
- URL contains a month name (`january`, `february`, ..., `december`) AND a 4-digit year
- AND URL contains `nhs-workforce-statistics` or `/statistical/`

Fallback: match by link text instead of URL.

**Step 2: Find the Trust-level CSV on the publication page**

Scrape the publication page. Look for a link where:

1. **Primary:** Link text contains `"trust"` AND `"csv"` (case-insensitive)
2. **Fallback 1:** `href` ends with `.csv` AND text or URL contains `"trust"`
3. **Fallback 2:** Any `.csv` link with `"workforce"` in the URL

### File Format

- **Type:** CSV or Excel
- **Auto-detect:** The system checks the file extension — CSV files are read with `pd.read_csv()`, Excel files use the openpyxl-with-fallback approach: `pd.read_excel(path, engine='openpyxl')`, falling back to `pd.read_excel(path)` on failure
- **Note:** Unlike Diagnostics, the Workforce fetcher reads Excel files as flat DataFrames (no multi-sheet scanning or header detection) since the data is typically in a simpler format

### Data Structure

Varies by publication but typically includes:
- Organisation code/name columns
- Staff group breakdowns (medical, nursing, admin, etc.)
- FTE (Full Time Equivalent) counts
- Headcount figures

### Operational Note

NHS Digital pages (`digital.nhs.uk`) may return `403 Forbidden` for automated HTTP requests. The current code does not add custom User-Agent headers. If you encounter 403 errors, consider adding a browser-like `User-Agent` header to your requests. This is not a code bug but an access restriction on the NHS Digital side.

---

## Dataset 7: Community Services

### Source

NHS Digital: Community Services Statistics for Children, Young People and Adults

### Discovery Method (Two-Level Navigation)

**Step 1: Find the latest publication page**

```
Entry URL: https://digital.nhs.uk/data-and-information/publications/statistical/community-services-statistics-for-children-young-people-and-adults
```

Scrape all links. Find the first link where:
- URL contains `community-services-statistics` AND a 4-digit year
- AND the link is NOT the base URL itself

Fallback: link text matches `(england|annual|quarter)` and contains a 4-digit year.

**Step 2: Find provider-level data on the publication page**

1. **Primary:** Link text contains `"provider"` or `"organisation"` AND `href` ends with `.csv`, `.xlsx`, or `.xls`
2. **Fallback 1:** Link text contains `"csv"` (or href ends with .csv) AND contains `"community"`
3. **Fallback 2:** Any downloadable data file (.csv, .xlsx, .xls)

### File Format

- **Type:** CSV or Excel
- **CSV:** Read directly with `pd.read_csv()`
- **Excel:** Opens with `pd.ExcelFile(path, engine='openpyxl')` first, falling back to `pd.ExcelFile(path)` on failure
- **Skip sheets:** Same list as Diagnostics and Cancer (`note`, `info`, `content`, `read me`, `readme`, `index`, `cover`)
- **Header detection:** Simpler than Diagnostics — scans first 20 rows looking for any row where at least one cell contains `provider`, `org`, or `trust` (no keyword counting, just presence check)
- **Sheet selection:** If a sheet has `"provider"` or `"organisation"` in its name AND has valid provider data, it is selected immediately. Otherwise the first sheet with valid provider data is used.

### Operational Note

Same potential `403 Forbidden` issue as Workforce — see [Workforce Operational Note](#operational-note) for details.

---

## Dataset 8: Maternity Services

### Source

NHS Digital: Maternity Services Monthly Statistics

### Discovery Method (Two-Level Navigation)

**Step 1: Find the latest publication page**

```
Entry URL: https://digital.nhs.uk/data-and-information/publications/statistical/maternity-services-monthly-statistics
```

Scrape all links. Find the first link where:
- URL contains `maternity-services` AND a 4-digit year AND a month name
- AND the link is NOT the base URL itself

Fallback 1: Match by link text — any link text containing a month name AND a 4-digit year.
Fallback 2: Any link with `maternity-services` + a 4-digit year in the URL (broadest match, excludes the base URL itself).

**Step 2: Find provider-level data on the publication page**

Same three-tier fallback as Community Services:
1. Link text has `"provider"` or `"organisation"` + downloadable file extension
2. CSV link with `"maternity"` in text or URL
3. Any downloadable data file

### File Format

- **Type:** CSV or Excel
- **CSV:** Read directly with `pd.read_csv()`
- **Excel:** Same approach as Community — opens with `pd.ExcelFile(path, engine='openpyxl')` first, falling back to `pd.ExcelFile(path)` on failure
- **Skip sheets:** Same list as other Excel datasets
- **Header detection:** Same simpler approach as Community — scans first 20 rows for any row containing `provider`, `org`, or `trust` in at least one cell
- **Sheet selection:** Same as Community — prefers sheets with `"provider"` or `"organisation"` in the name

### Operational Note

Same potential `403 Forbidden` issue as Workforce and Community — see [Workforce Operational Note](#operational-note) for details.

---

## Column Standardisation

All datasets go through column standardisation before filtering. The system identifies the provider code column by scanning all column names for these patterns (case-insensitive):

### Provider Code Column Detection

The system checks column names against this explicit list (checked in order, first match wins):

```python
provider_columns = [
    'Provider_Code', 'Org_Code', 'Organisation_Code', 'Trust_Code',
    'provider_code', 'org_code', 'organisation_code', 'trust_code',
    'Provider Code', 'Org Code', 'Organisation Code', 'Trust Code',
    'PROVIDER_CODE', 'ORG_CODE', 'ORGANISATION_CODE', 'TRUST_CODE',
    'Provider Org Code', 'Provider_Org_Code',
    'Org code', 'Provider code', 'Code',
]
```

If no exact match is found, a secondary fuzzy pass checks all column names: any column whose lowercased, underscore-normalised name contains both `provider` and either `code` or `org` is used as the provider code column.

The matched column is renamed to `provider_code` for consistent downstream use. If no match is found at all, a `ValueError` is raised.

### Trust Name Column Detection

A similar list is checked for the trust name column:

```python
name_columns = [
    'Provider_Name', 'Org_Name', 'Organisation_Name', 'Trust_Name',
    'provider_name', 'org_name', 'organisation_name', 'trust_name',
    'Provider Name', 'Org Name', 'Organisation Name', 'Trust Name',
    'PROVIDER_NAME', 'ORG_NAME', 'ORGANISATION_NAME', 'TRUST_NAME',
    'Provider Org Name', 'Provider_Org_Name',
    'Org name', 'Provider name', 'Name',
]
```

Renamed to `trust_name`. (Name column detection does not raise if missing.)

### ODS Code Filtering

Once the `provider_code` column is standardised, the `ODSResolver.filter_by_ods_codes()` method filters rows:

```python
filtered_df = df[df['provider_code'].str.upper().isin([code.upper() for code in ods_codes])]
```

---

## Common Pitfalls and Notes

### 1. NHS England vs NHS Digital

- **NHS England** hosts: RTT, A&E, Cancer, Diagnostics, Ambulance
  - Base domain: `www.england.nhs.uk`
  - Pages are generally accessible without special headers
- **NHS Digital** hosts: Workforce, Community, Maternity
  - Base domain: `digital.nhs.uk`
  - **Operational caveat:** These pages may return `403 Forbidden` for automated requests without browser-like headers. The current code does not add custom headers, so you may encounter this. Adding a `User-Agent` header (e.g. `Mozilla/5.0 ...`) can resolve it.

### 2. Two-Level vs Single-Level Navigation

| Pattern | Datasets |
|---------|----------|
| Two-level (main page -> year sub-page -> download) | RTT, A&E, Diagnostics, Workforce, Community, Maternity |
| Single-level (main page -> direct download links) | Cancer, Ambulance |

### 3. Excel Header Detection

NHS England Excel files often have several "junk" rows above the actual data headers (titles, blank rows, notes). The system scans the first 20 rows looking for the header row by counting keyword matches. This is essential for reliable processing.

### 4. Relative vs Absolute URLs

Many links on NHS pages use relative paths (starting with `/`). Always check and prepend the base domain:

```python
if href.startswith('/'):
    href = f"https://www.england.nhs.uk{href}"  # or digital.nhs.uk
```

### 5. File Format Ambiguity

Some `.xls` files are actually in the newer `.xlsx` format (or vice versa). Diagnostics, Community, and Maternity use `pd.ExcelFile(path, engine='openpyxl')` as the primary attempt, with a bare `pd.ExcelFile(path)` as fallback. Workforce uses `pd.read_excel(path, engine='openpyxl')` with the same fallback pattern. Cancer uses `pd.ExcelFile()` without specifying an engine.

### 6. Data Freshness

NHS publications are typically updated monthly. The system tracks file modification timestamps and flags data as "stale" if older than 7 days.

### 7. Request Timeouts (as configured in code)

- Page scraping (all datasets): 30 seconds
- A&E CSV download: 60 seconds
- RTT ZIP download: 120 seconds
- Cancer XLSX/CSV download: 120 seconds
- Diagnostics XLS download: 120 seconds
- Ambulance CSV download: 60 seconds
- Workforce download: 120 seconds
- Community download: 120 seconds
- Maternity download: 120 seconds

### 8. Output File Naming

Each dataset produces a "latest" file with a fixed name (e.g. `rtt_provider.csv`) plus a timestamped copy (e.g. `rtt_provider_20260219_164701.csv`). The fixed-name file is always overwritten with the newest data, making it easy to reference from dashboards.

### 9. Metadata Columns

Every processed dataset gets two extra columns appended:
- `data_source`: e.g. "NHS England RTT Statistics"
- `processing_date`: ISO timestamp of when the data was processed

### 10. Summary of All Entry URLs

| Dataset | Entry URL |
|---------|-----------|
| RTT | `https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/` |
| A&E | `https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/` |
| Cancer | `https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/` |
| Diagnostics | `https://www.england.nhs.uk/statistics/statistical-work-areas/diagnostics-waiting-times-and-activity/monthly-diagnostics-waiting-times-and-activity/` |
| Ambulance | `https://www.england.nhs.uk/statistics/statistical-work-areas/ambulance-quality-indicators/` |
| Workforce | `https://digital.nhs.uk/data-and-information/publications/statistical/nhs-workforce-statistics` |
| Community | `https://digital.nhs.uk/data-and-information/publications/statistical/community-services-statistics-for-children-young-people-and-adults` |
| Maternity | `https://digital.nhs.uk/data-and-information/publications/statistical/maternity-services-monthly-statistics` |
| ODS API | `https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/{CODE}` |
| ODS Search | `https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?Name={QUERY}&Status=Active&PrimaryRoleId=RO197&Limit=20` |
