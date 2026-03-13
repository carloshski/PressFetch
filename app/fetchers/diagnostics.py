"""
Diagnostics waiting times and activity data fetcher.
Fetches and processes NHS England monthly diagnostics statistics.
"""

import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ..utils.audit import AuditLogger, DataValidator
from ..utils.io import download_file, generate_filename, standardize_provider_column
from ..utils.ods import ODSResolver


class DiagnosticsFetcher:
    """Fetches and processes diagnostics waiting times data from NHS England."""

    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")

        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        self.validator = DataValidator()

        self.base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/diagnostics-waiting-times-and-activity/monthly-diagnostics-waiting-times-and-activity/"

    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest diagnostics XLS download link by navigating the two-level
        NHS England page structure: main page -> year sub-page -> download links.
        """
        try:
            print(f"Diagnostics: Fetching main page {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)

            year_page_url = None
            for link in links:
                href = link['href']
                if re.search(r'monthly-diagnostics-data-\d{4}-\d{2}', href):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    year_page_url = href
                    print(f"Diagnostics: Found latest year page: {href}")
                    break

            if not year_page_url:
                print("Diagnostics: No year sub-page found on main page")
                return None

            print(f"Diagnostics: Fetching year sub-page {year_page_url}")
            response2 = requests.get(year_page_url, timeout=30)
            response2.raise_for_status()

            soup2 = BeautifulSoup(response2.content, 'html.parser')
            links2 = soup2.find_all('a', href=True)

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if ('provider' in text_lower or 'provider' in href_lower) and (href_lower.endswith('.xls') or href_lower.endswith('.xlsx')):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"Diagnostics: Found XLS download: {link_text} -> {href}")
                    return href, link_text

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                href_lower = href.lower()
                if (href_lower.endswith('.xls') or href_lower.endswith('.xlsx')) and 'diagnostics' in href_lower:
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"Diagnostics: Found XLS download (fallback): {link_text} -> {href}")
                    return href, link_text

            print("Diagnostics: No XLS download found on year sub-page")
            return None

        except Exception as e:
            print(f"Diagnostics: Error discovering link: {e}")
            self.audit_logger.log_operation('diagnostics', 'discover', False, {'error': str(e)})
            return None

    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest diagnostics data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('diagnostics', 'discover', False,
                                          {'error': 'No Diagnostics download link found'})
            return None

        download_url, description = link_info

        ext = "xlsx" if download_url.lower().endswith('.xlsx') else "xls"
        filename = generate_filename("diagnostics_data", ext)
        save_path = os.path.join(self.raw_dir, filename)

        download_result = download_file(download_url, save_path, timeout=120)

        self.audit_logger.log_download(
            'diagnostics',
            download_url,
            save_path,
            download_result['success'],
            download_result.get('hash_sha256'),
            download_result.get('size_bytes'),
            download_result.get('error')
        )

        if download_result['success']:
            download_result['description'] = description

        return download_result

    def process_diagnostics_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process the diagnostics data file and filter by ODS codes."""
        try:
            try:
                excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            except Exception:
                excel_file = pd.ExcelFile(file_path)

            sheet_names = excel_file.sheet_names
            print(f"Diagnostics: Sheets found: {sheet_names}")

            provider_df = None
            for sheet_name in sheet_names:
                sheet_lower = sheet_name.lower().strip()
                if any(skip in sheet_lower for skip in ['note', 'info', 'content', 'read me', 'readme', 'index', 'cover']):
                    continue

                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)

                    header_row = None
                    for idx in range(min(20, len(df))):
                        row_vals = df.iloc[idx].astype(str).str.lower()
                        provider_matches = sum(1 for v in row_vals if any(k in v for k in ['provider', 'org', 'trust', 'code', 'diagnostic', 'waiting', 'median']))
                        if provider_matches >= 2:
                            header_row = idx
                            break

                    if header_row is None:
                        for idx in range(min(20, len(df))):
                            row_vals = df.iloc[idx].astype(str).str.lower()
                            if any('provider' in v or 'org' in v or 'trust' in v for v in row_vals):
                                header_row = idx
                                break

                    if header_row is None:
                        continue

                    df.columns = df.iloc[header_row]
                    df = df.iloc[header_row + 1:].reset_index(drop=True)
                    df.columns = [str(c).strip() if pd.notna(c) else f'col_{i}' for i, c in enumerate(df.columns)]

                    try:
                        df = standardize_provider_column(df)
                    except ValueError:
                        continue

                    if 'provider' in sheet_lower or provider_df is None:
                        provider_df = df
                        print(f"Diagnostics: Using sheet '{sheet_name}' with {len(df)} rows")
                        if 'provider' in sheet_lower:
                            break

                except Exception as e:
                    print(f"Diagnostics: Error reading sheet '{sheet_name}': {e}")
                    continue

            excel_file.close()

            if provider_df is None:
                print("Diagnostics: No suitable sheet found with provider data")
                return None

            print(f"Diagnostics: Loaded data with {len(provider_df)} rows, columns: {list(provider_df.columns[:10])}")

            filtered_df = self.ods_resolver.filter_by_ods_codes(provider_df, 'provider_code', self.ods_codes)

            filtered_df['data_source'] = 'NHS England Diagnostics Statistics'
            filtered_df['processing_date'] = datetime.now().isoformat()

            print(f"Diagnostics: Filtered to {len(filtered_df)} rows for codes {self.ods_codes}")
            return filtered_df

        except Exception as e:
            print(f"Diagnostics: Error processing data: {e}")
            self.audit_logger.log_operation('diagnostics', 'process', False,
                                          {'error': str(e), 'file_path': file_path})
            return None

    def save_processed_data(self, df: pd.DataFrame) -> str:
        """Save processed diagnostics data to CSV."""
        filename = generate_filename("diagnostics_provider", "csv")
        output_path = os.path.join(self.processed_dir, filename)

        os.makedirs(self.processed_dir, exist_ok=True)

        df.to_csv(output_path, index=False)

        latest_path = os.path.join(self.processed_dir, "diagnostics_provider.csv")
        df.to_csv(latest_path, index=False)

        return output_path

    def fetch_and_process(self) -> Dict:
        """Complete diagnostics data fetch and processing pipeline."""
        results = {
            'dataset': 'diagnostics',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }

        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download Diagnostics data'
                return results

            results['download'] = download_result

            processed_df = self.process_diagnostics_data(download_result['file_path'])
            if processed_df is None or processed_df.empty:
                results['error'] = 'Failed to process Diagnostics data or no matching records'
                return results

            output_path = self.save_processed_data(processed_df)

            results.update({
                'success': True,
                'output_file': output_path,
                'record_count': len(processed_df),
                'providers': processed_df['provider_code'].unique().tolist(),
                'columns': processed_df.columns.tolist()
            })

            self.audit_logger.log_processing(
                'diagnostics',
                download_result['file_path'],
                [output_path],
                True,
                len(processed_df)
            )

        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('diagnostics', 'fetch_and_process', False, {'error': str(e)})

        return results
