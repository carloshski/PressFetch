"""
Maternity Services Monthly Statistics data fetcher.
Fetches and processes NHS Digital maternity services statistics.
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


class MaternityFetcher:
    """Fetches and processes maternity services statistics data from NHS Digital."""

    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")

        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        self.validator = DataValidator()

        self.base_url = "https://digital.nhs.uk/data-and-information/publications/statistical/maternity-services-monthly-statistics"

    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest maternity services data download link by navigating:
        main page -> latest publication page -> CSV/Excel download.
        """
        try:
            print(f"Maternity: Fetching main page {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)

            publication_url = None
            months = ['january', 'february', 'march', 'april', 'may', 'june',
                      'july', 'august', 'september', 'october', 'november', 'december']

            for link in links:
                href = link['href']
                href_lower = href.lower()
                if 'maternity-services' in href_lower and re.search(r'\d{4}', href_lower):
                    if any(month in href_lower for month in months):
                        if href != self.base_url and href.rstrip('/') != self.base_url.rstrip('/'):
                            if href.startswith('/'):
                                href = f"https://digital.nhs.uk{href}"
                            publication_url = href
                            print(f"Maternity: Found latest publication: {href}")
                            break

            if not publication_url:
                for link in links:
                    href = link['href']
                    link_text = link.get_text(strip=True).lower()
                    if any(month in link_text for month in months) and re.search(r'\d{4}', link_text):
                        if href.startswith('/'):
                            href = f"https://digital.nhs.uk{href}"
                        publication_url = href
                        print(f"Maternity: Found publication (by text): {href}")
                        break

            if not publication_url:
                for link in links:
                    href = link['href']
                    href_lower = href.lower()
                    if 'maternity-services' in href_lower and href != self.base_url and href.rstrip('/') != self.base_url.rstrip('/'):
                        if re.search(r'\d{4}', href_lower):
                            if href.startswith('/'):
                                href = f"https://digital.nhs.uk{href}"
                            publication_url = href
                            print(f"Maternity: Found publication (broad): {href}")
                            break

            if not publication_url:
                print("Maternity: No publication page found")
                return None

            print(f"Maternity: Fetching publication page {publication_url}")
            response2 = requests.get(publication_url, timeout=30)
            response2.raise_for_status()

            soup2 = BeautifulSoup(response2.content, 'html.parser')
            links2 = soup2.find_all('a', href=True)

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if ('provider' in text_lower or 'organisation' in text_lower) and (href_lower.endswith('.csv') or href_lower.endswith('.xlsx') or href_lower.endswith('.xls')):
                    if href.startswith('/'):
                        href = f"https://digital.nhs.uk{href}"
                    print(f"Maternity: Found provider data: {link_text} -> {href}")
                    return href, link_text

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if ('csv' in text_lower or href_lower.endswith('.csv')) and ('maternity' in text_lower or 'maternity' in href_lower):
                    if href.startswith('/'):
                        href = f"https://digital.nhs.uk{href}"
                    print(f"Maternity: Found CSV (fallback): {link_text} -> {href}")
                    return href, link_text

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                href_lower = href.lower()
                if href_lower.endswith('.csv') or href_lower.endswith('.xlsx') or href_lower.endswith('.xls'):
                    if href.startswith('/'):
                        href = f"https://digital.nhs.uk{href}"
                    print(f"Maternity: Found data file (broad fallback): {link_text} -> {href}")
                    return href, link_text

            print("Maternity: No data download found on publication page")
            return None

        except Exception as e:
            print(f"Maternity: Error discovering link: {e}")
            self.audit_logger.log_operation('maternity', 'discover', False, {'error': str(e)})
            return None

    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest maternity services data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('maternity', 'discover', False,
                                          {'error': 'No Maternity download link found'})
            return None

        download_url, description = link_info

        if download_url.lower().endswith('.csv'):
            ext = "csv"
        elif download_url.lower().endswith('.xlsx'):
            ext = "xlsx"
        else:
            ext = "xls"
        filename = generate_filename("maternity_data", ext)
        save_path = os.path.join(self.raw_dir, filename)

        download_result = download_file(download_url, save_path, timeout=120)

        self.audit_logger.log_download(
            'maternity',
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

    def process_maternity_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process the maternity services data file and filter by ODS codes."""
        try:
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                try:
                    excel_file = pd.ExcelFile(file_path, engine='openpyxl')
                except Exception:
                    excel_file = pd.ExcelFile(file_path)

                sheet_names = excel_file.sheet_names
                print(f"Maternity: Sheets found: {sheet_names}")

                df = None
                for sheet_name in sheet_names:
                    sheet_lower = sheet_name.lower().strip()
                    if any(skip in sheet_lower for skip in ['note', 'info', 'content', 'read me', 'readme', 'index', 'cover']):
                        continue

                    try:
                        temp_df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)

                        header_row = None
                        for idx in range(min(20, len(temp_df))):
                            row_vals = temp_df.iloc[idx].astype(str).str.lower()
                            if any('provider' in v or 'org' in v or 'trust' in v for v in row_vals):
                                header_row = idx
                                break

                        if header_row is None:
                            continue

                        temp_df.columns = temp_df.iloc[header_row]
                        temp_df = temp_df.iloc[header_row + 1:].reset_index(drop=True)
                        temp_df.columns = [str(c).strip() if pd.notna(c) else f'col_{i}' for i, c in enumerate(temp_df.columns)]

                        try:
                            temp_df = standardize_provider_column(temp_df)
                        except ValueError:
                            continue

                        if 'provider' in sheet_lower or 'organisation' in sheet_lower or df is None:
                            df = temp_df
                            print(f"Maternity: Using sheet '{sheet_name}' with {len(temp_df)} rows")
                            if 'provider' in sheet_lower or 'organisation' in sheet_lower:
                                break

                    except Exception as e:
                        print(f"Maternity: Error reading sheet '{sheet_name}': {e}")
                        continue

                excel_file.close()

                if df is None:
                    print("Maternity: No suitable sheet found with provider data")
                    return None

            print(f"Maternity: Loaded data with {len(df)} rows, columns: {list(df.columns[:10])}")

            df = standardize_provider_column(df)

            filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)

            filtered_df['data_source'] = 'NHS Digital Maternity Services Statistics'
            filtered_df['processing_date'] = datetime.now().isoformat()

            print(f"Maternity: Filtered to {len(filtered_df)} rows for codes {self.ods_codes}")
            return filtered_df

        except Exception as e:
            print(f"Maternity: Error processing data: {e}")
            self.audit_logger.log_operation('maternity', 'process', False,
                                          {'error': str(e), 'file_path': file_path})
            return None

    def save_processed_data(self, df: pd.DataFrame) -> str:
        """Save processed maternity services data to CSV."""
        filename = generate_filename("maternity_provider", "csv")
        output_path = os.path.join(self.processed_dir, filename)

        os.makedirs(self.processed_dir, exist_ok=True)

        df.to_csv(output_path, index=False)

        latest_path = os.path.join(self.processed_dir, "maternity_provider.csv")
        df.to_csv(latest_path, index=False)

        return output_path

    def fetch_and_process(self) -> Dict:
        """Complete maternity services data fetch and processing pipeline."""
        results = {
            'dataset': 'maternity',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }

        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download Maternity data'
                return results

            results['download'] = download_result

            processed_df = self.process_maternity_data(download_result['file_path'])
            if processed_df is None or processed_df.empty:
                results['error'] = 'Failed to process Maternity data or no matching records'
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
                'maternity',
                download_result['file_path'],
                [output_path],
                True,
                len(processed_df)
            )

        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('maternity', 'fetch_and_process', False, {'error': str(e)})

        return results
