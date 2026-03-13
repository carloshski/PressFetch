"""
NHS Workforce Statistics data fetcher.
Fetches and processes NHS Digital workforce statistics.
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


class WorkforceFetcher:
    """Fetches and processes workforce statistics data from NHS Digital."""

    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")

        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        self.validator = DataValidator()

        self.base_url = "https://digital.nhs.uk/data-and-information/publications/statistical/nhs-workforce-statistics"

    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest workforce CSV download link by navigating two levels:
        main page -> latest publication page -> CSV download.
        """
        try:
            print(f"Workforce: Fetching main page {self.base_url}")
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
                if any(month in href_lower for month in months) and re.search(r'\d{4}', href_lower):
                    if 'nhs-workforce-statistics' in href_lower or '/statistical/' in href_lower:
                        if href.startswith('/'):
                            href = f"https://digital.nhs.uk{href}"
                        publication_url = href
                        print(f"Workforce: Found latest publication: {href}")
                        break

            if not publication_url:
                for link in links:
                    href = link['href']
                    link_text = link.get_text(strip=True).lower()
                    if any(month in link_text for month in months) and re.search(r'\d{4}', link_text):
                        if href.startswith('/'):
                            href = f"https://digital.nhs.uk{href}"
                        publication_url = href
                        print(f"Workforce: Found latest publication (by text): {href}")
                        break

            if not publication_url:
                print("Workforce: No publication page found")
                return None

            print(f"Workforce: Fetching publication page {publication_url}")
            response2 = requests.get(publication_url, timeout=30)
            response2.raise_for_status()

            soup2 = BeautifulSoup(response2.content, 'html.parser')
            links2 = soup2.find_all('a', href=True)

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                if 'trust' in text_lower and 'csv' in text_lower:
                    if href.startswith('/'):
                        href = f"https://digital.nhs.uk{href}"
                    print(f"Workforce: Found Trusts CSV: {link_text} -> {href}")
                    return href, link_text

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if href_lower.endswith('.csv') and ('trust' in text_lower or 'trust' in href_lower):
                    if href.startswith('/'):
                        href = f"https://digital.nhs.uk{href}"
                    print(f"Workforce: Found Trusts CSV (fallback): {link_text} -> {href}")
                    return href, link_text

            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                href_lower = href.lower()
                if href_lower.endswith('.csv') and 'workforce' in href_lower:
                    if href.startswith('/'):
                        href = f"https://digital.nhs.uk{href}"
                    print(f"Workforce: Found CSV (broad fallback): {link_text} -> {href}")
                    return href, link_text

            print("Workforce: No CSV download found on publication page")
            return None

        except Exception as e:
            print(f"Workforce: Error discovering link: {e}")
            self.audit_logger.log_operation('workforce', 'discover', False, {'error': str(e)})
            return None

    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest workforce data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('workforce', 'discover', False,
                                          {'error': 'No Workforce download link found'})
            return None

        download_url, description = link_info

        ext = "csv"
        if download_url.lower().endswith('.xlsx') or download_url.lower().endswith('.xls'):
            ext = "xlsx"
        filename = generate_filename("workforce_data", ext)
        save_path = os.path.join(self.raw_dir, filename)

        download_result = download_file(download_url, save_path, timeout=120)

        self.audit_logger.log_download(
            'workforce',
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

    def process_workforce_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process the workforce data file and filter by ODS codes."""
        try:
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                try:
                    df = pd.read_excel(file_path, engine='openpyxl')
                except Exception:
                    df = pd.read_excel(file_path)

            print(f"Workforce: Loaded file with {len(df)} rows, columns: {list(df.columns[:10])}")

            df = standardize_provider_column(df)

            filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)

            filtered_df['data_source'] = 'NHS Digital Workforce Statistics'
            filtered_df['processing_date'] = datetime.now().isoformat()

            print(f"Workforce: Filtered to {len(filtered_df)} rows for codes {self.ods_codes}")
            return filtered_df

        except Exception as e:
            print(f"Workforce: Error processing data: {e}")
            self.audit_logger.log_operation('workforce', 'process', False,
                                          {'error': str(e), 'file_path': file_path})
            return None

    def save_processed_data(self, df: pd.DataFrame) -> str:
        """Save processed workforce data to CSV."""
        filename = generate_filename("workforce_provider", "csv")
        output_path = os.path.join(self.processed_dir, filename)

        os.makedirs(self.processed_dir, exist_ok=True)

        df.to_csv(output_path, index=False)

        latest_path = os.path.join(self.processed_dir, "workforce_provider.csv")
        df.to_csv(latest_path, index=False)

        return output_path

    def fetch_and_process(self) -> Dict:
        """Complete workforce data fetch and processing pipeline."""
        results = {
            'dataset': 'workforce',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }

        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download Workforce data'
                return results

            results['download'] = download_result

            processed_df = self.process_workforce_data(download_result['file_path'])
            if processed_df is None or processed_df.empty:
                results['error'] = 'Failed to process Workforce data or no matching records'
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
                'workforce',
                download_result['file_path'],
                [output_path],
                True,
                len(processed_df)
            )

        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('workforce', 'fetch_and_process', False, {'error': str(e)})

        return results
