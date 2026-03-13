"""
Ambulance Quality Indicators data fetcher.
Fetches and processes NHS England ambulance response times statistics.
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


class AmbulanceFetcher:
    """Fetches and processes ambulance quality indicators data from NHS England."""

    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")

        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        self.validator = DataValidator()

        self.base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/ambulance-quality-indicators/"

    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest ambulance CSV download link from the main page.
        Looks for AmbSYS CSV files directly on the page.
        """
        try:
            print(f"Ambulance: Fetching main page {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)

            for link in links:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if 'ambsys' in text_lower and href_lower.endswith('.csv'):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"Ambulance: Found AmbSYS CSV: {link_text} -> {href}")
                    return href, link_text

            for link in links:
                href = link['href']
                link_text = link.get_text(strip=True)
                href_lower = href.lower()
                if 'ambsys' in href_lower and href_lower.endswith('.csv'):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"Ambulance: Found AmbSYS CSV by URL: {link_text} -> {href}")
                    return href, link_text

            for link in links:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if href_lower.endswith('.csv') and ('ambulance' in text_lower or 'ambulance' in href_lower):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"Ambulance: Found CSV (fallback): {link_text} -> {href}")
                    return href, link_text

            print("Ambulance: No AmbSYS CSV download found on page")
            return None

        except Exception as e:
            print(f"Ambulance: Error discovering link: {e}")
            self.audit_logger.log_operation('ambulance', 'discover', False, {'error': str(e)})
            return None

    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest ambulance data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('ambulance', 'discover', False,
                                          {'error': 'No Ambulance download link found'})
            return None

        download_url, description = link_info

        filename = generate_filename("ambulance_data", "csv")
        save_path = os.path.join(self.raw_dir, filename)

        download_result = download_file(download_url, save_path, timeout=60)

        self.audit_logger.log_download(
            'ambulance',
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

    def process_ambulance_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process the ambulance data CSV and filter by ODS codes."""
        try:
            df = pd.read_csv(file_path)
            print(f"Ambulance: Loaded CSV with {len(df)} rows, columns: {list(df.columns[:10])}")

            df = standardize_provider_column(df)

            filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)

            filtered_df['data_source'] = 'NHS England Ambulance Quality Indicators'
            filtered_df['processing_date'] = datetime.now().isoformat()

            print(f"Ambulance: Filtered to {len(filtered_df)} rows for codes {self.ods_codes}")
            return filtered_df

        except Exception as e:
            print(f"Ambulance: Error processing data: {e}")
            self.audit_logger.log_operation('ambulance', 'process', False,
                                          {'error': str(e), 'file_path': file_path})
            return None

    def save_processed_data(self, df: pd.DataFrame) -> str:
        """Save processed ambulance data to CSV."""
        filename = generate_filename("ambulance_provider", "csv")
        output_path = os.path.join(self.processed_dir, filename)

        os.makedirs(self.processed_dir, exist_ok=True)

        df.to_csv(output_path, index=False)

        latest_path = os.path.join(self.processed_dir, "ambulance_provider.csv")
        df.to_csv(latest_path, index=False)

        return output_path

    def fetch_and_process(self) -> Dict:
        """Complete ambulance data fetch and processing pipeline."""
        results = {
            'dataset': 'ambulance',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }

        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download Ambulance data'
                return results

            results['download'] = download_result

            processed_df = self.process_ambulance_data(download_result['file_path'])
            if processed_df is None or processed_df.empty:
                results['error'] = 'Failed to process Ambulance data or no matching records'
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
                'ambulance',
                download_result['file_path'],
                [output_path],
                True,
                len(processed_df)
            )

        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('ambulance', 'fetch_and_process', False, {'error': str(e)})

        return results
