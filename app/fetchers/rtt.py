"""
RTT (Referral to Treatment) waiting times data fetcher.
Fetches and processes NHS England RTT statistics.
"""

import os
import re
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ..utils.audit import AuditLogger
from ..utils.io import download_file, extract_zip_file, generate_filename, standardize_provider_column
from ..utils.ods import ODSResolver


class RTTFetcher:
    """Fetches and processes RTT waiting times data from NHS England."""
    
    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")
        
        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        
        self.base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/"
        
    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest RTT data download link by navigating the two-level
        NHS England page structure: main page -> year sub-page -> download links.
        """
        try:
            print(f"RTT: Fetching main page {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            
            year_page_url = None
            for link in links:
                href = link['href']
                link_text = link.get_text(strip=True).lower()
                if re.search(r'rtt-data-\d{4}-\d{2}', href) or re.search(r'\d{4}-\d{2}\s+rtt\s+waiting\s+times\s+data', link_text):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    year_page_url = href
                    print(f"RTT: Found latest year page: {href}")
                    break
            
            if not year_page_url:
                print("RTT: No year sub-page found on main page")
                return None
            
            print(f"RTT: Fetching year sub-page {year_page_url}")
            response2 = requests.get(year_page_url, timeout=30)
            response2.raise_for_status()
            
            soup2 = BeautifulSoup(response2.content, 'html.parser')
            links2 = soup2.find_all('a', href=True)
            
            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True).lower()
                if 'full csv data file' in link_text and href.lower().endswith('.zip'):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"RTT: Found Full CSV ZIP: {href}")
                    return href, link.get_text(strip=True)
            
            for link in links2:
                href = link['href']
                if href.lower().endswith('.zip') and 'full-csv' in href.lower():
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"RTT: Found Full CSV ZIP by URL pattern: {href}")
                    return href, "Full CSV data file"
            
            print("RTT: No Full CSV ZIP found on year sub-page")
            return None
            
        except Exception as e:
            print(f"RTT: Error discovering link: {e}")
            self.audit_logger.log_operation('rtt', 'discover', False, {'error': str(e)})
            return None
    
    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest RTT data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('rtt', 'discover', False, 
                                          {'error': 'No RTT download link found'})
            return None
        
        download_url, description = link_info
        
        filename = generate_filename("rtt_data", "zip")
        save_path = os.path.join(self.raw_dir, filename)
        
        download_result = download_file(download_url, save_path, timeout=120)
        
        self.audit_logger.log_download(
            'rtt',
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
    
    def extract_provider_csv(self, zip_path: str) -> Optional[str]:
        """Extract the provider-level CSV file from the downloaded ZIP."""
        try:
            extract_dir = os.path.join(self.raw_dir, "extracted")
            
            extracted_files = extract_zip_file(zip_path, extract_dir, "csv")
            
            provider_csv = None
            for file_path in extracted_files:
                filename = os.path.basename(file_path).lower()
                if any(keyword in filename for keyword in ['provider', 'trust', 'organisation']):
                    provider_csv = file_path
                    break
            
            if not provider_csv and extracted_files:
                csv_files = [f for f in extracted_files if f.endswith('.csv')]
                if csv_files:
                    largest = max(csv_files, key=lambda f: os.path.getsize(f))
                    provider_csv = largest
                    print(f"RTT: Using largest CSV: {os.path.basename(provider_csv)} ({os.path.getsize(provider_csv)} bytes)")
            
            return provider_csv
            
        except Exception as e:
            self.audit_logger.log_operation('rtt', 'extract', False, {'error': str(e), 'zip_path': zip_path})
            return None
    
    def process_rtt_data(self, csv_path: str) -> Optional[pd.DataFrame]:
        """Process the RTT CSV data and filter by ODS codes."""
        try:
            df = pd.read_csv(csv_path)
            print(f"RTT: Loaded CSV with {len(df)} rows, columns: {list(df.columns[:10])}")
            
            df = standardize_provider_column(df)
            
            filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)
            
            filtered_df['data_source'] = 'NHS England RTT Statistics'
            filtered_df['processing_date'] = datetime.now().isoformat()
            
            print(f"RTT: Filtered to {len(filtered_df)} rows for codes {self.ods_codes}")
            return filtered_df
            
        except Exception as e:
            print(f"RTT: Error processing data: {e}")
            self.audit_logger.log_operation('rtt', 'process', False, 
                                          {'error': str(e), 'csv_path': csv_path})
            return None
    
    def save_processed_data(self, df: pd.DataFrame) -> str:
        """Save processed RTT data to CSV."""
        filename = generate_filename("rtt_provider", "csv")
        output_path = os.path.join(self.processed_dir, filename)
        
        os.makedirs(self.processed_dir, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        
        latest_path = os.path.join(self.processed_dir, "rtt_provider.csv")
        df.to_csv(latest_path, index=False)
        
        return output_path
    
    def fetch_and_process(self) -> Dict:
        """Complete RTT data fetch and processing pipeline."""
        results = {
            'dataset': 'rtt',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }
        
        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download RTT data'
                return results
            
            results['download'] = download_result
            
            csv_path = self.extract_provider_csv(download_result['file_path'])
            if not csv_path:
                results['error'] = 'Failed to extract provider CSV from ZIP'
                return results
            
            results['csv_path'] = csv_path
            
            processed_df = self.process_rtt_data(csv_path)
            if processed_df is None or processed_df.empty:
                results['error'] = 'Failed to process RTT data or no matching records'
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
                'rtt',
                csv_path,
                [output_path],
                True,
                len(processed_df)
            )
            
        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('rtt', 'fetch_and_process', False, {'error': str(e)})
        
        return results
