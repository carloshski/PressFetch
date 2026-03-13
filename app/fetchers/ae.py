"""
A&E (Accident & Emergency) attendances and emergency admissions data fetcher.
Fetches and processes NHS England A&E statistics.
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


class AEFetcher:
    """Fetches and processes A&E attendances data from NHS England."""
    
    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")
        
        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        self.validator = DataValidator()
        
        self.base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/"
        
    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest A&E CSV download link by navigating the two-level
        NHS England page structure: main page -> year sub-page -> download links.
        """
        try:
            print(f"A&E: Fetching main page {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            
            year_page_url = None
            for link in links:
                href = link['href']
                link_text = link.get_text(strip=True).lower()
                if re.search(r'ae-attendances-and-emergency-admissions-\d{4}-\d{2}', href):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    year_page_url = href
                    print(f"A&E: Found latest year page: {href}")
                    break
            
            if not year_page_url:
                print("A&E: No year sub-page found on main page")
                return None
            
            print(f"A&E: Fetching year sub-page {year_page_url}")
            response2 = requests.get(year_page_url, timeout=30)
            response2.raise_for_status()
            
            soup2 = BeautifulSoup(response2.content, 'html.parser')
            links2 = soup2.find_all('a', href=True)
            
            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if href_lower.endswith('.csv') and ('csv' in text_lower or 'a&e' in text_lower or 'ae' in text_lower):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"A&E: Found CSV download: {link_text} -> {href}")
                    return href, link_text
            
            for link in links2:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                if (href_lower.endswith('.xls') or href_lower.endswith('.xlsx')) and ('ae' in text_lower or 'a&e' in text_lower) and 'provider' in href_lower:
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    print(f"A&E: Found XLS download: {href}")
                    return href, link_text
            
            print("A&E: No CSV or XLS download found on year sub-page")
            return None
            
        except Exception as e:
            print(f"A&E: Error discovering link: {e}")
            self.audit_logger.log_operation('ae', 'discover', False, {'error': str(e)})
            return None
    
    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest A&E data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('ae', 'discover', False, 
                                          {'error': 'No A&E download link found'})
            return None
        
        download_url, description = link_info
        
        ext = "csv" if download_url.lower().endswith('.csv') else "xls"
        filename = generate_filename("ae_data", ext)
        save_path = os.path.join(self.raw_dir, filename)
        
        download_result = download_file(download_url, save_path, timeout=60)
        
        self.audit_logger.log_download(
            'ae',
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
    
    def process_ae_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Process the A&E data file and filter by ODS codes."""
        try:
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            print(f"A&E: Loaded file with {len(df)} rows, columns: {list(df.columns[:10])}")
            
            df = standardize_provider_column(df)
            
            filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)
            
            column_mapping = {}
            for col in filtered_df.columns:
                col_lower = col.lower().replace(' ', '_').replace('-', '_')
                if 'attendances' in col_lower and 'total' in col_lower:
                    column_mapping[col] = 'total_attendances'
                elif '4' in col_lower and ('hour' in col_lower or 'hrs' in col_lower) and ('percent' in col_lower or '%' in col_lower or 'perf' in col_lower):
                    column_mapping[col] = 'pct_4_hours'
                elif 'breach' in col_lower and '12' in col_lower:
                    column_mapping[col] = 'breaches_12_hour'
                elif 'emergency' in col_lower and 'admission' in col_lower:
                    column_mapping[col] = 'emergency_admissions'
            
            if column_mapping:
                filtered_df = filtered_df.rename(columns=column_mapping)
            
            if 'pct_4_hours' in filtered_df.columns:
                filtered_df['pct_4_hours'] = pd.to_numeric(filtered_df['pct_4_hours'], errors='coerce')
                mask = filtered_df['pct_4_hours'] > 1
                filtered_df.loc[mask, 'pct_4_hours'] = filtered_df.loc[mask, 'pct_4_hours'] / 100
            
            filtered_df['data_source'] = 'NHS England A&E Statistics'
            filtered_df['processing_date'] = datetime.now().isoformat()
            
            print(f"A&E: Filtered to {len(filtered_df)} rows for codes {self.ods_codes}")
            return filtered_df
            
        except Exception as e:
            print(f"A&E: Error processing data: {e}")
            self.audit_logger.log_operation('ae', 'process', False, 
                                          {'error': str(e), 'file_path': file_path})
            return None
    
    def validate_ae_data(self, df: pd.DataFrame) -> Dict:
        """Validate A&E data quality."""
        validation_results = {}
        
        required_columns = ['provider_code', 'trust_name']
        schema_result = self.validator.validate_schema(df, required_columns)
        validation_results['schema'] = schema_result
        
        column_rules = {}
        if 'pct_4_hours' in df.columns:
            column_rules['pct_4_hours'] = {'min': 0, 'max': 1, 'type': 'percentage'}
        if 'total_attendances' in df.columns:
            column_rules['total_attendances'] = {'min': 0, 'type': 'count'}
        if 'breaches_12_hour' in df.columns:
            column_rules['breaches_12_hour'] = {'min': 0, 'type': 'count'}
        
        if column_rules:
            range_result = self.validator.validate_ranges(df, column_rules)
            validation_results['ranges'] = range_result
        
        return validation_results
    
    def save_processed_data(self, df: pd.DataFrame) -> str:
        """Save processed A&E data to CSV."""
        filename = generate_filename("ae_provider", "csv")
        output_path = os.path.join(self.processed_dir, filename)
        
        os.makedirs(self.processed_dir, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        
        latest_path = os.path.join(self.processed_dir, "ae_provider.csv")
        df.to_csv(latest_path, index=False)
        
        return output_path
    
    def fetch_and_process(self) -> Dict:
        """Complete A&E data fetch and processing pipeline."""
        results = {
            'dataset': 'ae',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }
        
        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download A&E data'
                return results
            
            results['download'] = download_result
            
            processed_df = self.process_ae_data(download_result['file_path'])
            if processed_df is None or processed_df.empty:
                results['error'] = 'Failed to process A&E data or no matching records'
                return results
            
            validation_results = self.validate_ae_data(processed_df)
            results['validation'] = validation_results
            
            output_path = self.save_processed_data(processed_df)
            
            results.update({
                'success': True,
                'output_file': output_path,
                'record_count': len(processed_df),
                'providers': processed_df['provider_code'].unique().tolist(),
                'columns': processed_df.columns.tolist()
            })
            
            self.audit_logger.log_processing(
                'ae',
                download_result['file_path'],
                [output_path],
                True,
                len(processed_df),
                validation_results
            )
            
        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('ae', 'fetch_and_process', False, {'error': str(e)})
        
        return results
