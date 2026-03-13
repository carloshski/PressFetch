"""
Cancer Waiting Times (CWT) data fetcher.
Fetches and processes NHS England Cancer waiting times statistics for 28-day, 31-day, and 62-day metrics.
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


class CancerFetcher:
    """Fetches and processes Cancer waiting times data from NHS England."""
    
    def __init__(self, ods_codes: List[str], data_dir: str = "data"):
        self.ods_codes = ods_codes
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, "raw")
        self.processed_dir = os.path.join(data_dir, "processed")
        
        self.audit_logger = AuditLogger()
        self.ods_resolver = ODSResolver()
        self.validator = DataValidator()
        
        self.base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/"
        
    def discover_latest_link(self) -> Optional[Tuple[str, str]]:
        """
        Discover the latest Cancer provider data extract link from NHS England.
        The cancer page has direct download links for provider extracts.
        We look for 'Data Extract (Provider)' XLSX files, or Combined CSV files.
        """
        try:
            print(f"Cancer: Fetching page {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            
            provider_extracts = []
            combined_csvs = []
            
            for link in links:
                href = link['href']
                link_text = link.get_text(strip=True)
                text_lower = link_text.lower()
                href_lower = href.lower()
                
                if 'data extract' in text_lower and 'provider' in text_lower:
                    if href_lower.endswith('.xlsx') or href_lower.endswith('.xls'):
                        if href.startswith('/'):
                            href = f"https://www.england.nhs.uk{href}"
                        provider_extracts.append((href, link_text))
                        print(f"Cancer: Found Provider Extract: {link_text}")
                
                if 'combined csv' in text_lower and href_lower.endswith('.csv'):
                    if href.startswith('/'):
                        href = f"https://www.england.nhs.uk{href}"
                    combined_csvs.append((href, link_text))
                    print(f"Cancer: Found Combined CSV: {link_text}")
            
            if provider_extracts:
                url, desc = provider_extracts[0]
                print(f"Cancer: Selected Provider Extract: {desc}")
                return url, desc
            
            if combined_csvs:
                url, desc = combined_csvs[0]
                print(f"Cancer: Selected Combined CSV: {desc}")
                return url, desc
            
            print("Cancer: No provider extract or combined CSV found")
            return None
            
        except Exception as e:
            print(f"Cancer: Error discovering link: {e}")
            self.audit_logger.log_operation('cancer', 'discover', False, {'error': str(e)})
            return None
    
    def download_latest_data(self) -> Optional[Dict]:
        """Download the latest Cancer waiting times data file."""
        link_info = self.discover_latest_link()
        if not link_info:
            self.audit_logger.log_operation('cancer', 'discover', False, 
                                          {'error': 'No Cancer download link found'})
            return None
        
        download_url, description = link_info
        
        if download_url.lower().endswith('.csv'):
            ext = "csv"
        else:
            ext = "xlsx"
        
        filename = generate_filename("cancer_data", ext)
        save_path = os.path.join(self.raw_dir, filename)
        
        download_result = download_file(download_url, save_path, timeout=120)
        
        self.audit_logger.log_download(
            'cancer',
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
    
    def process_cancer_data(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Process the Cancer data file and extract metrics filtered by ODS codes.
        Handles both Excel provider extract workbooks and Combined CSV files.
        """
        results = {}
        
        try:
            if file_path.lower().endswith('.csv'):
                return self._process_combined_csv(file_path)
            else:
                return self._process_provider_extract(file_path)
        except Exception as e:
            print(f"Cancer: Error processing data: {e}")
            self.audit_logger.log_operation('cancer', 'process', False, 
                                          {'error': str(e), 'file_path': file_path})
        
        return results
    
    def _process_combined_csv(self, csv_path: str) -> Dict[str, pd.DataFrame]:
        """Process a Combined CSV file containing all cancer waiting times data."""
        results = {}
        
        df = pd.read_csv(csv_path)
        print(f"Cancer CSV: Loaded {len(df)} rows, columns: {list(df.columns[:10])}")
        
        df = standardize_provider_column(df)
        
        filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)
        
        if filtered_df.empty:
            print("Cancer CSV: No matching records for ODS codes")
            return results
        
        standard_col = None
        for col in filtered_df.columns:
            if 'standard' in col.lower() or 'measure' in col.lower():
                standard_col = col
                break
        
        if standard_col:
            for standard_name in filtered_df[standard_col].unique():
                metric_key = self._classify_metric(str(standard_name))
                if metric_key:
                    metric_df = filtered_df[filtered_df[standard_col] == standard_name].copy()
                    metric_df['metric_type'] = metric_key
                    metric_df['data_source'] = 'NHS England Cancer Waiting Times'
                    metric_df['processing_date'] = datetime.now().isoformat()
                    
                    if metric_key in results:
                        results[metric_key] = pd.concat([results[metric_key], metric_df], ignore_index=True)
                    else:
                        results[metric_key] = metric_df
        else:
            filtered_df['metric_type'] = 'all'
            filtered_df['data_source'] = 'NHS England Cancer Waiting Times'
            filtered_df['processing_date'] = datetime.now().isoformat()
            results['all'] = filtered_df
        
        for key, metric_df in results.items():
            print(f"Cancer CSV: Metric '{key}' has {len(metric_df)} rows")
        
        return results
    
    def _process_provider_extract(self, excel_path: str) -> Dict[str, pd.DataFrame]:
        """Process a Provider Extract Excel workbook."""
        results = {}
        
        excel_file = pd.ExcelFile(excel_path)
        sheet_names = excel_file.sheet_names
        print(f"Cancer Excel: Sheets found: {sheet_names}")
        
        metric_keywords = {
            '28d': ['28', 'faster diagnosis', 'fd', 'fds'],
            '31d': ['31', 'first treatment', 'decision to treat'],
            '62d': ['62', 'urgent', 'screening', 'referral to treatment'],
        }
        
        for sheet_name in sheet_names:
            sheet_lower = sheet_name.lower().strip()
            
            if any(skip in sheet_lower for skip in ['note', 'info', 'content', 'read me', 'readme', 'index', 'cover']):
                continue
            
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                
                header_row = None
                for idx in range(min(20, len(df))):
                    row_vals = df.iloc[idx].astype(str).str.lower()
                    provider_matches = sum(1 for v in row_vals if any(k in v for k in ['provider', 'org', 'trust', 'code', 'standard', 'total', 'numerator', 'denominator']))
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
                    print(f"Cancer Excel: No header row found in sheet '{sheet_name}', skipping")
                    continue
                
                df.columns = df.iloc[header_row]
                df = df.iloc[header_row + 1:].reset_index(drop=True)
                df.columns = [str(c).strip() if pd.notna(c) else f'col_{i}' for i, c in enumerate(df.columns)]
                
                try:
                    df = standardize_provider_column(df)
                except ValueError:
                    print(f"Cancer Excel: No provider column in sheet '{sheet_name}', skipping")
                    continue
                
                filtered_df = self.ods_resolver.filter_by_ods_codes(df, 'provider_code', self.ods_codes)
                
                if filtered_df.empty:
                    continue
                
                metric_key = self._classify_metric(sheet_name)
                if not metric_key:
                    metric_key = sheet_name.strip()
                
                filtered_df['metric_type'] = metric_key
                filtered_df['data_source'] = 'NHS England Cancer Waiting Times'
                filtered_df['processing_date'] = datetime.now().isoformat()
                
                if metric_key in results:
                    results[metric_key] = pd.concat([results[metric_key], filtered_df], ignore_index=True)
                else:
                    results[metric_key] = filtered_df
                
                print(f"Cancer Excel: Sheet '{sheet_name}' -> metric '{metric_key}': {len(filtered_df)} rows")
                
            except Exception as e:
                print(f"Cancer Excel: Error processing sheet '{sheet_name}': {e}")
                continue
        
        excel_file.close()
        return results
    
    def _classify_metric(self, text: str) -> Optional[str]:
        """Classify a text string into a cancer metric category."""
        text_lower = text.lower()
        
        if '28' in text_lower or 'faster diagnosis' in text_lower or 'fds' in text_lower:
            return '28d'
        elif '31' in text_lower or 'first treatment' in text_lower or 'decision to treat' in text_lower:
            return '31d'
        elif '62' in text_lower or 'urgent' in text_lower and 'referral' in text_lower:
            return '62d'
        
        return None
    
    def validate_cancer_data(self, df: pd.DataFrame, metric: str) -> Dict:
        """Validate Cancer data quality."""
        validation_results = {}
        
        required_columns = ['provider_code', 'trust_name']
        schema_result = self.validator.validate_schema(df, required_columns)
        validation_results['schema'] = schema_result
        
        column_rules = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'percentage' in col_lower or '%' in col_lower:
                column_rules[col] = {'min': 0, 'max': 1, 'type': 'percentage'}
            elif 'numerator' in col_lower or 'denominator' in col_lower or 'total' in col_lower:
                column_rules[col] = {'min': 0, 'type': 'count'}
        
        if column_rules:
            range_result = self.validator.validate_ranges(df, column_rules)
            validation_results['ranges'] = range_result
        
        return validation_results
    
    def save_processed_data(self, metric_data: Dict[str, pd.DataFrame]) -> List[str]:
        """Save processed Cancer data to CSV files."""
        output_files = []
        
        os.makedirs(self.processed_dir, exist_ok=True)
        
        for metric, df in metric_data.items():
            safe_metric = re.sub(r'[^\w]', '_', metric).lower()
            
            filename = generate_filename(f"cwt_{safe_metric}", "csv")
            output_path = os.path.join(self.processed_dir, filename)
            df.to_csv(output_path, index=False)
            output_files.append(output_path)
            
            latest_path = os.path.join(self.processed_dir, f"cwt_{safe_metric}.csv")
            df.to_csv(latest_path, index=False)
        
        return output_files
    
    def fetch_and_process(self) -> Dict:
        """Complete Cancer waiting times data fetch and processing pipeline."""
        results = {
            'dataset': 'cancer',
            'success': False,
            'timestamp': datetime.now().isoformat(),
            'ods_codes': self.ods_codes
        }
        
        try:
            download_result = self.download_latest_data()
            if not download_result or not download_result['success']:
                results['error'] = 'Failed to download Cancer data'
                return results
            
            results['download'] = download_result
            
            metric_data = self.process_cancer_data(download_result['file_path'])
            if not metric_data:
                results['error'] = 'Failed to process Cancer data or no matching records'
                return results
            
            validation_results = {}
            for metric, df in metric_data.items():
                validation_results[metric] = self.validate_cancer_data(df, metric)
            
            results['validation'] = validation_results
            
            output_files = self.save_processed_data(metric_data)
            
            total_records = sum(len(df) for df in metric_data.values())
            all_providers = set()
            for df in metric_data.values():
                if 'provider_code' in df.columns:
                    all_providers.update(df['provider_code'].unique())
            
            results.update({
                'success': True,
                'output_files': output_files,
                'metrics_processed': list(metric_data.keys()),
                'total_record_count': total_records,
                'providers': list(all_providers)
            })
            
            self.audit_logger.log_processing(
                'cancer',
                download_result['file_path'],
                output_files,
                True,
                total_records,
                validation_results
            )
            
        except Exception as e:
            results['error'] = str(e)
            self.audit_logger.log_operation('cancer', 'fetch_and_process', False, {'error': str(e)})
        
        return results
