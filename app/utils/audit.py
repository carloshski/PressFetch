"""
Audit utility functions for logging, validation, and monitoring.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yaml

from .io import ensure_directory, load_yaml, calculate_file_hash


class AuditLogger:
    """Handles audit logging for data processing operations."""
    
    def __init__(self, audit_file: str = "data/processed/audit.jsonl"):
        self.audit_file = audit_file
        ensure_directory(os.path.dirname(audit_file))
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def log_operation(self, 
                     dataset_name: str,
                     operation: str,
                     success: bool,
                     details: Dict[str, Any]) -> None:
        """
        Log an audit entry for a data operation.
        
        Args:
            dataset_name: Name of the dataset (e.g., 'rtt', 'ae', 'cancer')
            operation: Operation type (e.g., 'fetch', 'process', 'validate')
            success: Whether operation succeeded
            details: Additional operation details
        """
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'dataset': dataset_name,
            'operation': operation,
            'success': success,
            **details
        }
        
        try:
            with open(self.audit_file, 'a') as f:
                f.write(json.dumps(audit_entry, default=str) + '\n')
                
            log_level = logging.INFO if success else logging.ERROR
            self.logger.log(log_level, 
                          f"{dataset_name} {operation}: {'SUCCESS' if success else 'FAILED'}")
            
        except Exception as e:
            self.logger.error(f"Failed to write audit log: {e}")
    
    def log_download(self, 
                    dataset_name: str,
                    url: str,
                    file_path: str,
                    success: bool,
                    file_hash: str = None,
                    size_bytes: int = None,
                    error: str = None) -> None:
        """Log a file download operation."""
        details = {
            'url': url,
            'file_path': file_path,
            'file_hash': file_hash,
            'size_bytes': size_bytes
        }
        
        if error:
            details['error'] = error
            
        self.log_operation(dataset_name, 'download', success, details)
    
    def log_processing(self,
                      dataset_name: str,
                      input_file: str,
                      output_files: List[str],
                      success: bool,
                      record_count: int = None,
                      validation_results: Dict = None,
                      error: str = None) -> None:
        """Log a data processing operation."""
        details = {
            'input_file': input_file,
            'output_files': output_files,
            'record_count': record_count
        }
        
        if validation_results:
            details['validation'] = validation_results
            
        if error:
            details['error'] = error
            
        self.log_operation(dataset_name, 'process', success, details)
    
    def get_recent_operations(self, hours: int = 24) -> List[Dict]:
        """Get recent audit entries within specified hours."""
        if not os.path.exists(self.audit_file):
            return []
        
        cutoff_time = datetime.now().timestamp() - (hours * 3600)
        recent_entries = []
        
        try:
            with open(self.audit_file, 'r') as f:
                for line in f:
                    entry = json.loads(line.strip())
                    entry_time = datetime.fromisoformat(entry['timestamp']).timestamp()
                    
                    if entry_time >= cutoff_time:
                        recent_entries.append(entry)
                        
        except Exception as e:
            self.logger.error(f"Failed to read audit log: {e}")
        
        return recent_entries


class DataValidator:
    """Handles data validation and quality checks."""
    
    def __init__(self, config_file: str = "app/config/thresholds.yaml"):
        self.config = load_yaml(config_file)
        self.logger = logging.getLogger(__name__)
    
    def validate_schema(self, df: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """
        Validate DataFrame schema against required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            Validation results dictionary
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        extra_columns = [col for col in df.columns if col not in required_columns]
        
        is_valid = len(missing_columns) == 0
        
        result = {
            'valid': is_valid,
            'missing_columns': missing_columns,
            'extra_columns': extra_columns,
            'total_columns': len(df.columns),
            'total_rows': len(df)
        }
        
        if not is_valid:
            self.logger.error(f"Schema validation failed: missing columns {missing_columns}")
        
        return result
    
    def validate_ranges(self, df: pd.DataFrame, column_rules: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Validate numeric ranges for specified columns.
        
        Args:
            df: DataFrame to validate
            column_rules: Dictionary mapping column names to validation rules
                         e.g., {'percentage_col': {'min': 0, 'max': 1, 'type': 'percentage'}}
            
        Returns:
            Validation results dictionary
        """
        validation_config = self.config.get('validation', {})
        issues = []
        
        for column, rules in column_rules.items():
            if column not in df.columns:
                continue
                
            col_data = pd.to_numeric(df[column], errors='coerce')
            
            # Check for non-numeric values
            non_numeric = df[column].isna().sum()
            if non_numeric > 0:
                issues.append(f"{column}: {non_numeric} non-numeric values")
            
            # Range validation
            if 'min' in rules:
                min_violations = (col_data < rules['min']).sum()
                if min_violations > 0:
                    issues.append(f"{column}: {min_violations} values below minimum {rules['min']}")
            
            if 'max' in rules:
                max_violations = (col_data > rules['max']).sum()
                if max_violations > 0:
                    issues.append(f"{column}: {max_violations} values above maximum {rules['max']}")
            
            # Type-specific validation
            if rules.get('type') == 'percentage':
                # Check percentage range
                pct_min = validation_config.get('percentage_min', 0)
                pct_max = validation_config.get('percentage_max', 1)
                
                out_of_range = ((col_data < pct_min) | (col_data > pct_max)).sum()
                if out_of_range > 0:
                    issues.append(f"{column}: {out_of_range} percentage values out of range [{pct_min}, {pct_max}]")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'total_checks': len(column_rules)
        }
    
    def validate_rtt_totals(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate RTT data for consistency (total should equal sum of bands).
        
        Args:
            df: RTT DataFrame
            
        Returns:
            Validation results
        """
        issues = []
        
        # Look for total and band columns
        total_col = None
        band_cols = []
        
        for col in df.columns:
            col_lower = col.lower()
            if 'total' in col_lower and 'incomplete' in col_lower:
                total_col = col
            elif any(band in col_lower for band in ['0-1', '1-2', '2-4', '4-13', '13-26', '26-52', '52+']):
                band_cols.append(col)
        
        if total_col and band_cols:
            for idx, row in df.iterrows():
                total_value = pd.to_numeric(row[total_col], errors='coerce')
                band_sum = pd.to_numeric(row[band_cols], errors='coerce').sum()
                
                if not pd.isna(total_value) and not pd.isna(band_sum):
                    if abs(total_value - band_sum) > 0.01:  # Allow small rounding differences
                        provider = row.get('provider_code', idx)
                        issues.append(f"Provider {provider}: total ({total_value}) != band sum ({band_sum})")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'total_column': total_col,
            'band_columns': band_cols
        }
    
    def check_month_over_month_changes(self, 
                                     current_df: pd.DataFrame,
                                     previous_df: pd.DataFrame,
                                     value_columns: List[str],
                                     key_column: str = 'provider_code') -> Dict[str, Any]:
        """
        Check for extreme month-over-month changes.
        
        Args:
            current_df: Current month's data
            previous_df: Previous month's data
            value_columns: Columns to check for changes
            key_column: Column to join on
            
        Returns:
            Change analysis results
        """
        validation_config = self.config.get('validation', {})
        extreme_threshold = validation_config.get('extreme_change_threshold', 0.3)
        
        extreme_changes = []
        
        if previous_df is None or previous_df.empty:
            return {'valid': True, 'extreme_changes': [], 'note': 'No previous data for comparison'}
        
        # Merge datasets
        merged = current_df.merge(
            previous_df, 
            on=key_column, 
            suffixes=('_current', '_previous'),
            how='inner'
        )
        
        for col in value_columns:
            current_col = f"{col}_current"
            previous_col = f"{col}_previous"
            
            if current_col in merged.columns and previous_col in merged.columns:
                # Calculate percentage change
                current_vals = pd.to_numeric(merged[current_col], errors='coerce')
                previous_vals = pd.to_numeric(merged[previous_col], errors='coerce')
                
                # Avoid division by zero
                mask = (previous_vals != 0) & (~current_vals.isna()) & (~previous_vals.isna())
                pct_change = ((current_vals - previous_vals) / previous_vals).abs()
                
                extreme_mask = mask & (pct_change > extreme_threshold)
                
                for idx in merged[extreme_mask].index:
                    row = merged.loc[idx]
                    provider = row[key_column]
                    change_pct = pct_change.loc[idx] * 100
                    
                    extreme_changes.append({
                        'provider': provider,
                        'column': col,
                        'previous_value': previous_vals.loc[idx],
                        'current_value': current_vals.loc[idx],
                        'change_percent': change_pct
                    })
        
        return {
            'valid': len(extreme_changes) == 0,
            'extreme_changes': extreme_changes,
            'threshold_percent': extreme_threshold * 100
        }