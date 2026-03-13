"""
I/O utility functions for file operations and data handling.
"""

import hashlib
import json
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd
import requests
import yaml


def ensure_directory(directory: str) -> str:
    """Ensure directory exists and return the path."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    return directory


def generate_filename(prefix: str, extension: str, timestamp: bool = True) -> str:
    """Generate a filename with optional timestamp."""
    if timestamp:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{prefix}_{ts}.{extension}"
    return f"{prefix}.{extension}"


def download_file(url: str, save_path: str, timeout: int = 30) -> Dict:
    """
    Download a file from URL and save locally.
    
    Args:
        url: URL to download from
        save_path: Local path to save file
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary with download metadata
    """
    try:
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()
        
        # Ensure directory exists
        ensure_directory(os.path.dirname(save_path))
        
        # Download with progress tracking
        total_size = 0
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
        
        # Calculate file hash
        file_hash = calculate_file_hash(save_path)
        
        return {
            'success': True,
            'file_path': save_path,
            'size_bytes': total_size,
            'hash_sha256': file_hash,
            'download_time': datetime.now().isoformat(),
            'source_url': url
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'file_path': save_path,
            'download_time': datetime.now().isoformat(),
            'source_url': url
        }


def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
            
    return sha256_hash.hexdigest()


def extract_zip_file(zip_path: str, extract_to: str, pattern: str = None) -> List[str]:
    """
    Extract files from ZIP archive.
    
    Args:
        zip_path: Path to ZIP file
        extract_to: Directory to extract to
        pattern: Optional filename pattern to match
        
    Returns:
        List of extracted file paths
    """
    extracted_files = []
    
    ensure_directory(extract_to)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if pattern is None or pattern.lower() in file_info.filename.lower():
                extracted_path = zip_ref.extract(file_info, extract_to)
                extracted_files.append(extracted_path)
    
    return extracted_files


def save_json(data: Dict, file_path: str, indent: int = 2) -> bool:
    """Save data as JSON file."""
    try:
        ensure_directory(os.path.dirname(file_path))
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=indent, default=str)
        return True
    except Exception as e:
        print(f"Error saving JSON to {file_path}: {e}")
        return False


def load_json(file_path: str) -> Dict:
    """Load JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON from {file_path}: {e}")
        return {}


def save_yaml(data: Dict, file_path: str) -> bool:
    """Save data as YAML file."""
    try:
        ensure_directory(os.path.dirname(file_path))
        with open(file_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        return True
    except Exception as e:
        print(f"Error saving YAML to {file_path}: {e}")
        return False


def load_yaml(file_path: str) -> Dict:
    """Load YAML file."""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading YAML from {file_path}: {e}")
        return {}


def save_csv(df: pd.DataFrame, file_path: str, index: bool = False) -> bool:
    """Save DataFrame as CSV file."""
    try:
        ensure_directory(os.path.dirname(file_path))
        df.to_csv(file_path, index=index)
        return True
    except Exception as e:
        print(f"Error saving CSV to {file_path}: {e}")
        return False


def standardize_provider_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize provider code column names across different datasets.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized 'provider_code' column
    """
    provider_columns = [
        'Provider_Code', 'Org_Code', 'Organisation_Code', 'Trust_Code',
        'provider_code', 'org_code', 'organisation_code', 'trust_code',
        'Provider Code', 'Org Code', 'Organisation Code', 'Trust Code',
        'PROVIDER_CODE', 'ORG_CODE', 'ORGANISATION_CODE', 'TRUST_CODE',
        'Provider Org Code', 'Provider_Org_Code',
        'Org code', 'Provider code', 'Code',
    ]
    
    df_copy = df.copy()
    
    for col in provider_columns:
        if col in df_copy.columns:
            df_copy = df_copy.rename(columns={col: 'provider_code'})
            break
    
    if 'provider_code' not in df_copy.columns:
        for col in df_copy.columns:
            col_lower = col.lower().replace(' ', '_').replace('-', '_')
            if 'provider' in col_lower and ('code' in col_lower or 'org' in col_lower):
                df_copy = df_copy.rename(columns={col: 'provider_code'})
                break
            elif col_lower in ('org_code', 'organisation_code', 'trust_code', 'code'):
                df_copy = df_copy.rename(columns={col: 'provider_code'})
                break
    
    if 'provider_code' not in df_copy.columns:
        raise ValueError(f"No provider code column found in dataset. Columns: {list(df_copy.columns[:20])}")
    
    return df_copy


def clean_percentage_columns(df: pd.DataFrame, percentage_columns: List[str]) -> pd.DataFrame:
    """
    Clean and validate percentage columns.
    
    Args:
        df: Input DataFrame
        percentage_columns: List of column names containing percentages
        
    Returns:
        DataFrame with cleaned percentage columns
    """
    df_copy = df.copy()
    
    for col in percentage_columns:
        if col in df_copy.columns:
            # Convert to numeric, handling various formats
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
            
            # If values are > 1, assume they're in percentage format (0-100)
            mask = df_copy[col] > 1
            df_copy.loc[mask, col] = df_copy.loc[mask, col] / 100
            
            # Clamp values to valid range [0, 1]
            df_copy[col] = df_copy[col].clip(0, 1)
    
    return df_copy


def get_file_age_days(file_path: str) -> int:
    """Get age of file in days."""
    if not os.path.exists(file_path):
        return float('inf')
    
    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    return (datetime.now() - file_time).days


def cleanup_old_files(directory: str, max_age_days: int, pattern: str = "*") -> int:
    """
    Clean up old files in directory.
    
    Args:
        directory: Directory to clean
        max_age_days: Maximum file age in days
        pattern: File pattern to match
        
    Returns:
        Number of files deleted
    """
    deleted_count = 0
    
    if not os.path.exists(directory):
        return deleted_count
    
    for file_path in Path(directory).glob(pattern):
        if file_path.is_file() and get_file_age_days(str(file_path)) > max_age_days:
            try:
                file_path.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
    
    return deleted_count