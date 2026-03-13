"""
ODS (Organisation Data Service) utility functions for Trust code resolution.
Interfaces with NHS England Digital's ORD API.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

class ODSResolver:
    """Handles ODS Trust code resolution and caching."""
    
    def __init__(self, cache_file: str = "data/processed/ods_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()
        self.base_url = "https://directory.spineservices.nhs.uk/ORD/2-0-0"
        
    def _load_cache(self) -> Dict:
        """Load cached ODS data if available and not expired."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    cache = json.load(f)
                    
                cached_time = datetime.fromisoformat(cache.get('timestamp', '1970-01-01'))
                if datetime.now() - cached_time < timedelta(hours=24):
                    all_known = all(
                        t.get('name', '').startswith('Unknown') is False 
                        for t in cache.get('trusts', {}).values()
                    )
                    has_unknown = any(
                        'Unknown' in t.get('name', '')
                        for t in cache.get('trusts', {}).values()
                    )
                    if not has_unknown:
                        logger.info("Using cached ODS data")
                        return cache
                    else:
                        logger.info("Cache has unknown trusts, will re-resolve")
                    
        except Exception as e:
            logger.warning(f"Failed to load ODS cache: {e}")
            
        return {'timestamp': datetime.now().isoformat(), 'trusts': {}}
    
    def _save_cache(self):
        """Save ODS cache to file."""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save ODS cache: {e}")
    
    def resolve_trust_codes(self, ods_codes: List[str]) -> Dict[str, Dict]:
        """
        Resolve Trust codes to names and metadata via ORD API.
        The ORD API returns JSON like: {"Organisation": {"Name": "...", ...}}
        """
        resolved = {}
        
        for code in ods_codes:
            if code in self.cache['trusts'] and 'Unknown' not in self.cache['trusts'][code].get('name', ''):
                resolved[code] = self.cache['trusts'][code]
                continue
                
            try:
                url = f"{self.base_url}/organisations/{code}"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    org = data.get('Organisation', {})
                    
                    name = org.get('Name', 'Unknown Trust')
                    status = org.get('Status', 'Unknown')
                    
                    primary_role = 'Unknown'
                    roles = org.get('Roles', {}).get('Role', [])
                    if isinstance(roles, list):
                        for role in roles:
                            if role.get('primaryRole', False):
                                primary_role = role.get('id', 'Unknown')
                                break
                    elif isinstance(roles, dict):
                        if roles.get('primaryRole', False):
                            primary_role = roles.get('id', 'Unknown')
                    
                    trust_info = {
                        'name': name,
                        'type': primary_role,
                        'status': status,
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    self.cache['trusts'][code] = trust_info
                    resolved[code] = trust_info
                    
                    logger.info(f"Resolved ODS code {code}: {name}")
                    print(f"ODS: Resolved {code} -> {name}")
                    
                else:
                    logger.warning(f"Failed to resolve ODS code {code}: HTTP {response.status_code}")
                    resolved[code] = {
                        'name': f'Unknown Trust ({code})',
                        'type': 'Unknown',
                        'status': 'Unknown',
                        'last_updated': datetime.now().isoformat()
                    }
                    
            except Exception as e:
                logger.error(f"Error resolving ODS code {code}: {e}")
                resolved[code] = {
                    'name': f'Unknown Trust ({code})',
                    'type': 'Unknown', 
                    'status': 'Unknown',
                    'last_updated': datetime.now().isoformat()
                }
        
        self.cache['timestamp'] = datetime.now().isoformat()
        self._save_cache()
        
        return resolved
    
    def get_trust_name(self, ods_code: str) -> str:
        """Get trust name for a single ODS code."""
        if ods_code in self.cache['trusts'] and 'Unknown' not in self.cache['trusts'][ods_code].get('name', ''):
            return self.cache['trusts'][ods_code]['name']
        
        resolved = self.resolve_trust_codes([ods_code])
        return resolved.get(ods_code, {}).get('name', f'Unknown Trust ({ods_code})')
    
    def filter_by_ods_codes(self, df: pd.DataFrame, ods_column: str, ods_codes: List[str]) -> pd.DataFrame:
        """
        Filter dataframe by ODS codes and add trust names.
        """
        if ods_column not in df.columns:
            ods_variations = [
                'Provider_Code', 'Org_Code', 'Organisation_Code', 'Trust_Code',
                'provider_code', 'org_code', 'organisation_code', 'trust_code',
                'Provider Code', 'Org Code', 'Organisation Code', 'Trust Code',
                'PROVIDER_CODE', 'ORG_CODE', 'ORGANISATION_CODE', 'TRUST_CODE',
                'Org code', 'Provider code'
            ]
            for var in ods_variations:
                if var in df.columns:
                    df = df.rename(columns={var: ods_column})
                    break
            else:
                raise ValueError(f"ODS column '{ods_column}' not found in dataframe. Columns: {list(df.columns[:20])}")
        
        df[ods_column] = df[ods_column].astype(str).str.strip().str.upper()
        upper_codes = [c.upper() for c in ods_codes]
        
        filtered_df = df[df[ods_column].isin(upper_codes)].copy()
        
        trust_mapping = self.resolve_trust_codes(ods_codes)
        filtered_df['trust_name'] = filtered_df[ods_column].apply(
            lambda x: trust_mapping.get(x, {}).get('name', f'Unknown Trust ({x})')
        )
        
        return filtered_df
