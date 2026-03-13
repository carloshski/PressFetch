# NHS England Data Explorer

## Overview

This is a FastAPI-based data explorer that fetches and processes NHS England healthcare statistics for any Trust. The system supports eight datasets: Referral to Treatment (RTT), A&E attendances, Cancer Waiting Times (CWT), Diagnostics (DM01), Ambulance Response Times (AmbSYS), NHS Workforce Statistics, Community Services, and Maternity Services. Users search for any NHS Trust via the ODS API, select one or more, then fetch data. No default trusts are hardcoded — the tool is fully generic. The system includes comprehensive audit logging, data validation, and caching mechanisms for the Organisation Data Service (ODS) lookups.

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes

- **2026-02-19**: Added four dashboard enhancement features
  - Trust Comparison View: New "Compare" tab with side-by-side metrics tables and multi-trust overlay Chart.js line charts
  - Date Range Filtering: From/To text inputs on each dataset tab for filtering table rows and CSV downloads by period
  - Saved/Favourite Trusts: localStorage-based save/load/delete of trust group selections via dropdown
  - Data Freshness Indicators: /api/freshness endpoint returns file modification timestamps; Summary tab shows freshness bar with green/stale dots; each dataset tab shows last-updated tag with stale warnings (>7 days)
  - New API endpoints: /api/freshness, /api/comparison
  - Updated /api/download to accept date_from/date_to query params
  - Added find_period_column(), get_primary_value_col(), get_primary_value_label() helper functions
- **2026-02-18**: Added 5 new data sources (Diagnostics, Ambulance, Workforce, Community, Maternity)
  - New fetchers: DiagnosticsFetcher, AmbulanceFetcher, WorkforceFetcher, CommunityFetcher, MaternityFetcher
  - ALL_FETCHERS dictionary maps dataset names to fetcher classes for dynamic pipeline execution
  - Metrics computation for all 8 datasets in compute_dataset_metrics()
  - Dashboard now has 9 tabs: Summary + 8 dataset tabs
  - Generic renderDatasetTab() function handles all datasets with specific metric cards for each
  - Row limit of 500 in table display with download CSV prompt for full data
- **2026-02-18**: Genericised app — removed hardcoded Norfolk defaults, renamed to "NHS England Data Explorer"
  - No default trusts; users search and select any Trust via ODS API
  - All pipeline runs use /api/run-custom with user-selected trusts
  - Moved from inline HTML to Jinja2 templates (templates/dashboard.html, static/css/dashboard.css, static/js/dashboard.js)
  - Added API endpoints: /api/trusts/search, /api/data/{dataset}, /api/download/{dataset}, /api/summary, /api/run-custom
  - Tabbed data views with metric cards and data tables
  - CSV download for filtered datasets

## System Architecture

### Core Application Framework
- **FastAPI Backend**: RESTful API server providing endpoints for data pipeline operations and interactive dashboard
- **Jinja2 Templates**: Dashboard UI served from templates/ with separate CSS/JS in static/
- **Modular Fetcher Pattern**: Separate fetcher classes for each of 8 dataset types following a consistent interface
- **ALL_FETCHERS Map**: Dictionary mapping dataset names to fetcher classes for dynamic pipeline execution
- **Utility Layer**: Shared utilities for I/O operations, audit logging, ODS resolution, and data validation

### Data Fetchers (app/fetchers/)
- **rtt.py** - RTTFetcher: Referral to Treatment waiting times from NHS England
- **ae.py** - AEFetcher: A&E attendances and emergency admissions from NHS England
- **cancer.py** - CancerFetcher: Cancer waiting times from NHS England
- **diagnostics.py** - DiagnosticsFetcher: DM01 diagnostics waiting times from NHS England
- **ambulance.py** - AmbulanceFetcher: AmbSYS ambulance response times from NHS England
- **workforce.py** - WorkforceFetcher: NHS workforce statistics from NHS Digital
- **community.py** - CommunityFetcher: Community services statistics from NHS Digital
- **maternity.py** - MaternityFetcher: Maternity services data from NHS Digital

### Data Processing Architecture
- **Web Scraping Discovery**: Automated detection of latest data files from NHS England/Digital statistics pages using BeautifulSoup
- **File Processing Pipeline**: Downloads raw files with timestamps, processes and standardizes data formats, filters by ODS codes
- **Data Storage Structure**: 
  - `/data/raw/`: Date-stamped downloads from NHS sources
  - `/data/processed/`: Cleaned outputs ready for dashboard consumption (8 *_provider.csv files)
  - `/data/mhs/`: Manual uploads from Model Health System

### Configuration Management
- **Environment-based ODS Codes**: Trust codes specified via `GROUP_ODS_LIST` environment variable
- **YAML Configuration**: Threshold settings stored in `app/config/thresholds.yaml`
- **Timezone Support**: London timezone configuration via environment variables

### Audit and Monitoring
- **Comprehensive Audit Logging**: JSONL format audit trail for all operations with timestamps and success indicators
- **Data Validation**: Built-in validators for data quality checks
- **File Management**: Automatic cleanup of old files to manage storage

### ODS Integration
- **NHS England Digital ORD API**: Integration for Trust code and name lookups
- **Intelligent Caching**: 24-hour cache for ODS data to reduce API calls and improve performance
- **Fallback Handling**: Graceful degradation when ODS services are unavailable

## External Dependencies

### NHS England Data Sources
- **RTT Statistics**: Main page → Year sub-page → "Full CSV data file" ZIP downloads
- **A&E Statistics**: Main page → Year sub-page → Monthly CSV/XLS files by provider
- **Cancer Waiting Times**: Direct links on main page → Provider XLSX/CSV files
- **Diagnostics (DM01)**: Main page → Year sub-page → Provider XLS downloads
- **Ambulance (AmbSYS)**: Main page → CSV download with all ambulance trust data
- **Workforce**: NHS Digital → Monthly publication pages → Trust-level CSV/Excel
- **Community Services**: NHS Digital → Publication pages → Provider CSV/Excel
- **Maternity Services**: NHS Digital → Monthly publication pages → Provider CSV/Excel
- **Organisation Data Service (ODS)**: NHS England Digital ORD API for Trust code resolution

### Python Dependencies
- **FastAPI + Uvicorn**: Web framework and ASGI server
- **Pandas**: Data processing and manipulation
- **Requests + BeautifulSoup**: Web scraping and HTTP operations
- **PyYAML**: Configuration file parsing
- **OpenPyXL**: Excel file processing

### Infrastructure Requirements
- **File System Storage**: Local storage for raw and processed data files
- **Internet Connectivity**: Required for fetching data from NHS England and ODS API
- **Environment Variables**: Configuration via `GROUP_ODS_LIST` and `TZ` settings

### Manual Integration Points
- **Model Health System (MHS)**: Manual CSV export process (credentialed access, no public API) with files placed in `/data/mhs/` directory
