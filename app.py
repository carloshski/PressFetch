"""
NHS England Data Fetching and Processing API
FastAPI application for automated collection and processing of NHS England healthcare statistics.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.fetchers.ae import AEFetcher
from app.fetchers.ambulance import AmbulanceFetcher
from app.fetchers.cancer import CancerFetcher
from app.fetchers.community import CommunityFetcher
from app.fetchers.diagnostics import DiagnosticsFetcher
from app.fetchers.maternity import MaternityFetcher
from app.fetchers.rtt import RTTFetcher
from app.fetchers.workforce import WorkforceFetcher
from app.utils.audit import AuditLogger
from app.utils.io import cleanup_old_files, load_yaml, save_json
from app.utils.ods import ODSResolver

app = FastAPI(
    title="NHS England Data Explorer",
    description="Search, fetch and analyse NHS healthcare statistics for any Trust",
    version="1.0.0"
)

from starlette.middleware.base import BaseHTTPMiddleware

class NoCacheStaticMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

app.add_middleware(NoCacheStaticMiddleware)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

config = load_yaml("app/config/thresholds.yaml")

ods_codes_str = os.getenv("GROUP_ODS_LIST", "RM1,RGP,RCX")
ods_codes = [code.strip() for code in ods_codes_str.split(",")]

audit_logger = AuditLogger()
ods_resolver = ODSResolver()

DATA_FILES = {
    'rtt': 'data/processed/rtt_provider.csv',
    'ae': 'data/processed/ae_provider.csv',
    'cancer': 'data/processed/cwt_cwt_crs_provider_extract.csv',
    'diagnostics': 'data/processed/diagnostics_provider.csv',
    'ambulance': 'data/processed/ambulance_provider.csv',
    'workforce': 'data/processed/workforce_provider.csv',
    'community': 'data/processed/community_provider.csv',
    'maternity': 'data/processed/maternity_provider.csv',
}

ALL_FETCHERS = {
    'rtt': RTTFetcher,
    'ae': AEFetcher,
    'cancer': CancerFetcher,
    'diagnostics': DiagnosticsFetcher,
    'ambulance': AmbulanceFetcher,
    'workforce': WorkforceFetcher,
    'community': CommunityFetcher,
    'maternity': MaternityFetcher,
}


class CustomRunRequest(BaseModel):
    ods_codes: List[str]


def safe_json(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, float):
        if obj != obj or obj == float('inf') or obj == float('-inf'):
            return None
        return obj
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, float):
        if v != v or v == float('inf') or v == float('-inf'):
            return None
        return v
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, np.ndarray):
        return v.tolist()
    return v


def df_to_json_safe(df):
    records = df.where(df.notna(), None).to_dict(orient='records')
    clean = []
    for row in records:
        clean.append({k: clean_value(v) for k, v in row.items()})
    return clean


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/status")
async def get_api_status():
    try:
        recent_ops = audit_logger.get_recent_operations(hours=24)
        raw_files = len(os.listdir("data/raw")) if os.path.exists("data/raw") else 0
        processed_files = len(os.listdir("data/processed")) if os.path.exists("data/processed") else 0

        last_runs = {}
        all_datasets = list(ALL_FETCHERS.keys())
        for dataset in all_datasets:
            dataset_ops = [op for op in recent_ops if op.get('dataset') == dataset and op.get('success')]
            if dataset_ops:
                last_runs[dataset] = max(dataset_ops, key=lambda x: x['timestamp'])['timestamp']
            else:
                last_runs[dataset] = None

        return {
            "timestamp": datetime.now().isoformat(),
            "ods_codes": ods_codes,
            "last_runs": last_runs,
            "file_counts": {
                "raw_files": raw_files,
                "processed_files": processed_files,
            },
            "recent_operations": len(recent_ops),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_status():
    try:
        recent_ops = audit_logger.get_recent_operations(hours=24)
        raw_files = len(os.listdir("data/raw")) if os.path.exists("data/raw") else 0
        processed_files = len(os.listdir("data/processed")) if os.path.exists("data/processed") else 0
        mhs_files = len(os.listdir("data/mhs")) if os.path.exists("data/mhs") else 0

        last_runs = {}
        all_datasets = list(ALL_FETCHERS.keys())
        for dataset in all_datasets:
            dataset_ops = [op for op in recent_ops if op.get('dataset') == dataset and op.get('success')]
            if dataset_ops:
                last_runs[dataset] = max(dataset_ops, key=lambda x: x['timestamp'])['timestamp']
            else:
                last_runs[dataset] = None

        trust_info = ods_resolver.resolve_trust_codes(ods_codes)

        status = {
            "timestamp": datetime.now().isoformat(),
            "ods_configuration": {"codes": ods_codes, "trusts": trust_info},
            "last_runs": last_runs,
            "file_counts": {"raw_files": raw_files, "processed_files": processed_files, "mhs_files": mhs_files},
            "recent_operations": len(recent_ops),
            "system_health": "operational"
        }
        return JSONResponse(content=status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trusts/search")
async def search_trusts(q: str = Query(..., min_length=2)):
    results = []
    query = q.upper().strip()

    if len(query) <= 5 and query.isalnum():
        try:
            resp = requests.get(
                f"https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/{query}",
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                org = data.get('Organisation', {})
                name = org.get('Name', '')
                if name:
                    results.append({"code": query, "name": name})
        except Exception:
            pass

    try:
        resp = requests.get(
            "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations",
            params={
                "Name": q,
                "PrimaryRoleId": "RO197",
                "Status": "Active",
                "Limit": 20
            },
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            orgs = data.get('Organisations', [])
            for org in orgs:
                code = org.get('OrgId', '')
                name = org.get('Name', '')
                if code and name and not any(r['code'] == code for r in results):
                    results.append({"code": code, "name": name})
    except Exception:
        pass

    if not results:
        try:
            resp = requests.get(
                "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations",
                params={
                    "Name": q,
                    "Status": "Active",
                    "Limit": 20
                },
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                orgs = data.get('Organisations', [])
                for org in orgs:
                    code = org.get('OrgId', '')
                    name = org.get('Name', '')
                    if code and name:
                        results.append({"code": code, "name": name})
        except Exception:
            pass

    return results


@app.get("/api/data/{dataset}")
async def get_dataset_data(dataset: str, ods_codes: str = Query("")):
    if dataset not in DATA_FILES:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset}")

    file_path = DATA_FILES[dataset]
    if not os.path.exists(file_path):
        return {"rows": [], "columns": [], "metrics": {}, "message": "No data available"}

    try:
        df = pd.read_csv(file_path)

        codes = [c.strip().upper() for c in ods_codes.split(",") if c.strip()] if ods_codes else []
        if codes:
            code_col = 'provider_code'
            if code_col in df.columns:
                df[code_col] = df[code_col].astype(str).str.strip().str.upper()
                df = df[df[code_col].isin(codes)]

        metrics = compute_dataset_metrics(dataset, df)

        display_cols = [c for c in df.columns if c not in ['data_source', 'processing_date']]

        if dataset == 'rtt':
            key_cols = ['Period', 'provider_code', 'Provider Org Name', 'trust_name',
                        'RTT Part Description', 'Treatment Function Name', 'Total']
            available_key = [c for c in key_cols if c in df.columns]
            if available_key:
                display_cols = available_key

        limit = 500
        rows = df_to_json_safe(df[display_cols].head(limit))

        clean_metrics = json.loads(json.dumps(metrics, default=safe_json))
        return JSONResponse(content={"rows": rows, "columns": display_cols, "metrics": clean_metrics, "total_rows": len(df)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def compute_dataset_metrics(dataset: str, df: pd.DataFrame) -> Dict:
    metrics = {}
    if df.empty:
        return metrics

    code_col = 'provider_code'
    if code_col not in df.columns:
        return metrics

    for code in df[code_col].unique():
        code_df = df[df[code_col] == code]

        if dataset == 'rtt':
            incomplete_df = code_df
            rtt_part_col = 'RTT Part Description'
            if rtt_part_col in code_df.columns:
                inc = code_df[code_df[rtt_part_col].str.contains('Incomplete', case=False, na=False)]
                if not inc.empty:
                    incomplete_df = inc

            total_all_col = 'Total All' if 'Total All' in incomplete_df.columns else 'Total'
            total_pathways = pd.to_numeric(incomplete_df[total_all_col], errors='coerce').sum() if total_all_col in incomplete_df.columns else 0

            within_18 = 0
            over_52 = 0
            week_cols = [c for c in incomplete_df.columns if 'Weeks SUM' in c]
            for wc in week_cols:
                try:
                    parts = wc.replace('Gt ', '').replace('Gt_', '').split(' To ')
                    if len(parts) < 2:
                        parts = wc.replace('Gt ', '').replace('Gt_', '').split('_To_')
                    week_num = int(parts[0].strip().split()[-1])
                    val = pd.to_numeric(incomplete_df[wc], errors='coerce').sum()
                    if week_num < 18:
                        within_18 += val
                    if week_num >= 52:
                        over_52 += val
                except (ValueError, IndexError):
                    pass

            metrics[code] = {
                "total_pathways": int(total_pathways) if not pd.isna(total_pathways) else 0,
                "within_18_weeks": int(within_18) if not pd.isna(within_18) else 0,
                "over_52_weeks": int(over_52) if not pd.isna(over_52) else 0
            }

        elif dataset == 'ae':
            type1_col = 'A&E attendances Type 1'
            emerg_col = None
            for c in code_df.columns:
                if 'emergency' in c.lower() and 'admission' in c.lower():
                    emerg_col = c
                    break
            dta_12h_col = None
            for c in code_df.columns:
                if '12' in c and ('dta' in c.lower() or 'hrs' in c.lower() or 'hour' in c.lower()):
                    dta_12h_col = c
                    break

            type1_att = pd.to_numeric(code_df[type1_col], errors='coerce').sum() if type1_col in code_df.columns else 0
            emerg_adm = pd.to_numeric(code_df[emerg_col], errors='coerce').sum() if emerg_col and emerg_col in code_df.columns else 0
            dta_12h = pd.to_numeric(code_df[dta_12h_col], errors='coerce').sum() if dta_12h_col and dta_12h_col in code_df.columns else 0

            metrics[code] = {
                "type1_attendances": int(type1_att),
                "emergency_admissions": int(emerg_adm),
                "dta_12h_waits": int(dta_12h)
            }

        elif dataset == 'cancer':
            total_treated_col = 'TOTAL TREATED'
            within_col = 'WITHIN STANDARD'
            breaches_col = 'BREACHES'

            total_treated = pd.to_numeric(code_df[total_treated_col], errors='coerce').sum() if total_treated_col in code_df.columns else 0
            within_std = pd.to_numeric(code_df[within_col], errors='coerce').sum() if within_col in code_df.columns else 0
            breaches = pd.to_numeric(code_df[breaches_col], errors='coerce').sum() if breaches_col in code_df.columns else 0

            metrics[code] = {
                "total_treated": int(total_treated),
                "within_standard": int(within_std),
                "breaches": int(breaches)
            }

        elif dataset == 'diagnostics':
            total_waiting = 0
            waiting_6plus = 0
            for c in code_df.columns:
                cl = c.lower()
                if 'total' in cl and ('waiting' in cl or 'list' in cl):
                    total_waiting = pd.to_numeric(code_df[c], errors='coerce').sum()
                if ('6' in c or 'six' in cl) and ('week' in cl or 'wk' in cl):
                    waiting_6plus = pd.to_numeric(code_df[c], errors='coerce').sum()
            metrics[code] = {
                "total_waiting": int(total_waiting) if not pd.isna(total_waiting) else 0,
                "waiting_over_6_weeks": int(waiting_6plus) if not pd.isna(waiting_6plus) else 0,
                "record_count": len(code_df)
            }

        elif dataset == 'ambulance':
            metrics[code] = {"record_count": len(code_df)}
            for c in code_df.columns:
                cl = c.lower()
                if 'mean' in cl or 'average' in cl or 'response' in cl:
                    val = pd.to_numeric(code_df[c], errors='coerce').mean()
                    metrics[code][c.replace(' ', '_').lower()] = round(float(val), 1) if not pd.isna(val) else None

        elif dataset == 'workforce':
            fte_val = 0
            hc_val = 0
            for c in code_df.columns:
                cl = c.lower()
                if 'fte' in cl and 'total' in cl:
                    fte_val = pd.to_numeric(code_df[c], errors='coerce').sum()
                elif 'headcount' in cl and 'total' in cl:
                    hc_val = pd.to_numeric(code_df[c], errors='coerce').sum()
            if fte_val == 0:
                for c in code_df.columns:
                    if 'fte' in c.lower():
                        fte_val = pd.to_numeric(code_df[c], errors='coerce').sum()
                        break
            if hc_val == 0:
                for c in code_df.columns:
                    if 'headcount' in c.lower() or c.lower() == 'hc':
                        hc_val = pd.to_numeric(code_df[c], errors='coerce').sum()
                        break
            metrics[code] = {
                "total_fte": round(float(fte_val), 1) if not pd.isna(fte_val) else 0,
                "total_headcount": int(hc_val) if not pd.isna(hc_val) else 0,
                "record_count": len(code_df)
            }

        elif dataset == 'community':
            metrics[code] = {"record_count": len(code_df)}
            for c in code_df.columns:
                cl = c.lower()
                if 'referral' in cl or 'contact' in cl or 'attendance' in cl:
                    val = pd.to_numeric(code_df[c], errors='coerce').sum()
                    key = c.replace(' ', '_').lower()[:30]
                    metrics[code][key] = int(val) if not pd.isna(val) else 0
                    break

        elif dataset == 'maternity':
            metrics[code] = {"record_count": len(code_df)}
            for c in code_df.columns:
                cl = c.lower()
                if 'booking' in cl or 'delivery' in cl or 'birth' in cl:
                    val = pd.to_numeric(code_df[c], errors='coerce').sum()
                    key = c.replace(' ', '_').lower()[:30]
                    metrics[code][key] = int(val) if not pd.isna(val) else 0
                    break

        else:
            metrics[code] = {"record_count": len(code_df)}

    return metrics


@app.get("/api/summary")
async def get_summary(ods_codes: str = Query("")):
    codes = [c.strip().upper() for c in ods_codes.split(",") if c.strip()] if ods_codes else []
    summary = {}

    for dataset, file_path in DATA_FILES.items():
        if not os.path.exists(file_path):
            continue
        try:
            df = pd.read_csv(file_path)
            if codes and 'provider_code' in df.columns:
                df['provider_code'] = df['provider_code'].astype(str).str.strip().str.upper()
                df = df[df['provider_code'].isin(codes)]
            if not df.empty:
                summary[dataset] = compute_dataset_metrics(dataset, df)
        except Exception:
            pass

    clean_summary = json.loads(json.dumps(summary, default=safe_json))
    return JSONResponse(content=clean_summary)


def find_period_column(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        cl = c.lower()
        if cl in ('period', 'month', 'date', 'year', 'reporting period', 'month_year'):
            return c
    for c in df.columns:
        cl = c.lower()
        if 'period' in cl or 'month' in cl or 'date' in cl:
            return c
    return None


@app.get("/download/methodology")
async def download_methodology():
    file_path = os.path.join("docs", "DATA_FETCHING_METHODOLOGY.md")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Methodology document not found")
    return FileResponse(
        file_path,
        media_type="text/markdown",
        filename="DATA_FETCHING_METHODOLOGY.md",
    )


@app.get("/api/download/{dataset}")
async def download_dataset(dataset: str, ods_codes: str = Query(""), date_from: str = Query(""), date_to: str = Query("")):
    if dataset not in DATA_FILES:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset}")

    file_path = DATA_FILES[dataset]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="No data available for download")

    try:
        df = pd.read_csv(file_path)
        codes = [c.strip().upper() for c in ods_codes.split(",") if c.strip()] if ods_codes else []
        if codes and 'provider_code' in df.columns:
            df['provider_code'] = df['provider_code'].astype(str).str.strip().str.upper()
            df = df[df['provider_code'].isin(codes)]

        if date_from or date_to:
            period_col = find_period_column(df)
            if period_col:
                df[period_col] = df[period_col].astype(str).str.strip()
                if date_from:
                    df = df[df[period_col].str.lower() >= date_from.lower()]
                if date_to:
                    df = df[df[period_col].str.lower() <= date_to.lower() + '\uffff']

        display_cols = [c for c in df.columns if c not in ['data_source', 'processing_date']]

        import io
        output = io.StringIO()
        df[display_cols].to_csv(output, index=False)
        output.seek(0)

        filename = f"nhs_{dataset}_data_{datetime.now().strftime('%Y%m%d')}.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run-custom")
async def run_custom_pipeline(req: CustomRunRequest):
    try:
        pipeline_start = datetime.now()
        custom_codes = [c.strip().upper() for c in req.ods_codes if c.strip()]

        if not custom_codes:
            raise HTTPException(status_code=400, detail="No ODS codes provided")

        results = {"pipeline_start": pipeline_start.isoformat(), "ods_codes": custom_codes, "datasets": {}}

        for name, FetcherClass in ALL_FETCHERS.items():
            try:
                fetcher = FetcherClass(custom_codes)
                results["datasets"][name] = fetcher.fetch_and_process()
            except Exception as e:
                results["datasets"][name] = {"success": False, "error": str(e), "dataset": name}

        pipeline_end = datetime.now()
        results["pipeline_duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()

        successful = sum(1 for d in results["datasets"].values() if d.get("success"))
        results["summary"] = {
            "total_datasets": len(results["datasets"]),
            "successful_datasets": successful,
            "overall_success": successful > 0
        }

        audit_logger.log_operation('pipeline', 'custom_run', successful > 0, {
            'ods_codes': custom_codes,
            'successful_datasets': successful,
            'duration_seconds': results["pipeline_duration_seconds"]
        })

        return JSONResponse(content=results)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run")
async def run_full_pipeline():
    try:
        pipeline_start = datetime.now()

        results = {"pipeline_start": pipeline_start.isoformat(), "ods_codes": ods_codes, "datasets": {}}

        for name, FetcherClass in ALL_FETCHERS.items():
            try:
                fetcher = FetcherClass(ods_codes)
                results["datasets"][name] = fetcher.fetch_and_process()
            except Exception as e:
                results["datasets"][name] = {"success": False, "error": str(e), "dataset": name}

        group_summary = generate_group_summary(results)
        save_json(group_summary, os.path.join("data/processed", "group_summary.json"))

        pipeline_end = datetime.now()
        results["pipeline_end"] = pipeline_end.isoformat()
        results["pipeline_duration_seconds"] = (pipeline_end - pipeline_start).total_seconds()
        results["group_summary"] = group_summary

        successful = sum(1 for d in results["datasets"].values() if d.get("success"))
        results["summary"] = {
            "total_datasets": len(results["datasets"]),
            "successful_datasets": successful,
            "overall_success": successful > 0
        }

        audit_logger.log_operation('pipeline', 'full_run', successful > 0, {
            'successful_datasets': successful,
            'total_datasets': len(results["datasets"]),
            'duration_seconds': results["pipeline_duration_seconds"]
        })

        return JSONResponse(content=results)
    except Exception as e:
        audit_logger.log_operation('pipeline', 'full_run', False, {'error': str(e)})
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload/mhs")
async def upload_mhs_data(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mhs_upload_{timestamp}.csv"
        file_path = os.path.join("data/mhs", filename)
        os.makedirs("data/mhs", exist_ok=True)

        content = await file.read()
        with open(file_path, 'wb') as f:
            f.write(content)

        mhs_result = process_mhs_data(file_path)

        audit_logger.log_operation('mhs', 'upload', mhs_result['success'], {
            'filename': file.filename, 'file_path': file_path, 'file_size': len(content)
        })

        return JSONResponse(content=mhs_result)
    except Exception as e:
        audit_logger.log_operation('mhs', 'upload', False, {'error': str(e)})
        raise HTTPException(status_code=500, detail=str(e))


def generate_group_summary(pipeline_results: Dict) -> Dict:
    summary = {
        "timestamp": datetime.now().isoformat(),
        "ods_codes": ods_codes,
        "source_urls": {},
        "aggregated_metrics": {},
        "last_updated": {}
    }

    for dataset_name, dataset_result in pipeline_results.get("datasets", {}).items():
        if not dataset_result.get("success", False):
            continue
        download_info = dataset_result.get("download", {})
        summary["source_urls"][dataset_name] = download_info.get("source_url", "")
        summary["last_updated"][dataset_name] = dataset_result.get("timestamp", "")

        output_file = dataset_result.get("output_file")
        if output_file and os.path.exists(output_file):
            try:
                df = pd.read_csv(output_file)
                if not df.empty:
                    summary["aggregated_metrics"][dataset_name] = {
                        "total_records": len(df),
                        "providers": df['provider_code'].unique().tolist() if 'provider_code' in df.columns else []
                    }
            except Exception as e:
                audit_logger.log_operation('summary', 'aggregate', False,
                                         {'dataset': dataset_name, 'error': str(e)})

    return summary


@app.get("/report", response_class=HTMLResponse)
async def report_page(request: Request):
    return templates.TemplateResponse("report.html", {"request": request})


@app.get("/api/freshness")
async def get_data_freshness():
    freshness = {}
    for dataset, file_path in DATA_FILES.items():
        if os.path.exists(file_path):
            mtime = os.path.getmtime(file_path)
            mod_dt = datetime.fromtimestamp(mtime)
            age_hours = (datetime.now() - mod_dt).total_seconds() / 3600
            freshness[dataset] = {
                "last_updated": mod_dt.strftime("%d %b %Y %H:%M"),
                "age_hours": round(age_hours, 1),
                "stale": age_hours > 168
            }
        else:
            freshness[dataset] = {
                "last_updated": None,
                "age_hours": None,
                "stale": True
            }
    return JSONResponse(content=freshness)


@app.get("/api/comparison")
async def get_comparison_data(ods_codes: str = Query(""), datasets: str = Query("")):
    codes = [c.strip().upper() for c in ods_codes.split(",") if c.strip()] if ods_codes else []
    ds_list = [d.strip() for d in datasets.split(",") if d.strip()] if datasets else list(DATA_FILES.keys())

    comparison = {}
    for dataset in ds_list:
        if dataset not in DATA_FILES:
            continue
        file_path = DATA_FILES[dataset]
        if not os.path.exists(file_path):
            continue
        try:
            df = pd.read_csv(file_path)
            if 'provider_code' not in df.columns:
                continue
            df['provider_code'] = df['provider_code'].astype(str).str.strip().str.upper()
            if codes:
                df = df[df['provider_code'].isin(codes)]
            if df.empty:
                continue

            period_col = None
            for c in df.columns:
                cl = c.lower()
                if cl in ('period', 'month', 'date', 'year', 'reporting period', 'month_year'):
                    period_col = c
                    break
            if not period_col:
                for c in df.columns:
                    cl = c.lower()
                    if 'period' in cl or 'month' in cl or 'date' in cl:
                        period_col = c
                        break

            metrics = compute_dataset_metrics(dataset, df)
            trust_series = {}

            if period_col:
                value_col = get_primary_value_col(dataset, df)
                if value_col:
                    for code in df['provider_code'].unique():
                        code_df = df[df['provider_code'] == code]
                        grouped = code_df.groupby(period_col)[value_col].apply(
                            lambda x: pd.to_numeric(x, errors='coerce').sum()
                        ).reset_index()
                        trust_series[code] = {
                            "labels": grouped[period_col].astype(str).tolist(),
                            "values": [clean_value(v) for v in grouped[value_col].tolist()]
                        }

            comparison[dataset] = {
                "metrics": json.loads(json.dumps(metrics, default=safe_json)),
                "has_timeseries": period_col is not None and len(trust_series) > 0,
                "value_label": get_primary_value_label(dataset),
                "trust_series": json.loads(json.dumps(trust_series, default=safe_json))
            }
        except Exception as e:
            comparison[dataset] = {"error": str(e)}

    return JSONResponse(content=comparison)


def get_primary_value_col(dataset: str, df: pd.DataFrame) -> Optional[str]:
    col_map = {
        'rtt': ['Total All', 'Total'],
        'ae': ['A&E attendances Type 1'],
        'cancer': ['TOTAL TREATED'],
        'diagnostics': ['Total Waiting List', 'Total waiting list', 'Total'],
        'ambulance': ['A1', 'Total'],
        'workforce': ['Total FTE', 'FTE'],
        'community': ['Referrals', 'Contacts', 'Attendances'],
        'maternity': ['Bookings', 'Deliveries', 'Births']
    }
    candidates = col_map.get(dataset, [])
    for c in candidates:
        if c in df.columns:
            return c
    for c in candidates:
        for dc in df.columns:
            if c.lower() in dc.lower():
                return dc
    heuristic_keywords = ['total', 'count', 'fte', 'attendan', 'referral', 'booking', 'delivery', 'birth', 'response', 'waiting']
    for c in df.columns:
        cl = c.lower()
        if any(kw in cl for kw in heuristic_keywords):
            if pd.api.types.is_numeric_dtype(df[c]) or df[c].dtype == object:
                try:
                    pd.to_numeric(df[c], errors='coerce').dropna()
                    return c
                except Exception:
                    pass
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    non_id_cols = [c for c in numeric_cols if 'code' not in c.lower() and 'id' not in c.lower()]
    if non_id_cols:
        return non_id_cols[0]
    return None


def get_primary_value_label(dataset: str) -> str:
    labels = {
        'rtt': 'Total Incomplete Pathways',
        'ae': 'Type 1 Attendances',
        'cancer': 'Total Treated',
        'diagnostics': 'Total Waiting',
        'ambulance': 'Response Records',
        'workforce': 'Total FTE',
        'community': 'Activity Count',
        'maternity': 'Activity Count'
    }
    return labels.get(dataset, 'Value')


@app.get("/api/trends")
async def get_trend_data(ods_codes: str = Query("")):
    codes = [c.strip().upper() for c in ods_codes.split(",") if c.strip()] if ods_codes else []
    trends = {}

    for dataset, file_path in DATA_FILES.items():
        if not os.path.exists(file_path):
            continue
        try:
            df = pd.read_csv(file_path)
            if codes and 'provider_code' in df.columns:
                df['provider_code'] = df['provider_code'].astype(str).str.strip().str.upper()
                df = df[df['provider_code'].isin(codes)]
            if df.empty:
                continue

            period_col = None
            for c in df.columns:
                cl = c.lower()
                if cl in ('period', 'month', 'date', 'year', 'reporting period', 'month_year'):
                    period_col = c
                    break
            if not period_col:
                for c in df.columns:
                    cl = c.lower()
                    if 'period' in cl or 'month' in cl or 'date' in cl:
                        period_col = c
                        break

            if not period_col:
                metrics = compute_dataset_metrics(dataset, df)
                trends[dataset] = {
                    "type": "snapshot",
                    "metrics": json.loads(json.dumps(metrics, default=safe_json))
                }
                continue

            trend_data = extract_trend_series(dataset, df, period_col)
            trends[dataset] = {
                "type": "timeseries",
                "period_column": period_col,
                "series": json.loads(json.dumps(trend_data, default=safe_json))
            }
        except Exception as e:
            trends[dataset] = {"type": "error", "error": str(e)}

    return JSONResponse(content=trends)


def extract_trend_series(dataset: str, df: pd.DataFrame, period_col: str) -> Dict:
    series = {}

    if dataset == 'rtt':
        inc_col = 'RTT Part Description'
        if inc_col in df.columns:
            df = df[df[inc_col].str.contains('Incomplete', case=False, na=False)]
        total_col = 'Total All' if 'Total All' in df.columns else 'Total'
        if total_col in df.columns:
            grouped = df.groupby(period_col)[total_col].apply(
                lambda x: pd.to_numeric(x, errors='coerce').sum()
            ).reset_index()
            series['total_pathways'] = {
                "labels": grouped[period_col].astype(str).tolist(),
                "values": grouped[total_col].tolist(),
                "label": "Total Incomplete Pathways"
            }

    elif dataset == 'ae':
        type1_col = 'A&E attendances Type 1'
        if type1_col in df.columns:
            grouped = df.groupby(period_col)[type1_col].apply(
                lambda x: pd.to_numeric(x, errors='coerce').sum()
            ).reset_index()
            series['type1_attendances'] = {
                "labels": grouped[period_col].astype(str).tolist(),
                "values": grouped[type1_col].tolist(),
                "label": "Type 1 Attendances"
            }

    elif dataset == 'cancer':
        for col_name, label in [('TOTAL TREATED', 'Total Treated'), ('WITHIN STANDARD', 'Within Standard')]:
            if col_name in df.columns:
                grouped = df.groupby(period_col)[col_name].apply(
                    lambda x: pd.to_numeric(x, errors='coerce').sum()
                ).reset_index()
                series[col_name.lower().replace(' ', '_')] = {
                    "labels": grouped[period_col].astype(str).tolist(),
                    "values": grouped[col_name].tolist(),
                    "label": label
                }

    elif dataset == 'workforce':
        for c in df.columns:
            if 'fte' in c.lower():
                grouped = df.groupby(period_col)[c].apply(
                    lambda x: pd.to_numeric(x, errors='coerce').sum()
                ).reset_index()
                series['fte'] = {
                    "labels": grouped[period_col].astype(str).tolist(),
                    "values": grouped[c].tolist(),
                    "label": "Total FTE"
                }
                break

    else:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        for nc in numeric_cols[:3]:
            try:
                grouped = df.groupby(period_col)[nc].apply(
                    lambda x: pd.to_numeric(x, errors='coerce').sum()
                ).reset_index()
                series[nc.lower().replace(' ', '_')[:30]] = {
                    "labels": grouped[period_col].astype(str).tolist(),
                    "values": grouped[nc].tolist(),
                    "label": nc
                }
            except Exception:
                pass

    return series


def process_mhs_data(file_path: str) -> Dict:
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            return {"success": False, "error": "Empty CSV file"}

        if "provider_code" in df.columns:
            our_trusts = df[df["provider_code"].isin(ods_codes)]
        else:
            our_trusts = df

        benchmarks = {}
        numeric_columns = df.select_dtypes(include=['number']).columns

        for col in numeric_columns:
            if col in our_trusts.columns:
                our_values = our_trusts[col].dropna()
                all_values = df[col].dropna()
                if len(all_values) > 0 and len(our_values) > 0:
                    median_value = all_values.median()
                    our_median = our_values.median()
                    percentile = (all_values <= our_median).mean() * 100
                    benchmarks[col] = {
                        "our_median": float(our_median),
                        "peer_median": float(median_value),
                        "percentile_rank": float(percentile),
                    }

        benchmark_result = {
            "timestamp": datetime.now().isoformat(),
            "file_path": file_path,
            "trust_count": len(our_trusts),
            "peer_count": len(df),
            "benchmarks": benchmarks
        }

        benchmark_path = os.path.join("data/processed", "mhs_benchmarks.json")
        save_json(benchmark_result, benchmark_path)

        return {"success": True, "benchmarks_file": benchmark_path, "benchmark_result": benchmark_result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.on_event("startup")
async def startup_event():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/mhs", exist_ok=True)
    audit_logger.log_operation('system', 'startup', True, {
        'ods_codes': ods_codes, 'timezone': os.getenv('TZ', 'UTC')
    })


@app.on_event("shutdown")
async def shutdown_event():
    retention_config = config.get('retention', {}) if config else {}
    cleanup_old_files("data/raw", retention_config.get('raw_files', 365))
    cleanup_old_files("data/processed", retention_config.get('processed_files', 180))
    audit_logger.log_operation('system', 'shutdown', True, {})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
