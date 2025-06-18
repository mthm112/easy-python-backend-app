# main.py
"""Dynamic Pricing Intelligence Report API
----------------------------------------
FastAPI service that accepts arbitrary **SELECT** SQL queries from N8N (or any HTTP
client), retrieves the data from Supabase via its REST endpoint, enriches the
result set with light‑weight analytical annotations, produces a self‑documenting
CSV, and—optionally—uploads the file to a Supabase Storage bucket.

Key features
~~~~~~~~~~~~
* Accepts *dynamic* SQL – no hard‑coded table or column list required.
* Built‑in query validation to mitigate destructive SQL.
* Transparent translation of simple SQL to Supabase REST parameters for
  efficient pagination; heavier transforms are completed in‑memory with
  pandas.
* Annotated CSV includes metadata, query hash, basic statistics, and pricing
  insights.
* Optional upload to Supabase Storage with automatic bucket provisioning.
* Ready for N8N “HTTP Request” node – returns either a **StreamingResponse**
  (binary CSV) **or** JSON describing where the file was stored.
"""

import base64
import io
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

# ---------------------------------------------------------------------------
# Logging -------------------------------------------------------------------
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastAPI initialisation -----------------------------------------------------
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Dynamic Pricing Intelligence Report API",
    description="Dynamic CSV report generation API for competitive pricing intelligence with automated storage",
    version="3.0.0",
)

# ---------------------------------------------------------------------------
# Data models ----------------------------------------------------------------
# ---------------------------------------------------------------------------


class SQLRequest(BaseModel):
    sql_query: str
    report_name: Optional[str] = None
    description: Optional[str] = None
    include_analytics: bool = True
    upload_to_storage: bool = True


class DynamicReportRequest(BaseModel):
    sql_query: str
    report_name: str
    description: Optional[str] = "Dynamic competitive intelligence report"
    business_value: Optional[str] = "Automated pricing insights and competitive analysis"
    include_analytics: bool = True
    upload_to_storage: bool = True
    custom_filename: Optional[str] = None


# ---------------------------------------------------------------------------
# Supabase Storage helper ----------------------------------------------------
# ---------------------------------------------------------------------------


class SupabaseStorage:
    """Utility class wrapping Supabase Storage REST calls."""

    def __init__(self, anon_key: str, bucket_name: str = "reports", enabled: bool = True):
        self.anon_key = anon_key
        self.bucket_name = bucket_name.replace("_", "-")
        self.enabled = enabled and bool(anon_key)
        self.rest_url = self._determine_rest_url()

        if not self.enabled:
            logger.info("Supabase storage uploads are disabled")
        elif not self.rest_url:
            logger.error("Could not determine valid Supabase REST URL – uploads will be skipped")
            self.enabled = False
        else:
            logger.info("Storage initialised – %s (bucket '%s')", self.rest_url, self.bucket_name)

    # .....................................................................
    # Private helpers ------------------------------------------------------
    # .....................................................................

    def _determine_rest_url(self) -> Optional[str]:
        """Resolve the Supabase REST endpoint from ENV or fallbacks."""
        explicit = os.getenv("SUPABASE_URL")
        if explicit and "supabase.co" in explicit:
            return explicit
        # fallback – *replace with your own default project URL*
        return "https://fbiqlsoheofdmgqmjxfc.supabase.co"

    # .....................................................................
    # Bucket helpers -------------------------------------------------------
    # .....................................................................

    def create_bucket_if_not_exists(self) -> bool:
        """Verify bucket exists; attempt creation if not (silent fail)."""
        if not self.enabled:
            return False

        test_url = f"{self.rest_url}/storage/v1/object/list/{self.bucket_name}"
        headers = {
            "apikey": self.anon_key,
            "Authorization": f"Bearer {self.anon_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(test_url, headers=headers, json={"limit": 1, "prefix": ""}, timeout=8)
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                logger.error("Bucket '%s' does not exist – create it via Supabase UI", self.bucket_name)
                return False
            # non‑fatal – assume bucket ok
            return True
        except Exception as exc:
            logger.warning("Bucket verification failed: %s", exc)
            return True

    # .....................................................................
    # Public upload --------------------------------------------------------
    # .....................................................................

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.HTTPError)),
    )
    def upload_file_content(self, content: str, filename: str, content_type: str = "text/csv") -> Optional[str]:
        """Upload text **content** directly without touching disk."""
        if not self.enabled:
            return None
        if not self.create_bucket_if_not_exists():
            return None

        upload_url = f"{self.rest_url}/storage/v1/object/{self.bucket_name}/{filename}"
        headers = {
            "apikey": self.anon_key,
            "Authorization": f"Bearer {self.anon_key}",
            "Content-Type": content_type,
        }
        logger.info("Uploading %s (%d bytes)…", filename, len(content))
        try:
            resp = requests.post(upload_url, headers=headers, data=content.encode("utf‑8"), timeout=30)
            resp.raise_for_status()
            public_url = f"{self.rest_url}/storage/v1/object/public/{self.bucket_name}/{filename}"
            logger.info("Uploaded successfully – %s", public_url)
            return public_url
        except requests.exceptions.HTTPError as errh:
            logger.error("HTTP error %s – %s", errh.response.status_code, errh.response.text)
        except Exception as exc:
            logger.error("Upload failed: %s", exc)
        return None

    # .....................................................................
    # Smoke test -----------------------------------------------------------
    # .....................................................................

    def test_storage_connection(self) -> bool:
        if not self.enabled:
            return True
        test_name = f"test_{int(time.time())}.txt"
        test_content = "hello,supabase"  # minimal
        url = self.upload_file_content(test_content, test_name, "text/plain")
        return bool(url)


# ---------------------------------------------------------------------------
# Supabase data access -------------------------------------------------------
# ---------------------------------------------------------------------------


def get_supabase_data_dynamic(sql_query: str) -> pd.DataFrame:
    """Execute *read‑only* SQL through Supabase REST API by translating simple
    clauses into REST query‑string parameters and applying remaining
    transforms in‑memory with pandas."""

    supabase_url = os.getenv("SUPABASE_URL", "https://fbiqlsoheofdmgqmjxfc.supabase.co")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    if not supabase_key:
        raise RuntimeError("Missing SUPABASE_ANON_KEY environment variable")

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    table_name = extract_table_from_sql(sql_query) or "mbm_price_comparison"
    params = convert_sql_to_rest_params(sql_query)

    page_size = 1000
    offset = 0
    rows: List[Dict[str, Any]] = []
    while True:
        q = {"limit": page_size, "offset": offset, **params}
        resp = requests.get(f"{supabase_url}/rest/v1/{table_name}", headers=headers, params=q)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return apply_sql_transformations(df, sql_query)


# ---------------------------------------------------------------------------
# SQL utility functions ------------------------------------------------------
# ---------------------------------------------------------------------------


def extract_table_from_sql(sql_query: str) -> str:
    match = re.search(r"\bfrom\s+(?:public\.)?([a-zA-Z_][a-zA-Z0-9_]*)", sql_query, re.IGNORECASE)
    if match:
        return match.group(1).replace("`", "").replace("\"", "")
    return "mbm_price_comparison"


def convert_sql_to_rest_params(sql_query: str) -> Dict[str, Any]:
    """Very naive translator – handles SELECT cols, ORDER BY, LIMIT."""
    lower = sql_query.lower()
    params: Dict[str, Any] = {}

    # SELECT columns
    sel = re.search(r"select\s+(.*?)\s+from", lower, re.DOTALL)
    if sel:
        cols = sel.group(1).strip()
        if cols != "*" and "distinct" not in cols:
            columns = [c.strip().replace("\"", "") for c in cols.split(",") if "(" not in c]
            if columns:
                params["select"] = ",".join(columns[:20])

    # ORDER BY
    order = re.search(r"order\s+by\s+([^;\n]+)", lower)
    if order:
        coldir = order.group(1).split(" ,")[0].strip().split()
        col = coldir[0].strip("`\"")
        dir_ = "desc" if len(coldir) > 1 and "desc" in coldir[1] else "asc"
        params["order"] = f"{col}.{dir_}"

    # LIMIT
    lim = re.search(r"limit\s+(\d+)", lower)
    if lim:
        params["limit"] = min(int(lim.group(1)), 5000)
    return params


# ............................ DataFrame transformations .....................


def apply_sql_transformations(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    lower = sql_query.lower()
    if "where" in lower:
        df = apply_where_filters(df, sql_query)
    if any(kw in lower for kw in ("case when", "round(")):
        df = add_calculated_columns(df, sql_query)
    if "group by" in lower:
        df = apply_group_by(df, sql_query)
    return df


def apply_where_filters(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    up = sql_query.upper()
    if '"PRICE DIFFERENCE" > 0' in up and 'Price Difference' in df.columns:
        df = df[df['Price Difference'] > 0]
    if '"PRICE DIFFERENCE" < 0' in up and 'Price Difference' in df.columns:
        df = df[df['Price Difference'] < 0]
    if '"PRICE DIFFERENCE" < -50' in up and 'Price Difference' in df.columns:
        df = df[df['Price Difference'] < -50]
    if '"YOUR PRICE RANK" > "COMPETITOR PRICE RANK"' in up and {
        'Your Price Rank',
        'Competitor Price Rank',
    }.issubset(df.columns):
        df = df[df['Your Price Rank'] > df['Competitor Price Rank']]
    return df


def add_calculated_columns(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    if 'Price Difference' in df.columns and 'overpricing_pounds' not in df.columns:
        df['overpricing_pounds'] = df['Price Difference'] / 100
    if {'Price Difference', 'Your Price'}.issubset(df.columns) and 'overpricing_percentage' not in df.columns:
        df['overpricing_percentage'] = (df['Price Difference'] / df['Your Price'] * 100).round(1)
    if {'Your Price Rank', 'Competitor Price Rank'}.issubset(df.columns) and 'ranking_gap' not in df.columns:
        df['ranking_gap'] = df['Your Price Rank'] - df['Competitor Price Rank']
    return df


def apply_group_by(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    lower = sql_query.lower()
    grp = re.search(r"group\s+by\s+([^order\n]+)", lower)
    if not grp:
        return df
    cols = [c.strip().replace('"', '') for c in grp.group(1).split(',')]
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return df
    num_cols = df.select_dtypes(include=['number']).columns[:5]
    if not num_cols.any():
        return df
    agg = {c: 'count' for c in num_cols}
    return df.groupby(cols).agg(agg).reset_index()


# ---------------------------------------------------------------------------
# Analytics & CSV helpers ----------------------------------------------------
# ---------------------------------------------------------------------------


def generate_dynamic_analytics(df: pd.DataFrame, sql_query: str, report_name: str) -> Dict[str, Any]:
    summary = {
        "total_records": len(df),
        "generated_at": datetime.now().isoformat(),
        "report_name": report_name,
        "sql_query_hash": re.sub(r"[^0-9a-f]", "", base64.b16encode(hashlib.md5(sql_query.encode()).digest()).decode().lower())[:8],
    }

    analytics: Dict[str, Any] = {"summary": summary}

    try:
        analytics["data_insights"] = {
            "columns": list(df.columns),
            "column_count": len(df.columns),
            "data_types": df.dtypes.astype(str).to_dict(),
            "null_counts": df.isna().sum().to_dict(),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
        }

        nums = df.select_dtypes(include=['number']).columns
        if nums.any():
            analytics["numeric_insights"] = {}
            for col in nums[:10]:
                analytics["numeric_insights"][col] = {
                    "mean": df[col].mean(),
                    "median": df[col].median(),
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "std": df[col].std(),
                }

        cats = df.select_dtypes(include=['object']).columns
        if cats.any():
            analytics["categorical_insights"] = {}
            for col in cats[:5]:
                vc = df[col].value_counts().head(10)
                analytics["categorical_insights"][col] = {
                    "unique_count": df[col].nunique(),
                    "top_values": vc.to_dict(),
                }
    except Exception as exc:
        logger.warning("Partial analytics failure: %s", exc)

    return analytics


def create_dynamic_enhanced_csv(
    df: pd.DataFrame,
    analytics: Dict[str, Any],
    report_name: str,
    description: str,
    business_value: str,
    sql_query: str,
) -> str:
    """Return **text** (str) content of annotated CSV."""
    buf = io.StringIO()
    buf.write(f"# {report_name}\n")
    buf.write(f"# {description}\n")
    buf.write(f"# Business Value: {business_value}\n")
    buf.write(f"# Generated: {analytics['summary']['generated_at']}\n")
    buf.write(f"# Total Records: {analytics['summary']['total_records']}\n")
    buf.write(f"# SQL Query Hash: {analytics['summary']['sql_query_hash']}\n#\n")
    buf.write("# EXECUTED SQL QUERY (truncated):\n")
    buf.write("# " + sql_query.replace("\n", " ")[:500] + ("..." if len(sql_query) > 500 else "") + "\n#\n")

    if "data_insights" in analytics:
        di = analytics["data_insights"]
        buf.write("# DATA INSIGHTS:\n")
        buf.write(f"# Columns: {di['column_count']}\n")
        buf.write(f"# Memory Usage: {di['memory_usage_mb']} MB\n#\n")

    buf.write("# DATA:\n")
    df.to_csv(buf, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Aux helpers ---------------------------------------------------------------
# ---------------------------------------------------------------------------


def get_storage_instance() -> SupabaseStorage:
    return SupabaseStorage(
        anon_key=os.getenv("SUPABASE_ANON_KEY"),
        bucket_name=os.getenv("SUPABASE_STORAGE_BUCKET", "reports"),
        enabled=os.getenv("ENABLE_STORAGE_UPLOADS", "true").lower() == "true",
    )


def validate_sql_query(sql_query: str) -> Tuple[bool, str]:
    if not sql_query or not sql_query.strip().lower().startswith("select"):
        return False, "Only SELECT queries are allowed"
    bad = [
        " drop ",
        " delete ",
        " update ",
        " insert ",
        " alter ",
        " create ",
        " truncate ",
        " exec ",
        " execute ",
    ]
    lower = sql_query.lower()
    if any(b in lower for b in bad):
        return False, "Dangerous keywords detected"
    if "--" in sql_query or "/*" in sql_query:
        return False, "SQL comments are not allowed"
    if sql_query.count(";") > 1:
        return False, "Multiple statements detected"
    return True, "OK"


def generate_filename(report_name: str, custom: Optional[str] = None) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if custom:
        name = re.sub(r"[^\w\-_.]", "_", custom)
        if not name.endswith(".csv"):
            name += ".csv"
        return f"{name.replace('.csv', '')}_{ts}.csv"
    safe = re.sub(r"[^\w\-]", "_", report_name.lower())
    return f"dynamic_{safe}_{ts}.csv"


# ---------------------------------------------------------------------------
# Endpoint implementations ---------------------------------------------------
# ---------------------------------------------------------------------------


@app.get("/")
async def root():
    return {
        "status": "healthy",
        "service": app.title,
        "version": app.version,
        "generated": datetime.now().isoformat(),
        "endpoints": [
            "/generate-dynamic-report",
            "/generate-csv",
            "/analyze",
            "/test-db",
            "/test-storage",
            "/reports/download/{filename}",
        ],
    }


@app.post("/generate-dynamic-report")
async def generate_dynamic_report(req: DynamicReportRequest):
    # Validate SQL -------------------------------------------------------
    ok, msg = validate_sql_query(req.sql_query)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    df = get_supabase_data_dynamic(req.sql_query)
    if df.empty:
        raise HTTPException(status_code=404, detail="Query returned no results")

    analytics: Dict[str, Any] = {}
    if req.include_analytics:
        analytics = generate_dynamic_analytics(df, req.sql_query, req.report_name)

    csv_content = create_dynamic_enhanced_csv(
        df=df,
        analytics=analytics,
        report_name=req.report_name,
        description=req.description or "Dynamic competitive intelligence report",
        business_value=req.business_value or "Automated pricing insights and competitive analysis",
        sql_query=req.sql_query,
    )

    filename = generate_filename(req.report_name, req.custom_filename)

    public_url: Optional[str] = None
    if req.upload_to_storage:
        public_url = get_storage_instance().upload_file_content(csv_content, filename)

    # Decide response shape ---------------------------------------------
    if req.upload_to_storage and public_url:
        return JSONResponse(
            status_code=200,
            content={
                "report_name": req.report_name,
                "filename": filename,
                "public_url": public_url,
                "analytics": analytics,
                "records": len(df),
            },
        )

    # otherwise, stream back the CSV directly
    headers = {"Content-Disposition": f"attachment; filename={filename}"}
    return StreamingResponse(iter([csv_content.encode("utf-8")]), media_type="text/csv", headers=headers)


# ---------------------------------------------------------------------------
# Legacy/simple endpoints ----------------------------------------------------
# ---------------------------------------------------------------------------


@app.post("/generate-csv")
async def generate_csv(req: SQLRequest):
    """Older endpoint – kept for backward compatibility."""
    req = DynamicReportRequest(**req.dict(), report_name=req.report_name or "legacy_report")
    return await generate_dynamic_report(req)  # type: ignore[arg-type]


@app.post("/analyze")
async def analyze(req: SQLRequest):
    ok, msg = validate_sql_query(req.sql_query)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    df = get_supabase_data_dynamic(req.sql_query)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data returned")
    return generate_dynamic_analytics(df, req.sql_query, req.report_name or "analysis")


@app.get("/test-db")
async def test_db():
    try:
        df = get_supabase_data_dynamic("select * from mbm_price_comparison limit 5")
        return {"rows": len(df), "columns": list(df.columns)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/test-storage")
async def test_storage():
    ok = get_storage_instance().test_storage_connection()
    return {"storage_ok": ok}


@app.get("/reports/download/{filename}")
async def redirect_to_public(filename: str):
    storage = get_storage_instance()
    if not storage.rest_url:
        raise HTTPException(status_code=500, detail="Storage not configured")
    url = f"{storage.rest_url}/storage/v1/object/public/{storage.bucket_name}/{filename}"
    return RedirectResponse(url)


# ---------------------------------------------------------------------------
# Uvicorn entry point --------------------------------------------------------
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
