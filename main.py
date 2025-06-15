#!/usr/bin/env python3
"""
MBM Pricing Analysis API with Supabase Storage Integration
"""

import os
import time
import io
import re
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MBM Pricing Analysis API",
    description="API for analyzing competitive pricing data with automated report generation and storage",
    version="2.0.0"
)

class SQLRequest(BaseModel):
    sql_query: str

class ReportRequest(BaseModel):
    report_type: str  # "overpriced", "ranking_issues", "price_increase", "competitive_threat"
    include_analytics: bool = True
    upload_to_storage: bool = True

# Supabase Storage Class (adapted from your working implementation)
class SupabaseStorage:
    """Supabase storage operations for MBM reports"""

    def __init__(self, anon_key: str, bucket_name: str = "reports", enabled: bool = True):
        self.anon_key = anon_key
        # normalize bucket name to use hyphens
        self.bucket_name = bucket_name.replace('_', '-')
        self.enabled = enabled and bool(anon_key)

        # Use your specific Supabase URL
        self.rest_url = self._determine_rest_url()
        
        if not self.enabled:
            logger.info("Supabase storage uploads are disabled")
        elif not self.anon_key:
            logger.warning("Supabase anon key not provided - uploads will be skipped")
            self.enabled = False
        elif not self.rest_url:
            logger.error("Could not determine valid Supabase REST URL - uploads will be skipped")
            self.enabled = False
        else:
            logger.info(f"Storage initialized - REST URL: {self.rest_url}, Bucket: {self.bucket_name}")

    def _determine_rest_url(self) -> Optional[str]:
        """Determine the correct Supabase REST API URL"""
        # Priority 1: Use SUPABASE_URL if available
        supabase_url = os.getenv("SUPABASE_URL")
        if supabase_url and "supabase.co" in supabase_url and "pooler" not in supabase_url:
            logger.info(f"Using SUPABASE_URL: {supabase_url}")
            return supabase_url
        
        # Priority 2: Use your specific project URL as fallback
        default_url = "https://fbiqlsoheofdmgqmjxfc.supabase.co"
        logger.info(f"Using default project URL: {default_url}")
        return default_url

    def create_bucket_if_not_exists(self) -> bool:
        """Check if bucket exists; if not, provide helpful error message"""
        if not self.enabled or not self.anon_key or not self.rest_url:
            logger.debug("Storage uploads disabled or missing credentials.")
            return False
        
        try:
            # Test bucket access by trying to list objects
            test_url = f"{self.rest_url}/storage/v1/object/list/{self.bucket_name}?limit=1"
            headers = {
                "apikey": self.anon_key,
                "Authorization": f"Bearer {self.anon_key}"
            }
            
            response = requests.post(test_url, headers=headers, json={}, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"Bucket '{self.bucket_name}' is accessible")
                return True
            elif response.status_code == 404:
                logger.error(f"Bucket '{self.bucket_name}' does not exist. Please create it in your Supabase dashboard.")
                return False
            else:
                logger.warning(f"Bucket check returned {response.status_code}: {response.text}")
                return True  # Assume it exists and try upload anyway
                
        except Exception as e:
            logger.warning(f"Could not verify bucket existence: {str(e)}. Proceeding with upload attempt.")
            return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.HTTPError))
    )
    def upload_file_content(self, file_content: str, file_name: str, content_type: str = "text/csv") -> Optional[str]:
        """Upload file content to Supabase storage and return public URL"""
        if not self.enabled:
            logger.debug("Storage uploads are disabled. Skipping upload.")
            return None
        if not self.anon_key:
            logger.warning("SUPABASE_ANON_KEY not set. Skipping upload.")
            return None
        if not self.rest_url:
            logger.error("Could not determine Supabase URL. Cannot upload file.")
            return None
            
        if not self.create_bucket_if_not_exists():
            logger.error(f"Failed to verify bucket '{self.bucket_name}'. Cannot upload file.")
            return None

        try:
            # Convert string content to bytes
            file_data = file_content.encode('utf-8')

            upload_url = f"{self.rest_url}/storage/v1/object/{self.bucket_name}/{file_name}"
            
            # Debug logging
            logger.debug(f"REST URL: {self.rest_url}")
            logger.debug(f"Bucket name: {self.bucket_name}")
            logger.debug(f"Full upload URL: {upload_url}")
            
            headers = {
                "apikey": self.anon_key,
                "Authorization": f"Bearer {self.anon_key}",
                "Content-Type": content_type
            }
            
            response = requests.post(upload_url, headers=headers, data=file_data, timeout=30)
            response.raise_for_status()

            public_url = f"{self.rest_url}/storage/v1/object/public/{self.bucket_name}/{file_name}"
            logger.info(f"Successfully uploaded {file_name} to bucket '{self.bucket_name}'")
            logger.info(f"Public URL: {public_url}")
            return public_url
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error uploading to Supabase storage: {e.response.status_code} - {e.response.text}")
            logger.error(f"Failed URL was: {upload_url}")
            return None
        except Exception as e:
            logger.error(f"Error uploading to Supabase storage: {str(e)}")
            return None

    def test_storage_connection(self) -> bool:
        """Test Supabase storage by uploading and deleting a small test file"""
        if not self.enabled:
            logger.info("Storage uploads disabled; skipping test.")
            return True
        if not self.anon_key:
            logger.warning("SUPABASE_ANON_KEY not set; skipping test.")
            return True
        if not self.rest_url:
            logger.error("No Supabase URL; cannot test storage.")
            return False

        test_content = "test,data\n1,hello\n2,world"
        filename = f"test_upload_{int(time.time())}.csv"
        
        logger.info(f"Testing upload to bucket: {self.bucket_name}")
        upload_url = self.upload_file_content(test_content, filename)
        
        if not upload_url:
            logger.error("Test upload failed")
            return False

        logger.info(f"Successfully uploaded test file: {filename}")
        logger.info(f"Public URL: {upload_url}")
        
        # Cleanup - try to delete test file
        try:
            delete_url = f"{self.rest_url}/storage/v1/object/{self.bucket_name}/{filename}"
            headers = {
                "apikey": self.anon_key,
                "Authorization": f"Bearer {self.anon_key}"
            }
            del_resp = requests.delete(delete_url, headers=headers, timeout=10)
            if del_resp.status_code not in (200, 204):
                logger.warning(f"Could not delete test file: {del_resp.status_code}")
        except Exception as e:
            logger.warning(f"Could not delete test file: {str(e)}")
        
        return True

# Pre-defined report configurations
REPORT_CONFIGS = {
    "overpriced": {
        "name": "Overpriced Products Report",
        "description": "Products where MBM is more expensive than competitors",
        "sql": '''
            SELECT "Product ID", "Product Name", "Brand", "Category", "Sub-Category", 
                   "Seller Name", 
                   ROUND("Your Price"::numeric/100, 2) as mbm_price_pounds,
                   ROUND("Seller Total Price"::numeric/100, 2) as competitor_price_pounds,
                   ROUND("Price Difference"::numeric/100, 2) as overpricing_pounds,
                   "Your Price Rank", "Competitor Price Rank"
            FROM public.mbm_price_comparison 
            WHERE "Price Difference" > 0 
            ORDER BY "Price Difference" DESC
        ''',
        "business_value": "Immediate revenue protection opportunities"
    },
    "ranking_issues": {
        "name": "Ranking Issues Report", 
        "description": "Products with good pricing but poor search visibility",
        "sql": '''
            SELECT "Product ID", "Product Name", "Category", "Sub-Category",
                   "Seller Name",
                   ROUND("Your Price"::numeric/100, 2) as mbm_price_pounds,
                   ROUND("Seller Total Price"::numeric/100, 2) as competitor_price_pounds,
                   ROUND(ABS("Price Difference")::numeric/100, 2) as price_advantage_pounds,
                   "Your Price Rank", "Competitor Price Rank",
                   ("Your Price Rank" - "Competitor Price Rank") as ranking_gap
            FROM public.mbm_price_comparison 
            WHERE "Price Difference" < 0 AND "Your Price Rank" > "Competitor Price Rank"
            ORDER BY ("Your Price Rank" - "Competitor Price Rank") DESC
        ''',
        "business_value": "SEO/visibility optimization opportunities"
    },
    "price_increase": {
        "name": "Price Increase Opportunities",
        "description": "Products where MBM can safely increase prices",
        "sql": '''
            SELECT "Product ID", "Product Name", "Brand", "Category", "Sub-Category",
                   "Seller Name",
                   ROUND("Your Price"::numeric/100, 2) as current_mbm_price,
                   ROUND("Seller Total Price"::numeric/100, 2) as competitor_price,
                   ROUND(ABS("Price Difference")::numeric/100, 2) as potential_increase,
                   "Your Price Rank", "Competitor Price Rank"
            FROM public.mbm_price_comparison 
            WHERE "Price Difference" < -50
            ORDER BY ABS("Price Difference") DESC
        ''',
        "business_value": "Margin improvement without losing competitiveness"
    },
    "competitive_threat": {
        "name": "Competitive Threat Analysis",
        "description": "Category-level competitive intelligence",
        "sql": '''
            SELECT "Category", "Seller Name",
                   COUNT(*) as products_compared,
                   COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END) as they_beat_us,
                   COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END) as we_beat_them,
                   ROUND(AVG("Price Difference"::numeric)/100, 2) as avg_difference_pounds,
                   ROUND(AVG(CASE WHEN "Price Difference" > 0 THEN "Price Difference" END)::numeric/100, 2) as avg_we_lose_by
            FROM public.mbm_price_comparison 
            GROUP BY "Category", "Seller Name"
            HAVING COUNT(*) >= 5
            ORDER BY they_beat_us DESC, avg_we_lose_by DESC
        ''',
        "business_value": "Strategic competitive positioning insights"
    }
}

def get_supabase_data(sql_query: str = None, table: str = "mbm_price_comparison", limit: int = None):
    """Get data from Supabase using REST API"""
    try:
        supabase_url = os.getenv('SUPABASE_URL', 'https://fbiqlsoheofdmgqmjxfc.supabase.co')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_key:
            raise Exception("Missing SUPABASE_ANON_KEY environment variable")
        
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        clean_table = table.replace('public.', '').replace('`', '').replace('"', '')
        url = f"{supabase_url}/rest/v1/{clean_table}"
        
        params = {}
        if limit:
            params['limit'] = limit
        else:
            params['limit'] = 5000  # Increased default limit for reports
            
        logger.info(f"Making request to: {url} with params: {params}")
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Received {len(data)} records from Supabase")
            return pd.DataFrame(data)
        else:
            error_msg = f"Supabase API error: {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        logger.error(f"Supabase REST API failed: {str(e)}")
        raise

def generate_report_analytics(df: pd.DataFrame, report_type: str) -> Dict[str, Any]:
    """Generate comprehensive analytics for the report data"""
    analytics = {
        "summary": {
            "total_products": len(df),
            "generated_at": datetime.now().isoformat(),
            "report_type": report_type
        }
    }
    
    try:
        if report_type == "overpriced":
            analytics["insights"] = {
                "total_overpriced_products": len(df),
                "total_revenue_at_risk": round(df['overpricing_pounds'].sum(), 2) if 'overpricing_pounds' in df.columns else 0,
                "avg_overpricing": round(df['overpricing_pounds'].mean(), 2) if 'overpricing_pounds' in df.columns else 0,
                "max_overpricing": round(df['overpricing_pounds'].max(), 2) if 'overpricing_pounds' in df.columns else 0,
                "categories_affected": df['Category'].nunique() if 'Category' in df.columns else 0,
                "top_problem_categories": df.groupby('Category')['overpricing_pounds'].sum().head(5).round(2).to_dict() if 'Category' in df.columns and 'overpricing_pounds' in df.columns else {}
            }
            
        elif report_type == "ranking_issues":
            analytics["insights"] = {
                "products_with_ranking_issues": len(df),
                "avg_ranking_gap": round(df['ranking_gap'].mean(), 2) if 'ranking_gap' in df.columns else 0,
                "max_ranking_gap": df['ranking_gap'].max() if 'ranking_gap' in df.columns else 0,
                "total_price_advantage": round(df['price_advantage_pounds'].sum(), 2) if 'price_advantage_pounds' in df.columns else 0,
                "categories_with_issues": df['Category'].nunique() if 'Category' in df.columns else 0
            }
            
        elif report_type == "price_increase":
            analytics["insights"] = {
                "products_for_increase": len(df),
                "total_potential_revenue": round(df['potential_increase'].sum(), 2) if 'potential_increase' in df.columns else 0,
                "avg_potential_increase": round(df['potential_increase'].mean(), 2) if 'potential_increase' in df.columns else 0,
                "max_potential_increase": round(df['potential_increase'].max(), 2) if 'potential_increase' in df.columns else 0,
                "categories_with_opportunities": df['Category'].nunique() if 'Category' in df.columns else 0
            }
            
        elif report_type == "competitive_threat":
            analytics["insights"] = {
                "competitors_analyzed": len(df),
                "total_products_compared": df['products_compared'].sum() if 'products_compared' in df.columns else 0,
                "categories_analyzed": df['Category'].nunique() if 'Category' in df.columns else 0,
                "avg_competitive_gap": round(df['avg_difference_pounds'].mean(), 2) if 'avg_difference_pounds' in df.columns else 0
            }
            
    except Exception as e:
        logger.warning(f"Analytics generation partially failed: {str(e)}")
        analytics["analytics_error"] = str(e)
    
    return analytics

def create_enhanced_csv(df: pd.DataFrame, analytics: Dict[str, Any], report_config: Dict[str, str]) -> str:
    """Create an enhanced CSV with metadata and analytics"""
    csv_buffer = io.StringIO()
    
    # Write report metadata
    csv_buffer.write(f"# {report_config['name']}\n")
    csv_buffer.write(f"# {report_config['description']}\n") 
    csv_buffer.write(f"# Business Value: {report_config['business_value']}\n")
    csv_buffer.write(f"# Generated: {analytics['summary']['generated_at']}\n")
    csv_buffer.write(f"# Total Records: {analytics['summary']['total_products']}\n")
    csv_buffer.write("#\n")
    
    # Write analytics summary
    if 'insights' in analytics:
        csv_buffer.write("# KEY INSIGHTS:\n")
        for key, value in analytics['insights'].items():
            if isinstance(value, dict):
                csv_buffer.write(f"# {key.replace('_', ' ').title()}:\n")
                for sub_key, sub_value in list(value.items())[:5]:  # Top 5 only
                    csv_buffer.write(f"#   {sub_key}: {sub_value}\n")
            else:
                csv_buffer.write(f"# {key.replace('_', ' ').title()}: {value}\n")
        csv_buffer.write("#\n")
    
    csv_buffer.write("# DATA:\n")
    
    # Write the actual data
    df.to_csv(csv_buffer, index=False)
    
    return csv_buffer.getvalue()

# Initialize storage instance
def get_storage_instance():
    """Get configured storage instance"""
    anon_key = os.getenv('SUPABASE_ANON_KEY')
    bucket_name = os.getenv('SUPABASE_STORAGE_BUCKET', 'reports')
    enabled = os.getenv('ENABLE_STORAGE_UPLOADS', 'true').lower() == 'true'
    
    return SupabaseStorage(anon_key=anon_key, bucket_name=bucket_name, enabled=enabled)

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "MBM Pricing Analysis API",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "available_reports": list(REPORT_CONFIGS.keys()),
        "storage_enabled": bool(os.getenv('SUPABASE_ANON_KEY')),
        "endpoints": [
            "/generate-report/{report_type}",
            "/generate-csv",
            "/analyze", 
            "/test-db",
            "/test-storage",
            "/schema",
            "/reports/list",
            "/reports/download/{filename}",
            "/docs"
        ]
    }

@app.get("/test-storage")
async def test_storage():
    """Test Supabase storage connection"""
    try:
        storage = get_storage_instance()
        success = storage.test_storage_connection()
        
        return {
            "success": success,
            "message": "Storage connection test completed",
            "storage_enabled": storage.enabled,
            "bucket_name": storage.bucket_name,
            "rest_url": storage.rest_url,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Storage test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Storage test failed: {str(e)}")

@app.get("/reports/list")
async def list_reports():
    """List available report types"""
    return {
        "available_reports": {
            report_type: {
                "name": config["name"],
                "description": config["description"],
                "business_value": config["business_value"]
            }
            for report_type, config in REPORT_CONFIGS.items()
        }
    }

@app.post("/generate-report/{report_type}")
async def generate_report(report_type: str, include_analytics: bool = True, upload_to_storage: bool = True):
    """Generate a comprehensive report with analytics and optional storage upload"""
    try:
        if report_type not in REPORT_CONFIGS:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid report type. Available: {list(REPORT_CONFIGS.keys())}"
            )
        
        report_config = REPORT_CONFIGS[report_type]
        logger.info(f"Generating {report_config['name']}")
        
        # Execute the predefined SQL query
        df = get_supabase_data(sql_query=report_config["sql"], table="mbm_price_comparison", limit=10000)
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="Report query returned no results")
        
        # Generate analytics
        analytics = {}
        if include_analytics:
            analytics = generate_report_analytics(df, report_type)
        
        # Create enhanced CSV content
        csv_content = create_enhanced_csv(df, analytics, report_config)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mbm_{report_type}_report_{timestamp}.csv"
        
        storage_result = {}
        public_url = None
        
        if upload_to_storage:
            storage = get_storage_instance()
            public_url = storage.upload_file_content(csv_content, filename)
            storage_result = {
                "success": bool(public_url),
                "filename": filename,
                "public_url": public_url,
                "file_size": len(csv_content.encode('utf-8')),
                "bucket": storage.bucket_name
            }
        
        # Prepare response
        response_data = {
            "success": True,
            "report_type": report_type,
            "report_name": report_config["name"],
            "report_description": report_config["description"],
            "business_value": report_config["business_value"],
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "filename": filename,
            "generated_at": datetime.now().isoformat(),
            "analytics": analytics if include_analytics else None,
            "storage_upload": storage_result if upload_to_storage else None,
            "download_url": f"/reports/download/{filename}" if public_url else None
        }
        
        # Add sample data (first 3 rows)
        if len(df) > 0:
            response_data["sample_data"] = df.head(3).to_dict('records')
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")

@app.get("/reports/download/{filename}")
async def download_report(filename: str):
    """Download a report file from Supabase Storage or return redirect to public URL"""
    try:
        storage = get_storage_instance()
        bucket_name = storage.bucket_name
        
        # Construct public URL
        public_url = f"{storage.rest_url}/storage/v1/object/public/{bucket_name}/{filename}"
        
        # Test if file exists by making a HEAD request
        response = requests.head(public_url, timeout=10)
        
        if response.status_code == 200:
            # File exists, redirect to public URL
            return JSONResponse(
                content={
                    "success": True,
                    "filename": filename,
                    "public_url": public_url,
                    "download_link": public_url,
                    "message": "File is available for download"
                }
            )
        else:
            raise HTTPException(
                status_code=404, 
                detail=f"File not found: {filename}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File download failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File download failed: {str(e)}")

# Keep existing endpoints for backward compatibility
def parse_simple_sql(sql_query: str):
    """Parse basic SQL queries to REST API parameters"""
    sql_lower = sql_query.lower().strip()
    
    table = "mbm_price_comparison"
    if "from" in sql_lower:
        parts = sql_lower.split("from")
        if len(parts) > 1:
            table_part = parts[1].strip().split()[0]
            if "." in table_part:
                table = table_part.split(".")[-1]
            else:
                table = table_part
    
    limit = None
    if "limit" in sql_lower:
        limit_parts = sql_lower.split("limit")
        if len(limit_parts) > 1:
            limit_str = limit_parts[-1].strip().split()[0]
            try:
                limit = int(limit_str)
            except ValueError:
                pass
    
    return table, limit

def validate_sql(sql_query: str) -> tuple[bool, str]:
    """Validate that the SQL query is safe and well-formed"""
    if not sql_query:
        return False, "No SQL query provided"
    
    sql_query = sql_query.strip()
    
    if not sql_query.lower().startswith('select'):
        return False, "Only SELECT queries allowed"
    
    dangerous_keywords = ['drop', 'delete', 'update', 'insert', 'alter', 'create', 'truncate', 'exec']
    sql_lower = sql_query.lower()
    
    for keyword in dangerous_keywords:
        if f' {keyword} ' in sql_lower or sql_lower.startswith(f'{keyword} '):
            return False, f"Dangerous SQL keyword detected: {keyword}"
    
    return True, sql_query

@app.get("/test-db")
async def test_database():
    """Test Supabase REST API connection"""
    try:
        supabase_url = os.getenv('SUPABASE_URL', 'https://fbiqlsoheofdmgqmjxfc.supabase.co')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_key:
            raise HTTPException(status_code=500, detail="Missing SUPABASE_ANON_KEY environment variable")
        
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}'
        }
        
        available_info = {
            "supabase_url": supabase_url,
            "connection_method": "REST API",
            "storage_configured": bool(os.getenv('SUPABASE_STORAGE_BUCKET'))
        }
        
        possible_tables = ["mbm_price_comparison", "mbm_pricing", "pricing", "price_comparison"]
        
        for table_name in possible_tables:
            try:
                url = f"{supabase_url}/rest/v1/{table_name}?limit=1"
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "success": True,
                        "message": "Supabase REST API connection successful",
                        "table": table_name,
                        "sample_record_exists": len(data) > 0,
                        "sample_data": data[0] if len(data) > 0 else None,
                        "timestamp": datetime.now().isoformat(),
                        **available_info
                    }
                else:
                    available_info[f"{table_name}_error"] = f"{response.status_code}: {response.text[:100]}"
                    
            except Exception as e:
                available_info[f"{table_name}_exception"] = str(e)[:100]
        
        return {
            "success": False,
            "message": "Could not find accessible tables",
            "timestamp": datetime.now().isoformat(),
            "debug_info": available_info,
            "suggestion": "Check table names in Supabase dashboard and verify API permissions"
        }
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database test failed: {str(e)}")

@app.post("/generate-csv")
async def generate_csv(request: SQLRequest):
    """Generate and download CSV using Supabase REST API (legacy endpoint)"""
    try:
        is_valid, validation_message = validate_sql(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Executing query via REST API: {request.sql_query[:100]}...")
        
        table, limit = parse_simple_sql(request.sql_query)
        df = get_supabase_data(sql_query=request.sql_query, table=table, limit=limit)
        
        logger.info(f"Query returned {len(df)} rows")
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="Query returned no results")
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mbm_pricing_export_{timestamp}.csv"
        
        logger.info(f"Generated CSV with {len(df)} rows, filename: {filename}")
        
        return StreamingResponse(
            io.BytesIO(csv_buffer.getvalue().encode('utf-8')),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "X-Row-Count": str(len(df)),
                "X-Columns": str(len(df.columns)),
                "X-Query-Executed": "true"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"CSV generation failed: {str(e)}")

@app.post("/analyze")
async def analyze_data(request: SQLRequest):
    """Get data analysis using Supabase REST API (legacy endpoint)"""
    try:
        is_valid, validation_message = validate_sql(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Analyzing data via REST API: {request.sql_query[:100]}...")
        
        table, limit = parse_simple_sql(request.sql_query)
        df = get_supabase_data(sql_query=request.sql_query, table=table, limit=limit)
        
        logger.info(f"Analysis query returned {len(df)} rows")
        
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        summary_stats = {}
        
        if numeric_columns:
            summary_stats = df[numeric_columns].describe().to_dict()
        
        return {
            "success": True,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "numeric_columns": numeric_columns,
            "sql_executed": request.sql_query,
            "sample_data": df.head(5).to_dict('records') if len(df) > 0 else [],
            "summary_statistics": summary_stats,
            "timestamp": datetime.now().isoformat(),
            "message": f"Analysis complete: {len(df)} rows processed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Data analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data analysis failed: {str(e)}")

@app.get("/schema")
async def get_table_schema():
    """Get basic table info using REST API"""
    try:
        supabase_url = os.getenv('SUPABASE_URL', 'https://fbiqlsoheofdmgqmjxfc.supabase.co')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_key:
            raise HTTPException(status_code=500, detail="Missing Supabase configuration")
        
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}'
        }
        
        url = f"{supabase_url}/rest/v1/mbm_price_comparison?limit=1"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                columns = list(data[0].keys())
                return {
                    "success": True,
                    "table": "mbm_price_comparison",
                    "columns": columns,
                    "column_count": len(columns),
                    "sample_data": data[0],
                    "timestamp": datetime.now().isoformat(),
                    "note": "Schema inferred from sample data via REST API"
                }
            else:
                return {
                    "success": True,
                    "table": "mbm_price_comparison",
                    "columns": [],
                    "column_count": 0,
                    "message": "Table exists but is empty"
                }
        else:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch schema: {response.text}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Schema fetch failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Schema fetch failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
