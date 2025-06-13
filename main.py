from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import requests
import os
from datetime import datetime
import io
from pydantic import BaseModel
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MBM Pricing Analysis API",
    description="API for analyzing competitive pricing data from Supabase database",
    version="1.0.0"
)

class SQLRequest(BaseModel):
    sql_query: str

def get_supabase_data(sql_query: str = None, table: str = "mbm_price_comparison", limit: int = None):
    """Get data from Supabase using REST API"""
    try:
        # Supabase REST API configuration
        supabase_url = os.getenv('SUPABASE_URL')  # https://your-project.supabase.co
        supabase_key = os.getenv('SUPABASE_ANON_KEY')  # Your anon/public key
        
        if not supabase_url or not supabase_key:
            raise Exception("Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables")
        
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }
        
        # Build the REST API URL
        url = f"{supabase_url}/rest/v1/{table}"
        
        # Add query parameters
        params = {}
        if limit:
            params['limit'] = limit
            
        # For simple queries, we can add some basic filtering
        if sql_query and 'where' in sql_query.lower():
            # This is a simplified parser - for complex queries you'd need stored procedures
            pass
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            raise Exception(f"Supabase API error: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"Supabase REST API failed: {str(e)}")
        raise

def parse_simple_sql(sql_query: str):
    """Parse basic SQL queries to REST API parameters"""
    sql_lower = sql_query.lower().strip()
    
    # Extract table name
    table = "mbm_price_comparison"  # default
    if "from" in sql_lower:
        parts = sql_lower.split("from")
        if len(parts) > 1:
            table_part = parts[1].strip().split()[0]
            if "." in table_part:
                table = table_part.split(".")[-1]  # Get table name after schema
            else:
                table = table_part
    
    # Extract limit
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
    
    # Remove extra whitespace and normalize
    sql_query = sql_query.strip()
    
    # Must be a SELECT query
    if not sql_query.lower().startswith('select'):
        return False, "Only SELECT queries allowed"
    
    # Basic SQL injection protection
    dangerous_keywords = ['drop', 'delete', 'update', 'insert', 'alter', 'create', 'truncate', 'exec']
    sql_lower = sql_query.lower()
    
    for keyword in dangerous_keywords:
        if f' {keyword} ' in sql_lower or sql_lower.startswith(f'{keyword} '):
            return False, f"Dangerous SQL keyword detected: {keyword}"
    
    return True, sql_query

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "MBM Pricing Analysis API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "endpoints": [
            "/generate-csv",
            "/analyze", 
            "/test-db",
            "/schema",
            "/docs"
        ]
    }

@app.get("/test-db")
async def test_database():
    """Test Supabase REST API connection"""
    try:
        # Check environment variables
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_url:
            raise HTTPException(status_code=500, detail="Missing SUPABASE_URL environment variable")
        if not supabase_key:
            raise HTTPException(status_code=500, detail="Missing SUPABASE_ANON_KEY environment variable")
        
        # Test connection with a simple API call
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}'
        }
        
        url = f"{supabase_url}/rest/v1/mbm_price_comparison?limit=1"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # Get total count
            count_url = f"{supabase_url}/rest/v1/mbm_price_comparison?select=count"
            count_response = requests.get(count_url, headers={**headers, 'Prefer': 'count=exact'})
            total_count = "unknown"
            if count_response.status_code == 200:
                try:
                    total_count = len(count_response.json())
                except:
                    pass
            
            return {
                "success": True,
                "message": "Supabase REST API connection successful",
                "table": "mbm_price_comparison",
                "total_records": total_count,
                "sample_record_exists": len(data) > 0,
                "sample_data": data[0] if len(data) > 0 else None,
                "timestamp": datetime.now().isoformat(),
                "supabase_url": supabase_url,
                "connection_method": "REST API"
            }
        else:
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Supabase API error: {response.text}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database test failed: {str(e)}")

@app.post("/generate-csv")
async def generate_csv(request: SQLRequest):
    """Generate and download CSV using Supabase REST API"""
    try:
        # Validate SQL
        is_valid, validation_message = validate_sql(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Executing query via REST API: {request.sql_query[:100]}...")
        
        # Parse the SQL to extract table and limit
        table, limit = parse_simple_sql(request.sql_query)
        
        # Get data using REST API
        df = get_supabase_data(sql_query=request.sql_query, table=table, limit=limit)
        
        logger.info(f"Query returned {len(df)} rows")
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="Query returned no results")
        
        # Generate CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0)
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mbm_pricing_export_{timestamp}.csv"
        
        logger.info(f"Generated CSV with {len(df)} rows, filename: {filename}")
        
        # Return CSV as download
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
    """Get data analysis using Supabase REST API"""
    try:
        # Validate SQL
        is_valid, validation_message = validate_sql(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Analyzing data via REST API: {request.sql_query[:100]}...")
        
        # Parse the SQL to extract table and limit
        table, limit = parse_simple_sql(request.sql_query)
        
        # Get data using REST API
        df = get_supabase_data(sql_query=request.sql_query, table=table, limit=limit)
        
        logger.info(f"Analysis query returned {len(df)} rows")
        
        # Generate summary statistics for numeric columns
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
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_url or not supabase_key:
            raise HTTPException(status_code=500, detail="Missing Supabase configuration")
        
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}'
        }
        
        # Get a sample record to understand the structure
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
