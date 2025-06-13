from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import psycopg2
import os
from datetime import datetime
import io
from pydantic import BaseModel
import logging

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

def get_db_connection():
    """Get database connection to Supabase"""
    try:
        return psycopg2.connect(
            host=os.getenv('SUPABASE_HOST'),
            port=int(os.getenv('SUPABASE_PORT', 6543)),
            database=os.getenv('SUPABASE_DB'),
            user=os.getenv('SUPABASE_USER'),
            password=os.getenv('SUPABASE_PASSWORD')
        )
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

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
            "/docs"
        ]
    }

@app.get("/test-db")
async def test_database():
    """Test database connection and verify data"""
    try:
        # Check environment variables
        required_vars = ['SUPABASE_HOST', 'SUPABASE_DB', 'SUPABASE_USER', 'SUPABASE_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise HTTPException(
                status_code=500, 
                detail=f"Missing environment variables: {missing_vars}"
            )
        
        # Test connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Test query on the pricing table
        cursor.execute("SELECT COUNT(*) FROM public.mbm_price_comparison")
        count = cursor.fetchone()[0]
        
        # Get sample data structure
        cursor.execute("SELECT * FROM public.mbm_price_comparison LIMIT 1")
        sample = cursor.fetchone()
        
        # Get column names
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'mbm_price_comparison' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return {
            "success": True,
            "message": "Database connection successful",
            "table": "public.mbm_price_comparison",
            "total_records": count,
            "columns": columns,
            "sample_data_exists": sample is not None,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database test failed: {str(e)}")

@app.post("/generate-csv")
async def generate_csv(request: SQLRequest):
    """Generate and download CSV from SQL query"""
    try:
        # Validate SQL
        is_valid, validation_message = validate_sql(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Executing SQL query: {request.sql_query[:100]}...")
        
        # Connect and execute
        conn = get_db_connection()
        df = pd.read_sql_query(request.sql_query, conn)
        conn.close()
        
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
    """Get data analysis without CSV download - useful for quick insights"""
    try:
        # Validate SQL
        is_valid, validation_message = validate_sql(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Analyzing data with query: {request.sql_query[:100]}...")
        
        # Connect and execute
        conn = get_db_connection()
        df = pd.read_sql_query(request.sql_query, conn)
        conn.close()
        
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
    """Get the schema information for the mbm_price_comparison table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get detailed column information
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = 'mbm_price_comparison' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == 'YES',
                "default": row[3]
            })
        
        conn.close()
        
        return {
            "success": True,
            "table": "public.mbm_price_comparison",
            "columns": columns,
            "column_count": len(columns),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Schema fetch failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Schema fetch failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
