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
import hashlib
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dynamic Pricing Intelligence Report API",
    description="Dynamic CSV report generation API for competitive pricing intelligence with automated storage",
    version="3.0.0"
)

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

class SupabaseStorage:
    """Supabase storage operations for dynamic reports"""

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
        """Check if bucket exists using the correct API format"""
        if not self.enabled or not self.anon_key or not self.rest_url:
            logger.debug("Storage uploads disabled or missing credentials.")
            return False
        
        try:
            # Test bucket access by trying to list objects with correct format
            test_url = f"{self.rest_url}/storage/v1/object/list/{self.bucket_name}"
            headers = {
                "apikey": self.anon_key,
                "Authorization": f"Bearer {self.anon_key}",
                "Content-Type": "application/json"
            }
            
            # Use the correct body format with required 'prefix' property
            body = {
                "limit": 1,
                "prefix": ""  # Empty prefix to list any files
            }
            
            response = requests.post(test_url, headers=headers, json=body, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"Bucket '{self.bucket_name}' is accessible")
                return True
            elif response.status_code == 404:
                logger.error(f"Bucket '{self.bucket_name}' does not exist. Please create it in your Supabase dashboard.")
                return False
            else:
                logger.warning(f"Bucket check returned {response.status_code}: {response.text}")
                # If it's not a clear 404, assume bucket exists and try upload anyway
                return True
                
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
            logger.info(f"Uploading file: {file_name} ({len(file_data)} bytes)")
            
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
            else:
                logger.info(f"Successfully cleaned up test file: {filename}")
        except Exception as e:
            logger.warning(f"Could not delete test file: {str(e)}")
        
        return True

def execute_sql_directly_supabase(sql_query: str) -> pd.DataFrame:
    """Execute SQL directly against Supabase PostgreSQL with proper SSL and connection handling"""
    try:
        # Build connection parameters
        host = os.getenv('SUPABASE_HOST', 'aws-0-eu-west-2.pooler.supabase.com')
        database = os.getenv('SUPABASE_DB', 'postgres') 
        user = os.getenv('SUPABASE_USER', 'postgres.fbiqlsoheofdmgqmjxfc')
        password = os.getenv('SUPABASE_PASSWORD')
        port = os.getenv('SUPABASE_PORT', '6543')  # Changed from 5432 to 6543 for pooler
        
        if not password:
            raise Exception("SUPABASE_PASSWORD environment variable is required")
        
        # URL encode the password to handle special characters
        from urllib.parse import quote_plus
        encoded_password = quote_plus(password)
        
        # Create connection string with SSL and proper pooler configuration
        conn_string = f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}?sslmode=require&connect_timeout=60&application_name=pricing_api"
        
        logger.info("🚀 Attempting direct PostgreSQL execution...")
        logger.info(f"Host: {host}:{port}")
        logger.info(f"Database: {database}")
        logger.info(f"User: {user}")
        logger.info(f"SQL Query: {sql_query[:150]}...")
        
        # Execute query using pandas with connection timeout
        df = pd.read_sql_query(
            sql_query, 
            conn_string,
            params=None
        )
        
        logger.info(f"✅ Direct SQL execution successful: {len(df)} rows returned")
        return df
        
    except Exception as e:
        logger.error(f"❌ Direct SQL execution failed: {str(e)}")
        logger.error(f"Connection details - Host: {host}:{port}, User: {user}, DB: {database}")
        raise

def execute_sql_with_sqlalchemy(sql_query: str) -> pd.DataFrame:
    """Alternative method using SQLAlchemy engine with proper Supabase configuration"""
    try:
        # Build connection parameters
        host = os.getenv('SUPABASE_HOST', 'aws-0-eu-west-2.pooler.supabase.com')
        database = os.getenv('SUPABASE_DB', 'postgres')
        user = os.getenv('SUPABASE_USER', 'postgres.fbiqlsoheofdmgqmjxfc')
        password = os.getenv('SUPABASE_PASSWORD')
        port = os.getenv('SUPABASE_PORT', '6543')  # Use pooler port
        
        if not password:
            raise Exception("SUPABASE_PASSWORD environment variable is required")
        
        # URL encode the password
        from urllib.parse import quote_plus
        encoded_password = quote_plus(password)
        
        # Create SQLAlchemy engine with Supabase-specific settings
        engine = create_engine(
            f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            pool_recycle=300,
            pool_timeout=60,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 60,
                "application_name": "pricing_api_sqlalchemy"
            }
        )
        
        logger.info("🔧 Attempting SQLAlchemy execution...")
        logger.info(f"SQL Query: {sql_query[:150]}...")
        
        # Execute query with timeout
        with engine.connect() as connection:
            df = pd.read_sql_query(sql_query, connection)
        
        logger.info(f"✅ SQLAlchemy execution successful: {len(df)} rows returned")
        return df
        
    except Exception as e:
        logger.error(f"❌ SQLAlchemy execution failed: {str(e)}")
        logger.error(f"Connection details - Host: {host}:{port}, User: {user}, DB: {database}")
        raise

def get_supabase_data_enhanced(sql_query: str) -> pd.DataFrame:
    """
    Enhanced SQL execution with multiple fallback methods
    This replaces get_supabase_data_dynamic
    """
    
    # Method 1: Direct PostgreSQL with pandas (PREFERRED)
    try:
        logger.info("🎯 Method 1: Trying direct PostgreSQL with pandas...")
        return execute_sql_directly_supabase(sql_query)
    except Exception as e:
        logger.warning(f"Method 1 failed: {str(e)}")
    
    # Method 2: SQLAlchemy engine (BACKUP)
    try:
        logger.info("🎯 Method 2: Trying SQLAlchemy engine...")
        return execute_sql_with_sqlalchemy(sql_query)
    except Exception as e:
        logger.warning(f"Method 2 failed: {str(e)}")
    
    # Method 3: Fallback to your existing REST API method (LAST RESORT)
    try:
        logger.info("🎯 Method 3: Falling back to REST API...")
        return get_supabase_data_dynamic(sql_query)  # Your existing function
    except Exception as e:
        logger.error(f"All methods failed. Final error: {str(e)}")
        raise Exception(f"Unable to execute SQL query with any available method: {str(e)}")

def test_direct_connection() -> bool:
    """Test the direct PostgreSQL connection with proper error logging"""
    try:
        test_query = "SELECT COUNT(*) as total_rows FROM public.mbm_price_comparison LIMIT 1"
        df = execute_sql_directly_supabase(test_query)
        
        if len(df) > 0 and 'total_rows' in df.columns:
            total_rows = df['total_rows'].iloc[0]
            logger.info(f"✅ Connection test successful! Database has {total_rows} rows")
            return True
        else:
            logger.error("❌ Connection test failed: No data returned")
            return False
            
    except Exception as e:
        logger.error(f"❌ Connection test failed with detailed error: {str(e)}")
        
        # Additional diagnostic information
        logger.error("🔍 Environment variables check:")
        logger.error(f"SUPABASE_HOST: {os.getenv('SUPABASE_HOST', 'NOT SET')}")
        logger.error(f"SUPABASE_DB: {os.getenv('SUPABASE_DB', 'NOT SET')}")
        logger.error(f"SUPABASE_USER: {os.getenv('SUPABASE_USER', 'NOT SET')}")
        logger.error(f"SUPABASE_PASSWORD: {'SET' if os.getenv('SUPABASE_PASSWORD') else 'NOT SET'}")
        logger.error(f"SUPABASE_PORT: {os.getenv('SUPABASE_PORT', 'NOT SET (defaulting to 5432)')}")
        
        return False

def get_supabase_data_dynamic(sql_query: str) -> pd.DataFrame:
    """Execute dynamic SQL query against Supabase using REST API with intelligent table detection"""
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
        
        # Extract table name from SQL query
        table_name = extract_table_from_sql(sql_query)
        if not table_name:
            table_name = "mbm_price_comparison"
        
        logger.info(f"Executing dynamic query on table: {table_name}")
        logger.info(f"SQL Query: {sql_query[:200]}...")
        
        # Check if this is a simple SELECT * query
        sql_lower = sql_query.lower().strip()
        is_simple_query = (
            sql_lower.startswith('select *') and 
            'where' not in sql_lower and 
            'group by' not in sql_lower and
            'having' not in sql_lower and
            '(' not in sql_lower
        )
        
        if is_simple_query:
            # Use REST API parameters for simple queries
            rest_params = convert_sql_to_rest_params(sql_query)
            base_url = f"{supabase_url}/rest/v1/{table_name}"
            
            all_data = []
            page_size = 1000
            offset = 0
            
            while True:
                params = {
                    'limit': page_size,
                    'offset': offset,
                    **rest_params
                }
                
                logger.info(f"Fetching rows {offset} to {offset + page_size}")
                
                response = requests.get(base_url, headers=headers, params=params)
                
                if response.status_code == 200:
                    page_data = response.json()
                    
                    if not page_data:
                        break
                        
                    all_data.extend(page_data)
                    logger.info(f"Retrieved {len(page_data)} rows (total so far: {len(all_data)})")
                    
                    if len(page_data) < page_size:
                        break
                        
                    offset += page_size
                    
                else:
                    error_msg = f"Supabase API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        
        else:
            # For complex queries, get all data first then filter in Python
            logger.info("Complex query detected - fetching all data for Python filtering")
            base_url = f"{supabase_url}/rest/v1/{table_name}"
            
            all_data = []
            page_size = 1000
            offset = 0
            
            while True:
                params = {
                    'limit': page_size,
                    'offset': offset
                }
                
                logger.info(f"Fetching rows {offset} to {offset + page_size}")
                
                response = requests.get(base_url, headers=headers, params=params)
                
                if response.status_code == 200:
                    page_data = response.json()
                    
                    if not page_data:
                        break
                        
                    all_data.extend(page_data)
                    logger.info(f"Retrieved {len(page_data)} rows (total so far: {len(all_data)})")
                    
                    if len(page_data) < page_size:
                        break
                        
                    offset += page_size
                    
                else:
                    error_msg = f"Supabase API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        
        logger.info(f"Completed data fetch: {len(all_data)} total records")
        df = pd.DataFrame(all_data)
        
        # Apply SQL transformations if this was a complex query
        if not is_simple_query:
            df = apply_sql_transformations(df, sql_query)
        
        return df
            
    except Exception as e:
        logger.error(f"Dynamic SQL execution failed: {str(e)}")
        raise

def extract_table_from_sql(sql_query: str) -> str:
    """Extract table name from SQL query"""
    try:
        sql_lower = sql_query.lower().strip()
        
        # Look for FROM clause
        from_match = re.search(r'\bfrom\s+(?:public\.)?([a-zA-Z_][a-zA-Z0-9_]*)', sql_lower)
        if from_match:
            table_name = from_match.group(1)
            # Clean table name
            table_name = table_name.replace('`', '').replace('"', '').replace('public.', '')
            return table_name
        
        # If no FROM found, return default
        return "mbm_price_comparison"
        
    except Exception as e:
        logger.warning(f"Could not extract table name: {str(e)}")
        return "mbm_price_comparison"

def convert_sql_to_rest_params(sql_query: str) -> Dict[str, Any]:
    """Convert basic SQL operations to Supabase REST API parameters - CASE SENSITIVE"""
    params = {}
    sql_original = sql_query.strip()  # Keep original case
    sql_lower = sql_query.lower().strip()  # Only use lowercase for pattern matching
    
    try:
        # Handle SELECT columns - preserve original case
        select_match = re.search(r'select\s+(.*?)\s+from', sql_lower, re.DOTALL)
        if select_match:
            # Get the original case version of the select clause
            select_start = select_match.start(1)
            select_end = select_match.end(1)
            # Find the corresponding position in the original query
            from_pos = sql_original.lower().find(' from ')
            if from_pos > 0:
                select_original = sql_original[select_original.lower().find('select') + 6:from_pos].strip()
                
                if select_original != '*' and 'distinct' not in select_original.lower():
                    # Extract column names preserving case - only if no functions
                    if '(' not in select_original and 'round' not in select_original.lower():
                        columns = [col.strip().replace('"', '') for col in select_original.split(',')]
                        if columns and len(columns) <= 20:  # Limit to first 20 columns
                            params['select'] = ','.join(columns[:20])
        
        # Handle ORDER BY - preserve case
        order_match = re.search(r'order\s+by\s+([^;]+)', sql_lower)
        if order_match:
            # Find original case version
            order_start = sql_original.lower().find('order by') + 8
            order_clause = sql_original[order_start:].split(';')[0].strip()
            
            # Basic order conversion (first column only)
            order_parts = order_clause.split(',')[0].strip().split()
            if len(order_parts) >= 1:
                column = order_parts[0].replace('"', '').replace('`', '')
                direction = 'desc' if len(order_parts) > 1 and 'desc' in order_parts[1].lower() else 'asc'
                params['order'] = f"{column}.{direction}"
        
        # Handle LIMIT
        limit_match = re.search(r'limit\s+(\d+)', sql_lower)
        if limit_match:
            params['limit'] = min(int(limit_match.group(1)), 5000)  # Cap at 5000
        
        return params
        
    except Exception as e:
        logger.warning(f"SQL to REST conversion failed: {str(e)}, using basic params")
        return {}

def apply_sql_transformations(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    """Apply SQL transformations that couldn't be handled by REST API"""
    try:
        sql_lower = sql_query.lower().strip()
        
        # Handle WHERE clauses
        if 'where' in sql_lower:
            df = apply_where_filters(df, sql_query)
        
        # Handle calculated columns (basic support)
        if 'case when' in sql_lower or 'round(' in sql_lower:
            df = add_calculated_columns(df, sql_query)
        
        # Handle GROUP BY (basic support)
        if 'group by' in sql_lower:
            df = apply_group_by(df, sql_query)
        
        return df
        
    except Exception as e:
        logger.warning(f"SQL transformation failed: {str(e)}, returning original data")
        return df

def apply_where_filters(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    """Apply WHERE clause filters to DataFrame"""
    try:
        sql_upper = sql_query.upper()
        
        # Common pricing filters
        if '"PRICE DIFFERENCE" > 0' in sql_upper and 'Price Difference' in df.columns:
            df = df[df['Price Difference'] > 0]
        elif '"PRICE DIFFERENCE" < 0' in sql_upper and 'Price Difference' in df.columns:
            df = df[df['Price Difference'] < 0]
        elif '"PRICE DIFFERENCE" < -50' in sql_upper and 'Price Difference' in df.columns:
            df = df[df['Price Difference'] < -50]
        
        # Ranking filters
        if '"YOUR PRICE RANK" > "COMPETITOR PRICE RANK"' in sql_upper:
            if 'Your Price Rank' in df.columns and 'Competitor Price Rank' in df.columns:
                df = df[df['Your Price Rank'] > df['Competitor Price Rank']]
        
        return df
        
    except Exception as e:
        logger.warning(f"WHERE filter application failed: {str(e)}")
        return df

def add_calculated_columns(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    """Add calculated columns based on SQL query"""
    try:
        # Add common calculated columns for pricing analysis
        if 'Price Difference' in df.columns:
            if 'overpricing_pounds' not in df.columns:
                df['overpricing_pounds'] = df['Price Difference'] / 100
            
            if 'Your Price' in df.columns and 'overpricing_percentage' not in df.columns:
                df['overpricing_percentage'] = (df['Price Difference'] / df['Your Price'] * 100).round(1)
        
        if 'Your Price Rank' in df.columns and 'Competitor Price Rank' in df.columns:
            if 'ranking_gap' not in df.columns:
                df['ranking_gap'] = df['Your Price Rank'] - df['Competitor Price Rank']
        
        return df
        
    except Exception as e:
        logger.warning(f"Calculated column addition failed: {str(e)}")
        return df

def apply_group_by(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    """Apply GROUP BY operations (basic support)"""
    try:
        # This is a simplified implementation
        # For complex GROUP BY operations, consider using PostgreSQL directly
        sql_lower = sql_query.lower()
        
        if 'group by' in sql_lower and 'count(' in sql_lower:
            # Extract group by columns (basic implementation)
            group_match = re.search(r'group\s+by\s+([^order\s]+)', sql_lower)
            if group_match:
                group_cols = [col.strip().replace('"', '') for col in group_match.group(1).split(',')]
                group_cols = [col for col in group_cols if col in df.columns]
                
                if group_cols:
                    # Basic aggregation
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    agg_dict = {col: 'count' for col in numeric_cols[:5]}  # Limit aggregations
                    
                    if agg_dict:
                        df = df.groupby(group_cols).agg(agg_dict).reset_index()
        
        return df
        
    except Exception as e:
        logger.warning(f"GROUP BY application failed: {str(e)}")
        return df

def generate_dynamic_analytics(df: pd.DataFrame, sql_query: str, report_name: str) -> Dict[str, Any]:
    """Generate analytics for dynamic reports"""
    analytics = {
        "summary": {
            "total_records": len(df),
            "generated_at": datetime.now().isoformat(),
            "report_name": report_name,
            "sql_query_hash": hashlib.md5(sql_query.encode()).hexdigest()[:8]
        }
    }
    
    try:
        # Basic data insights
        analytics["data_insights"] = {
            "columns": list(df.columns),
            "column_count": len(df.columns),
            "data_types": df.dtypes.astype(str).to_dict(),
            "null_counts": df.isnull().sum().to_dict(),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        }
        
        # Numeric column insights
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            analytics["numeric_insights"] = {}
            for col in numeric_cols[:10]:  # Limit to first 10 numeric columns
                analytics["numeric_insights"][col] = {
                    "mean": round(df[col].mean(), 2) if pd.notna(df[col].mean()) else None,
                    "median": round(df[col].median(), 2) if pd.notna(df[col].median()) else None,
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "std": round(df[col].std(), 2) if pd.notna(df[col].std()) else None
                }
        
        # Categorical insights
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        if categorical_cols:
            analytics["categorical_insights"] = {}
            for col in categorical_cols[:5]:  # Limit to first 5 categorical columns
                value_counts = df[col].value_counts().head(10)
                analytics["categorical_insights"][col] = {
                    "unique_count": df[col].nunique(),
                    "top_values": value_counts.to_dict()
                }
        
        # Pricing-specific insights (if pricing columns detected)
        pricing_columns = [col for col in df.columns if any(keyword in col.lower() for keyword in ['price', 'cost', 'amount', 'revenue'])]
        if pricing_columns:
            analytics["pricing_insights"] = {}
            for col in pricing_columns[:5]:
                if pd.api.types.is_numeric_dtype(df[col]):
                    analytics["pricing_insights"][col] = {
                        "total": round(df[col].sum(), 2),
                        "average": round(df[col].mean(), 2),
                        "range": round(df[col].max() - df[col].min(), 2)
                    }
        
    except Exception as e:
        logger.warning(f"Analytics generation partially failed: {str(e)}")
        analytics["analytics_error"] = str(e)
    
    return analytics

def create_dynamic_enhanced_csv(df: pd.DataFrame, analytics: Dict[str, Any], report_name: str, description: str, business_value: str, sql_query: str) -> str:
    """Create an enhanced CSV with metadata and analytics for dynamic reports"""
    csv_buffer = io.StringIO()
    
    # Write report metadata
    csv_buffer.write(f"# {report_name}\n")
    csv_buffer.write(f"# {description}\n") 
    csv_buffer.write(f"# Business Value: {business_value}\n")
    csv_buffer.write(f"# Generated: {analytics['summary']['generated_at']}\n")
    csv_buffer.write(f"# Total Records: {analytics['summary']['total_records']}\n")
    csv_buffer.write(f"# SQL Query Hash: {analytics['summary']['sql_query_hash']}\n")
    csv_buffer.write("#\n")
    
    # Write SQL query (first 500 chars)
    csv_buffer.write("# EXECUTED SQL QUERY:\n")
    sql_preview = sql_query.replace('\n', ' ').strip()[:500]
    csv_buffer.write(f"# {sql_preview}{'...' if len(sql_query) > 500 else ''}\n")
    csv_buffer.write("#\n")
    
    # Write analytics summary
    if 'data_insights' in analytics:
        csv_buffer.write("# DATA INSIGHTS:\n")
        csv_buffer.write(f"# Columns: {analytics['data_insights']['column_count']}\n")
        csv_buffer.write(f"# Memory Usage: {analytics['data_insights']['memory_usage_mb']} MB\n")
        
        # Top null columns
        null_counts = analytics['data_insights']['null_counts']
        top_nulls = sorted([(k, v) for k, v in null_counts.items() if v > 0], key=lambda x: x[1], reverse=True)[:3]
        if top_nulls:
            csv_buffer.write("# Columns with missing data:\n")
            for col, count in top_nulls:
                csv_buffer.write(f"#   {col}: {count} missing values\n")
        csv_buffer.write("#\n")
    
    # Write numeric insights
    if 'numeric_insights' in analytics:
        csv_buffer.write("# NUMERIC COLUMN INSIGHTS:\n")
        for col, stats in list(analytics['numeric_insights'].items())[:3]:
            csv_buffer.write(f"# {col}: Mean={stats.get('mean', 'N/A')}, Range={stats.get('min', 'N/A')}-{stats.get('max', 'N/A')}\n")
        csv_buffer.write("#\n")
    
    # Write pricing insights if available
    if 'pricing_insights' in analytics:
        csv_buffer.write("# PRICING INSIGHTS:\n")
        for col, stats in analytics['pricing_insights'].items():
            csv_buffer.write(f"# {col}: Total={stats.get('total', 'N/A')}, Average={stats.get('average', 'N/A')}\n")
        csv_buffer.write("#\n")
    
    csv_buffer.write("# DATA:\n")
    
    # Write the actual data
    df.to_csv(csv_buffer, index=False)
    
    return csv_buffer.getvalue()

def get_storage_instance():
    """Get configured storage instance"""
    anon_key = os.getenv('SUPABASE_ANON_KEY')
    bucket_name = os.getenv('SUPABASE_STORAGE_BUCKET', 'reports')
    enabled = os.getenv('ENABLE_STORAGE_UPLOADS', 'true').lower() == 'true'
    
    return SupabaseStorage(anon_key=anon_key, bucket_name=bucket_name, enabled=enabled)

def validate_sql_query(sql_query: str) -> tuple[bool, str]:
    """Validate that the SQL query is safe for execution"""
    if not sql_query:
        return False, "No SQL query provided"
    
    sql_query = sql_query.strip()
    
    if not sql_query.lower().startswith('select'):
        return False, "Only SELECT queries are allowed"
    
    # Dangerous keywords that could modify data
    dangerous_keywords = [
        'drop', 'delete', 'update', 'insert', 'alter', 'create', 'truncate', 
        'exec', 'execute', 'grant', 'revoke', 'merge', 'replace'
    ]
    
    sql_lower = sql_query.lower()
    
    for keyword in dangerous_keywords:
        if f' {keyword} ' in sql_lower or sql_lower.startswith(f'{keyword} '):
            return False, f"Dangerous SQL keyword detected: {keyword}"
    
    # Check for suspicious patterns
    if '--' in sql_query or '/*' in sql_query or '*/' in sql_query:
        return False, "SQL comments are not allowed"
    
    if ';' in sql_query and sql_query.count(';') > 1:
        return False, "Multiple SQL statements are not allowed"
    
    return True, "Query validated successfully"

def convert_numpy_types(obj):
    """Convert numpy types to Python native types for JSON serialization - NumPy 2.0 compatible"""
    import numpy as np
    
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.bytes_, str)):  # Updated for NumPy 2.0
        return str(obj)
    elif pd.isna(obj):
        return None
    elif hasattr(obj, 'item'):  # Handle numpy scalars
        return obj.item()
    else:
        return obj

def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame to ensure JSON serialization compatibility - NumPy 2.0 compatible"""
    df_clean = df.copy()
    
    # Convert all columns to ensure JSON compatibility
    for col in df_clean.columns:
        # Handle different data types
        if df_clean[col].dtype.name.startswith('int'):
            # Convert to nullable integer, then to Python int
            df_clean[col] = df_clean[col].astype('Int64')
            df_clean[col] = df_clean[col].apply(lambda x: int(x) if pd.notna(x) else None)
        elif df_clean[col].dtype.name.startswith('float'):
            # Convert to Python float
            df_clean[col] = df_clean[col].apply(lambda x: float(x) if pd.notna(x) else None)
        elif df_clean[col].dtype.name.startswith('bool'):
            # Convert to Python bool
            df_clean[col] = df_clean[col].apply(lambda x: bool(x) if pd.notna(x) else None)
        elif df_clean[col].dtype == 'object':
            # Convert to string and handle nulls
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if pd.notna(x) else None)
        
        # Apply numpy conversion as final step
        df_clean[col] = df_clean[col].apply(convert_numpy_types)
    
    return df_clean

def safe_to_dict(df: pd.DataFrame, orient: str = 'records') -> list:
    """Safely convert DataFrame to dict with proper type conversion - NumPy 2.0 compatible"""
    try:
        # Clean the dataframe first
        df_clean = clean_dataframe_for_json(df)
        
        # Convert to dict
        result = df_clean.to_dict(orient=orient)
        
        # Apply additional cleaning to the result
        if orient == 'records':
            cleaned_result = []
            for record in result:
                cleaned_record = {}
                for key, value in record.items():
                    cleaned_record[str(key)] = convert_numpy_types(value)
                cleaned_result.append(cleaned_record)
            return cleaned_result
        else:
            # For other orientations, recursively clean
            return convert_numpy_types(result)
            
    except Exception as e:
        logger.warning(f"DataFrame to dict conversion failed: {str(e)}")
        # Fallback: return empty list or basic structure
        return []

def generate_filename(report_name: str, custom_filename: Optional[str] = None) -> str:
    """Generate a safe filename for the report"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if custom_filename:
        # Clean custom filename
        safe_filename = re.sub(r'[^\w\-_.]', '_', custom_filename)
        if not safe_filename.endswith('.csv'):
            safe_filename += '.csv'
        # Add timestamp to ensure uniqueness
        name_part = safe_filename.replace('.csv', '')
        return f"{name_part}_{timestamp}.csv"
    else:
        # Generate from report name
        safe_name = re.sub(r'[^\w\-_]', '_', report_name.lower().replace(' ', '_'))
        return f"dynamic_report_{safe_name}_{timestamp}.csv"

def clean_dataframe_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame to ensure JSON serialization compatibility"""
    df_clean = df.copy()
    
    # Convert all columns to ensure JSON compatibility
    for col in df_clean.columns:
        # Handle different data types
        if df_clean[col].dtype.name.startswith('int'):
            # Convert to nullable integer, then to Python int
            df_clean[col] = df_clean[col].astype('Int64')
            df_clean[col] = df_clean[col].apply(lambda x: int(x) if pd.notna(x) else None)
        elif df_clean[col].dtype.name.startswith('float'):
            # Convert to Python float
            df_clean[col] = df_clean[col].apply(lambda x: float(x) if pd.notna(x) else None)
        elif df_clean[col].dtype.name.startswith('bool'):
            # Convert to Python bool
            df_clean[col] = df_clean[col].apply(lambda x: bool(x) if pd.notna(x) else None)
        elif df_clean[col].dtype == 'object':
            # Convert to string and handle nulls
            df_clean[col] = df_clean[col].apply(lambda x: str(x) if pd.notna(x) else None)
        
        # Apply numpy conversion as final step
        df_clean[col] = df_clean[col].apply(convert_numpy_types)
    
    return df_clean

# API Endpoints

@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Dynamic Pricing Intelligence Report API",
        "version": "3.0.0",
        "timestamp": datetime.now().isoformat(),
        "features": [
            "Dynamic SQL query execution",
            "Automated CSV generation",
            "Supabase storage integration",
            "Real-time analytics",
            "N8N workflow compatibility"
        ],
        "endpoints": [
            "/generate-dynamic-report",
            "/generate-csv (legacy)",
            "/analyze (legacy)", 
            "/test-db",
            "/test-storage",
            "/schema",
            "/reports/download/{filename}",
            "/docs"
        ]
    }

@app.post("/generate-dynamic-report")
async def generate_dynamic_report(request: DynamicReportRequest):
    """Generate dynamic CSV report from any SELECT query - PRIMARY ENDPOINT FOR N8N"""
    try:
        # Validate SQL query
        is_valid, validation_message = validate_sql_query(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Generating dynamic report: {request.report_name}")
        logger.info(f"SQL Query: {request.sql_query[:200]}...")
        
        # Execute dynamic SQL query
        df = get_supabase_data_enhanced(request.sql_query)
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="Query returned no results")
        
        logger.info(f"Dynamic report generated with {len(df)} rows")
        
        # Generate analytics
        analytics = {}
        if request.include_analytics:
            analytics = generate_dynamic_analytics(df, request.sql_query, request.report_name)
        
        # Create enhanced CSV content
        csv_content = create_dynamic_enhanced_csv(
            df, 
            analytics, 
            request.report_name, 
            request.description or "Dynamic competitive intelligence report",
            request.business_value or "Automated pricing insights and competitive analysis",
            request.sql_query
        )
        
        # Generate filename
        filename = generate_filename(request.report_name, request.custom_filename)
        
        storage_result = {}
        public_url = None
        
        if request.upload_to_storage:
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
            "report_name": request.report_name,
            "description": request.description,
            "business_value": request.business_value,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "filename": filename,
            "generated_at": datetime.now().isoformat(),
            "sql_query_executed": request.sql_query,
            "analytics": analytics if request.include_analytics else None,
            "storage_upload": storage_result if request.upload_to_storage else None,
            "download_url": f"/reports/download/{filename}" if public_url else None,
            "execution_summary": {
                "query_validation": "passed",
                "data_source": "supabase_rest_api",
                "processing_time": "dynamic",
                "total_records_processed": len(df)
            }
        }
        
        # Add sample data (first 3 rows) with proper type conversion
        if len(df) > 0:
            response_data["sample_data"] = safe_to_dict(df.head(3), 'records')
        
        # Ensure all data is JSON serializable
        def make_json_safe(obj):
            """Recursively make object JSON safe - NumPy 2.0 compatible"""
            import numpy as np
            
            if isinstance(obj, dict):
                return {k: make_json_safe(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [make_json_safe(item) for item in obj]
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, (np.bytes_, str)):  # Updated for NumPy 2.0
                return str(obj)
            elif hasattr(obj, 'item'):  # numpy scalars
                return obj.item()
            elif pd.isna(obj):
                return None
            else:
                return obj
        
        # Clean the response data
        safe_response_data = make_json_safe(response_data)
        
        # Return as JSONResponse with explicit JSON conversion
        return JSONResponse(
            content=safe_response_data,
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dynamic report generation failed: {str(e)}")
        return JSONResponse(
            content={
                "success": False,
                "error": f"Dynamic report generation failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            },
            status_code=500
        )

@app.post("/generate-csv-from-query")
async def generate_csv_from_query(request: SQLRequest):
    """Generate CSV directly from SQL query - SIMPLIFIED ENDPOINT FOR N8N"""
    try:
        # Validate SQL query
        is_valid, validation_message = validate_sql_query(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Executing SQL query for CSV generation: {request.sql_query[:100]}...")
        
        # Execute query
        df = get_supabase_data_enhanced(request.sql_query)
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="Query returned no results")
        
        logger.info(f"Query returned {len(df)} rows")
        
        # Generate simple analytics if requested
        analytics = {}
        if request.include_analytics:
            analytics = generate_dynamic_analytics(df, request.sql_query, request.report_name or "SQL Query Report")
        
        # Create CSV content with basic metadata
        csv_buffer = io.StringIO()
        
        # Add basic metadata
        csv_buffer.write(f"# Report: {request.report_name or 'Dynamic SQL Query Report'}\n")
        csv_buffer.write(f"# Generated: {datetime.now().isoformat()}\n")
        csv_buffer.write(f"# Records: {len(df)}\n")
        csv_buffer.write(f"# Columns: {len(df.columns)}\n")
        if request.description:
            csv_buffer.write(f"# Description: {request.description}\n")
        csv_buffer.write("#\n")
        csv_buffer.write("# DATA:\n")
        
        # Write data
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sql_report_{timestamp}.csv"
        
        # Upload to storage if requested
        storage_result = {}
        public_url = None
        
        if request.upload_to_storage:
            storage = get_storage_instance()
            public_url = storage.upload_file_content(csv_content, filename)
            storage_result = {
                "success": bool(public_url),
                "filename": filename,
                "public_url": public_url,
                "file_size": len(csv_content.encode('utf-8')),
                "bucket": storage.bucket_name
            }
        
        return {
            "success": True,
            "filename": filename,
            "row_count": len(df),
            "column_count": len(df.columns),
            "generated_at": datetime.now().isoformat(),
            "storage_upload": storage_result if request.upload_to_storage else None,
            "download_url": f"/reports/download/{filename}" if public_url else None,
            "analytics": analytics if request.include_analytics else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV generation from query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"CSV generation from query failed: {str(e)}")

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
        
        # Test multiple possible tables
        possible_tables = ["mbm_price_comparison", "mbm_pricing", "pricing", "price_comparison"]
        
        working_tables = []
        for table_name in possible_tables:
            try:
                url = f"{supabase_url}/rest/v1/{table_name}?limit=1"
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    working_tables.append({
                        "table": table_name,
                        "accessible": True,
                        "has_data": len(data) > 0,
                        "sample_columns": list(data[0].keys()) if data else []
                    })
                    
            except Exception as e:
                working_tables.append({
                    "table": table_name,
                    "accessible": False,
                    "error": str(e)[:100]
                })
        
        if any(table["accessible"] for table in working_tables):
            return {
                "success": True,
                "message": "Database connection successful",
                "supabase_url": supabase_url,
                "tables": working_tables,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "success": False,
                "message": "No accessible tables found",
                "supabase_url": supabase_url,
                "tables": working_tables,
                "timestamp": datetime.now().isoformat()
            }
        
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database test failed: {str(e)}")

@app.get("/schema")
async def get_table_schema():
    """Get table schema information"""
    try:
        supabase_url = os.getenv('SUPABASE_URL', 'https://fbiqlsoheofdmgqmjxfc.supabase.co')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_key:
            raise HTTPException(status_code=500, detail="Missing Supabase configuration")
        
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}'
        }
        
        # Get schema for main table
        url = f"{supabase_url}/rest/v1/mbm_price_comparison?limit=1"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                sample_record = data[0]
                columns = list(sample_record.keys())
                
                # Infer data types from sample
                column_info = {}
                for col, value in sample_record.items():
                    if value is None:
                        column_info[col] = "unknown (null sample)"
                    elif isinstance(value, bool):
                        column_info[col] = "boolean"
                    elif isinstance(value, int):
                        column_info[col] = "integer"
                    elif isinstance(value, float):
                        column_info[col] = "numeric"
                    elif isinstance(value, str):
                        column_info[col] = "text"
                    else:
                        column_info[col] = str(type(value).__name__)
                
                return {
                    "success": True,
                    "table": "mbm_price_comparison",
                    "columns": columns,
                    "column_count": len(columns),
                    "column_types": column_info,
                    "sample_data": sample_record,
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

@app.get("/reports/download/{filename}")
async def download_report(filename: str):
    """Download a report file from Supabase Storage"""
    try:
        storage = get_storage_instance()
        bucket_name = storage.bucket_name
        
        # Construct public URL
        public_url = f"{storage.rest_url}/storage/v1/object/public/{bucket_name}/{filename}"
        
        # Test if file exists
        response = requests.head(public_url, timeout=10)
        
        if response.status_code == 200:
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

# Legacy endpoints for backward compatibility

@app.post("/generate-csv")
async def generate_csv_legacy(request: SQLRequest):
    """Legacy CSV generation endpoint"""
    try:
        # Convert to new format
        dynamic_request = DynamicReportRequest(
            sql_query=request.sql_query,
            report_name=request.report_name or "Legacy SQL Report",
            description="Generated via legacy endpoint",
            include_analytics=request.include_analytics,
            upload_to_storage=request.upload_to_storage
        )
        
        # Use new dynamic endpoint
        return await generate_dynamic_report(dynamic_request)
        
    except Exception as e:
        logger.error(f"Legacy CSV generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Legacy CSV generation failed: {str(e)}")

@app.post("/analyze")
async def analyze_data_legacy(request: SQLRequest):
    """Legacy data analysis endpoint"""
    try:
        # Validate SQL query
        is_valid, validation_message = validate_sql_query(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {validation_message}")
        
        logger.info(f"Analyzing data via legacy endpoint: {request.sql_query[:100]}...")
        
        # Execute query
        df = get_supabase_data_enhanced(request.sql_query)
        
        logger.info(f"Analysis query returned {len(df)} rows")
        
        # Generate analytics
        analytics = generate_dynamic_analytics(df, request.sql_query, "Legacy Analysis")
        
        # Extract summary stats for legacy format
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
            "enhanced_analytics": analytics,
            "timestamp": datetime.now().isoformat(),
            "message": f"Analysis complete: {len(df)} rows processed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Legacy data analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Legacy data analysis failed: {str(e)}")

@app.get("/query-examples")
async def get_query_examples():
    """Get example SQL queries for common pricing intelligence reports"""
    return {
        "examples": {
            "overpriced_products": {
                "description": "Products where you're overpriced vs competitors",
                "sql": """
                SELECT 
                    "Product ID", 
                    "Product Name", 
                    "Brand", 
                    "Category",
                    ROUND("Your Price"::numeric/100, 2) as your_price_pounds,
                    ROUND("Seller Total Price"::numeric/100, 2) as competitor_price_pounds,
                    ROUND("Price Difference"::numeric/100, 2) as overpricing_pounds,
                    "Your Price Rank", 
                    "Competitor Price Rank"
                FROM mbm_price_comparison 
                WHERE "Price Difference" > 0 
                ORDER BY "Price Difference" DESC
                LIMIT 100
                """
            },
            "competitive_advantages": {
                "description": "Products where you have price advantages but poor rankings",
                "sql": """
                SELECT 
                    "Product ID", 
                    "Product Name", 
                    "Category",
                    ROUND("Your Price"::numeric/100, 2) as your_price_pounds,
                    ROUND("Seller Total Price"::numeric/100, 2) as competitor_price_pounds,
                    "Your Price Rank", 
                    "Competitor Price Rank",
                    ("Your Price Rank" - "Competitor Price Rank") as ranking_gap
                FROM mbm_price_comparison 
                WHERE "Price Difference" < 0 AND "Your Price Rank" > "Competitor Price Rank"
                ORDER BY ("Your Price Rank" - "Competitor Price Rank") DESC
                LIMIT 100
                """
            },
            "price_increase_opportunities": {
                "description": "Products where you can safely increase prices",
                "sql": """
                SELECT 
                    "Product ID", 
                    "Product Name", 
                    "Brand",
                    ROUND("Your Price"::numeric/100, 2) as current_price,
                    ROUND("Seller Total Price"::numeric/100, 2) as competitor_price,
                    ROUND(ABS("Price Difference")::numeric/100, 2) as potential_increase,
                    "Your Price Rank"
                FROM mbm_price_comparison 
                WHERE "Price Difference" < -100
                ORDER BY ABS("Price Difference") DESC
                LIMIT 100
                """
            },
            "category_analysis": {
                "description": "Competitive performance by category",
                "sql": """
                SELECT 
                    "Category",
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END) as overpriced_count,
                    COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END) as underpriced_count,
                    ROUND(AVG("Price Difference"::numeric)/100, 2) as avg_difference_pounds
                FROM mbm_price_comparison 
                WHERE "Category" IS NOT NULL
                GROUP BY "Category"
                ORDER BY total_products DESC
                """
            },
            "competitor_analysis": {
                "description": "Performance against specific competitors",
                "sql": """
                SELECT 
                    "Seller Name" as competitor,
                    COUNT(*) as products_compared,
                    COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END) as they_beat_us,
                    COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END) as we_beat_them,
                    ROUND(AVG("Price Difference"::numeric)/100, 2) as avg_difference
                FROM mbm_price_comparison 
                WHERE "Seller Name" IS NOT NULL
                GROUP BY "Seller Name"
                HAVING COUNT(*) >= 10
                ORDER BY they_beat_us DESC
                """
            }
        },
        "tips": [
            "Always include LIMIT clause to avoid large result sets",
            "Use ROUND() for price calculations to improve readability",
            "Filter NULL values for cleaner analysis",
            "Use CASE WHEN for conditional calculations",
            "GROUP BY for category and competitor analysis"
        ]
    }

@app.get("/test-connection-detailed")
async def test_connection_detailed():
    """Detailed connection testing with multiple configurations"""
    try:
        results = test_direct_connection_detailed()
        return {
            "timestamp": datetime.now().isoformat(),
            "results": results,
            "recommendations": {
                "environment_variables": {
                    "SUPABASE_PORT": "6543 (for pooler) or 5432 (direct)",
                    "SUPABASE_HOST": "aws-0-eu-west-2.pooler.supabase.com",
                    "required": ["SUPABASE_PASSWORD", "SUPABASE_USER"]
                },
                "next_steps": [
                    "Ensure SUPABASE_PASSWORD is correctly set",
                    "Try port 6543 for Supabase pooler connection",
                    "Check if your Supabase project allows direct connections",
                    "Verify SSL certificates if connection fails"
                ]
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
