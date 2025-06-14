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
            
            logger.info(f"Upload response: {response.status_code}")
            if response.text:
                logger.debug(f"Upload response body: {response.text[:200]}")
            
            if response.status_code in [200, 201]:
                public_url = f"{self.rest_url}/storage/v1/object/public/{self.bucket_name}/{file_name}"
                logger.info(f"Successfully uploaded {file_name} to bucket '{self.bucket_name}'")
                logger.info(f"Public URL: {public_url}")
                return public_url
            else:
                logger.error(f"Upload failed with status {response.status_code}: {response.text}")
                return None
            
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


REPORT_CONFIGS = {
    "overpriced": {
        "name": "Strategic Overpriced Products Report",
        "description": "Products where MBM is overpriced with optimization recommendations",
        "sql": '''
            SELECT 
                "Product ID", 
                "Product Name", 
                "Brand", 
                "Category", 
                "Sub-Category", 
                "Seller Name",
                
                -- Current pricing in pounds
                ROUND("Your Price"::numeric/100, 2) as mbm_price_pounds,
                ROUND("Seller Total Price"::numeric/100, 2) as competitor_price_pounds,
                ROUND("Price Difference"::numeric/100, 2) as overpricing_pounds,
                
                -- Percentage overpricing for better context
                CASE 
                    WHEN "Seller Total Price" > 0 THEN 
                        ROUND(("Price Difference"::numeric / "Seller Total Price"::numeric) * 100, 1)
                    ELSE 0 
                END as overpricing_percentage,
                
                -- Strategic pricing recommendations
                ROUND("Seller Total Price"::numeric/100, 2) as match_competitor_price,
                ROUND(("Seller Total Price"::numeric * 0.99)/100, 2) as beat_by_1_percent_price,
                ROUND(("Seller Total Price"::numeric * 0.95)/100, 2) as beat_by_5_percent_price,
                ROUND((("Your Price"::numeric + "Seller Total Price"::numeric) / 2)/100, 2) as split_difference_price,
                
                -- Revenue impact calculations
                ROUND(("Your Price"::numeric - "Seller Total Price"::numeric)/100, 2) as revenue_loss_per_unit,
                
                -- Priority scoring
                CASE 
                    WHEN "Price Difference" >= 1000 THEN 'CRITICAL'
                    WHEN "Price Difference" >= 500 THEN 'HIGH'
                    WHEN "Price Difference" >= 100 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as priority_level,
                
                -- Competitive position analysis
                "Your Price Rank", 
                "Competitor Price Rank",
                CASE 
                    WHEN "Your Price Rank" = 1 THEN 'Leading but overpriced'
                    WHEN "Your Price Rank" <= 3 THEN 'Top 3 but overpriced'
                    WHEN "Your Price Rank" <= 5 THEN 'Top 5 but overpriced'
                    ELSE 'Poor ranking and overpriced'
                END as competitive_position,
                
                -- Market opportunity assessment
                CASE 
                    WHEN "Price Difference" >= 500 AND "Your Price Rank" <= 3 THEN 'Quick Win - High Impact'
                    WHEN "Price Difference" >= 100 AND "Your Price Rank" <= 5 THEN 'Standard Opportunity'
                    WHEN "Price Difference" < 100 THEN 'Minor Adjustment'
                    ELSE 'Review Required'
                END as opportunity_type,
                
                "Product URL"

            FROM public.mbm_price_comparison 
            WHERE "Price Difference" > 0 
            ORDER BY 
                CASE 
                    WHEN "Price Difference" >= 500 AND "Your Price Rank" <= 3 THEN 1
                    WHEN "Price Difference" >= 100 AND "Your Price Rank" <= 5 THEN 2
                    ELSE 3
                END,
                "Price Difference" DESC
        ''',
        "business_value": "Immediate revenue protection with strategic pricing guidance"
    },
    
    "ranking_issues": {
        "name": "SEO & Visibility Optimization Report", 
        "description": "Products with competitive pricing but poor search visibility requiring SEO attention",
        "sql": '''
            SELECT 
                "Product ID", 
                "Product Name", 
                "Brand",
                "Category", 
                "Sub-Category",
                "Seller Name",
                
                -- Pricing advantage
                ROUND("Your Price"::numeric/100, 2) as mbm_price_pounds,
                ROUND("Seller Total Price"::numeric/100, 2) as competitor_price_pounds,
                ROUND(ABS("Price Difference")::numeric/100, 2) as price_advantage_pounds,
                
                -- Percentage price advantage
                CASE 
                    WHEN "Your Price" > 0 THEN 
                        ROUND((ABS("Price Difference")::numeric / "Your Price"::numeric) * 100, 1)
                    ELSE 0 
                END as price_advantage_percentage,
                
                -- Ranking analysis
                "Your Price Rank", 
                "Competitor Price Rank",
                ("Your Price Rank" - "Competitor Price Rank") as ranking_gap,
                
                -- Visibility impact assessment
                CASE 
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 10 THEN 'CRITICAL - Major visibility loss'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 6 THEN 'HIGH - Significant gap'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 3 THEN 'MEDIUM - Notable difference'
                    ELSE 'LOW - Minor gap'
                END as visibility_impact,
                
                -- Revenue opportunity from better ranking
                CASE 
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 10 THEN 'High Revenue Potential'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 6 THEN 'Medium Revenue Potential'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 3 THEN 'Standard Revenue Potential'
                    ELSE 'Low Revenue Potential'
                END as revenue_opportunity,
                
                -- Action recommendations
                CASE 
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 10 AND ABS("Price Difference") >= 100 THEN 'Urgent SEO + Marketing Focus'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 6 THEN 'SEO Optimization Required'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 3 THEN 'Monitor and Optimize'
                    ELSE 'Standard Monitoring'
                END as recommended_action,
                
                -- Lost traffic estimate (theoretical)
                CASE 
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 10 THEN 'High traffic loss'
                    WHEN ("Your Price Rank" - "Competitor Price Rank") >= 6 THEN 'Medium traffic loss'
                    ELSE 'Low traffic loss'
                END as estimated_traffic_impact,
                
                "Product URL"

            FROM public.mbm_price_comparison 
            WHERE "Price Difference" < 0 AND "Your Price Rank" > "Competitor Price Rank"
            ORDER BY 
                ("Your Price Rank" - "Competitor Price Rank") DESC,
                ABS("Price Difference") DESC
        ''',
        "business_value": "SEO/visibility optimization with quantified revenue impact"
    },
    
    "price_increase": {
        "name": "Margin Improvement Opportunities",
        "description": "Products where MBM can safely increase prices to maximize profitability",
        "sql": '''
            SELECT 
                "Product ID", 
                "Product Name", 
                "Brand", 
                "Category", 
                "Sub-Category",
                "Seller Name",
                
                -- Current pricing
                ROUND("Your Price"::numeric/100, 2) as current_mbm_price,
                ROUND("Seller Total Price"::numeric/100, 2) as competitor_price,
                ROUND(ABS("Price Difference")::numeric/100, 2) as potential_increase_pounds,
                
                -- Percentage increase potential
                CASE 
                    WHEN "Your Price" > 0 THEN 
                        ROUND((ABS("Price Difference")::numeric / "Your Price"::numeric) * 100, 1)
                    ELSE 0 
                END as potential_increase_percentage,
                
                -- Strategic pricing recommendations
                ROUND(("Seller Total Price"::numeric * 0.95)/100, 2) as conservative_increase_price,
                ROUND(("Seller Total Price"::numeric * 0.90)/100, 2) as moderate_increase_price,
                ROUND(("Seller Total Price"::numeric * 0.85)/100, 2) as aggressive_increase_price,
                
                -- Revenue impact projections
                ROUND((("Seller Total Price"::numeric * 0.95) - "Your Price"::numeric)/100, 2) as conservative_revenue_gain,
                ROUND((("Seller Total Price"::numeric * 0.90) - "Your Price"::numeric)/100, 2) as moderate_revenue_gain,
                ROUND((("Seller Total Price"::numeric * 0.85) - "Your Price"::numeric)/100, 2) as aggressive_revenue_gain,
                
                -- Risk assessment
                CASE 
                    WHEN ABS("Price Difference") >= 500 THEN 'LOW RISK - Large margin'
                    WHEN ABS("Price Difference") >= 200 THEN 'MEDIUM RISK - Moderate margin'
                    WHEN ABS("Price Difference") >= 100 THEN 'HIGHER RISK - Small margin'
                    ELSE 'HIGH RISK - Minimal margin'
                END as price_increase_risk,
                
                -- Market position after increase
                "Your Price Rank", 
                "Competitor Price Rank",
                
                -- Opportunity classification
                CASE 
                    WHEN ABS("Price Difference") >= 500 AND "Your Price Rank" <= 2 THEN 'Premium Opportunity'
                    WHEN ABS("Price Difference") >= 200 AND "Your Price Rank" <= 3 THEN 'Standard Opportunity'
                    WHEN ABS("Price Difference") >= 100 THEN 'Careful Opportunity'
                    ELSE 'Monitor Only'
                END as opportunity_class,
                
                -- Implementation priority
                CASE 
                    WHEN ABS("Price Difference") >= 500 THEN 'IMMEDIATE'
                    WHEN ABS("Price Difference") >= 200 THEN 'HIGH'
                    WHEN ABS("Price Difference") >= 100 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as implementation_priority,
                
                "Product URL"

            FROM public.mbm_price_comparison 
            WHERE "Price Difference" < -50
            ORDER BY 
                ABS("Price Difference") DESC,
                "Your Price Rank" ASC
        ''',
        "business_value": "Margin improvement with risk-assessed pricing strategies"
    },
    
    "competitive_threat": {
        "name": "Strategic Competitive Intelligence",
        "description": "Comprehensive competitor analysis with market positioning insights",
        "sql": '''
            SELECT 
                COALESCE(NULLIF("Category", 'NA'), 'Uncategorized') as category,
                "Seller Name" as competitor,
                COUNT(*) as products_compared,
                COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END) as they_beat_us_count,
                COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END) as we_beat_them_count,
                COUNT(CASE WHEN "Price Difference" = 0 THEN 1 END) as price_matches,
                
                -- Win rate analysis
                ROUND((COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100, 1) as their_win_rate_percent,
                ROUND((COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END)::numeric / COUNT(*)) * 100, 1) as our_win_rate_percent,
                
                -- Financial impact
                ROUND(AVG("Price Difference"::numeric)/100, 2) as avg_difference_pounds,
                ROUND(AVG(CASE WHEN "Price Difference" > 0 THEN "Price Difference" END)::numeric/100, 2) as avg_we_lose_by,
                ROUND(AVG(CASE WHEN "Price Difference" < 0 THEN ABS("Price Difference") END)::numeric/100, 2) as avg_we_win_by,
                ROUND(SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END)::numeric/100, 2) as total_revenue_loss,
                ROUND(SUM(CASE WHEN "Price Difference" < 0 THEN ABS("Price Difference") ELSE 0 END)::numeric/100, 2) as total_revenue_advantage,
                
                -- Net competitive position
                ROUND((SUM(CASE WHEN "Price Difference" < 0 THEN ABS("Price Difference") ELSE 0 END)::numeric - 
                       SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END)::numeric)/100, 2) as net_revenue_impact,
                
                -- Threat level assessment
                CASE 
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 70 THEN 'CRITICAL THREAT'
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 50 THEN 'HIGH THREAT'
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 30 THEN 'MODERATE THREAT'
                    ELSE 'LOW THREAT'
                END as threat_level,
                
                -- Strategic priority
                CASE 
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 60 
                         AND SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END) >= 5000 THEN 'URGENT ACTION'
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 50 
                         AND SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END) >= 2000 THEN 'HIGH PRIORITY'
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 40 THEN 'MONITOR CLOSELY'
                    ELSE 'STANDARD MONITORING'
                END as strategic_priority,
                
                -- Market position
                CASE 
                    WHEN (COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 60 THEN 'MARKET LEADER'
                    WHEN (COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 40 THEN 'COMPETITIVE'
                    WHEN (COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 20 THEN 'CHALLENGING'
                    ELSE 'STRUGGLING'
                END as market_position,
                
                -- Ranking performance
                ROUND(AVG("Your Price Rank"::numeric), 1) as avg_our_rank,
                ROUND(AVG("Competitor Price Rank"::numeric), 1) as avg_their_rank,
                ROUND((AVG("Your Price Rank"::numeric) - AVG("Competitor Price Rank"::numeric)), 1) as avg_ranking_gap,
                
                -- Opportunity assessment
                CASE 
                    WHEN (COUNT(CASE WHEN "Price Difference" < 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 50 
                         AND AVG("Your Price Rank"::numeric) > AVG("Competitor Price Rank"::numeric) THEN 'SEO/Visibility Focus'
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 50 
                         AND SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END) >= 2000 THEN 'Pricing Strategy Review'
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 60 THEN 'Comprehensive Strategy Needed'
                    ELSE 'Maintain Current Strategy'
                END as recommended_strategy

            FROM public.mbm_price_comparison 
            WHERE "Seller Name" IS NOT NULL
            GROUP BY "Category", "Seller Name"
            HAVING COUNT(*) >= 5
            ORDER BY 
                CASE 
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 60 
                         AND SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END) >= 5000 THEN 1
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 50 
                         AND SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END) >= 2000 THEN 2
                    WHEN (COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END)::numeric / COUNT(*)) * 100 >= 50 THEN 3
                    ELSE 4
                END,
                SUM(CASE WHEN "Price Difference" > 0 THEN "Price Difference" ELSE 0 END) DESC,
                COUNT(CASE WHEN "Price Difference" > 0 THEN 1 END) DESC
        ''',
        "business_value": "Strategic competitive positioning with actionable intelligence and revenue impact analysis"
    }
}

def get_supabase_data_all(sql_query: str = None, table: str = "mbm_price_comparison"):
    """Get ALL data from Supabase using REST API with pagination"""
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
        base_url = f"{supabase_url}/rest/v1/{clean_table}"
        
        all_data = []
        page_size = 1000  # Process in chunks of 1000
        offset = 0
        
        logger.info(f"Starting to fetch ALL data from {clean_table}")
        
        while True:
            # Build URL with pagination
            params = {
                'limit': page_size,
                'offset': offset
            }
            
            logger.info(f"Fetching rows {offset} to {offset + page_size}")
            
            response = requests.get(base_url, headers=headers, params=params)
            
            if response.status_code == 200:
                page_data = response.json()
                
                if not page_data:  # No more data
                    break
                    
                all_data.extend(page_data)
                logger.info(f"Retrieved {len(page_data)} rows (total so far: {len(all_data)})")
                
                # If we got less than page_size, we're done
                if len(page_data) < page_size:
                    break
                    
                offset += page_size
                
            else:
                error_msg = f"Supabase API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
        
        logger.info(f"Completed data fetch: {len(all_data)} total records")
        df = pd.DataFrame(all_data)
        
        # Apply SQL filtering if needed
        if sql_query:
            df = apply_sql_filters(df, sql_query)
            
        return df
            
    except Exception as e:
        logger.error(f"Supabase data fetch failed: {str(e)}")
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

def get_storage_instance():
    """Get configured storage instance"""
    anon_key = os.getenv('SUPABASE_ANON_KEY')
    bucket_name = os.getenv('SUPABASE_STORAGE_BUCKET', 'reports')
    enabled = os.getenv('ENABLE_STORAGE_UPLOADS', 'true').lower() == 'true'
    
    return SupabaseStorage(anon_key=anon_key, bucket_name=bucket_name, enabled=enabled)

def apply_sql_filters(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    """Apply basic SQL-like filters to the DataFrame"""
    try:
        sql_upper = sql_query.upper()
        
        # Handle WHERE Price Difference > 0 (overpriced)
        if 'WHERE "PRICE DIFFERENCE" > 0' in sql_upper:
            df = df[df['Price Difference'] > 0]
            logger.info(f"Filtered to {len(df)} overpriced products")
            
        # Handle WHERE Price Difference < 0 AND ranking condition (ranking issues)
        elif 'WHERE "PRICE DIFFERENCE" < 0 AND "YOUR PRICE RANK" > "COMPETITOR PRICE RANK"' in sql_upper:
            df = df[(df['Price Difference'] < 0) & (df['Your Price Rank'] > df['Competitor Price Rank'])]
            logger.info(f"Filtered to {len(df)} products with ranking issues")
            
        # Handle WHERE Price Difference < -50 (price increase opportunities)
        elif 'WHERE "PRICE DIFFERENCE" < -50' in sql_upper:
            df = df[df['Price Difference'] < -50]
            logger.info(f"Filtered to {len(df)} price increase opportunities")
        
        # Handle ORDER BY
        if 'ORDER BY' in sql_upper:
            if 'ORDER BY "PRICE DIFFERENCE" DESC' in sql_upper:
                df = df.sort_values('Price Difference', ascending=False)
            elif 'ORDER BY ABS("PRICE DIFFERENCE") DESC' in sql_upper:
                df = df.reindex(df['Price Difference'].abs().sort_values(ascending=False).index)
            elif 'ORDER BY ("YOUR PRICE RANK" - "COMPETITOR PRICE RANK") DESC' in sql_upper:
                df['ranking_gap'] = df['Your Price Rank'] - df['Competitor Price Rank']
                df = df.sort_values('ranking_gap', ascending=False)
        
        return df
        
    except Exception as e:
        logger.warning(f"SQL filter application failed: {str(e)}, returning unfiltered data")
        return df

def process_competitive_threat_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Process competitive threat analysis with proper grouping"""
    try:
        # Group by Category and Seller Name
        grouped = df.groupby(['Category', 'Seller Name']).agg({
            'Price Difference': ['count', 'mean', lambda x: (x > 0).sum(), lambda x: (x < 0).sum()],
        }).round(2)
        
        # Flatten column names
        grouped.columns = ['products_compared', 'avg_difference_pounds', 'they_beat_us', 'we_beat_them']
        grouped['avg_difference_pounds'] = grouped['avg_difference_pounds'] / 100  # Convert to pounds
        
        # Filter to categories with meaningful comparison volume
        grouped = grouped[grouped['products_compared'] >= 5]
        
        # Calculate average we lose by
        lose_data = df[df['Price Difference'] > 0].groupby(['Category', 'Seller Name'])['Price Difference'].mean() / 100
        grouped['avg_we_lose_by'] = lose_data
        
        # Sort by they_beat_us desc, then avg_we_lose_by desc
        grouped = grouped.sort_values(['they_beat_us', 'avg_we_lose_by'], ascending=[False, False])
        
        # Reset index to make Category and Seller Name regular columns
        grouped = grouped.reset_index()
        
        return grouped
        
    except Exception as e:
        logger.error(f"Competitive threat analysis failed: {str(e)}")
        return df

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

@app.get("/debug/storage")
async def debug_storage():
    """Debug storage configuration and test upload"""
    try:
        # Check environment variables
        supabase_url = os.getenv('SUPABASE_URL', 'https://fbiqlsoheofdmgqmjxfc.supabase.co')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        bucket_name = os.getenv('SUPABASE_STORAGE_BUCKET', 'reports')
        
        debug_info = {
            "environment_variables": {
                "SUPABASE_URL": supabase_url,
                "SUPABASE_ANON_KEY": "SET" if supabase_key else "MISSING",
                "SUPABASE_ANON_KEY_LENGTH": len(supabase_key) if supabase_key else 0,
                "SUPABASE_STORAGE_BUCKET": bucket_name,
                "ENABLE_STORAGE_UPLOADS": os.getenv('ENABLE_STORAGE_UPLOADS', 'true')
            },
            "storage_config": {
                "rest_url": supabase_url,
                "bucket_name": bucket_name,
                "enabled": bool(supabase_key)
            }
        }
        
        if not supabase_key:
            debug_info["error"] = "SUPABASE_ANON_KEY environment variable is missing"
            return debug_info
        
        # Test bucket access with correct API format
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        
        # Try to list bucket contents with correct body format
        list_url = f"{supabase_url}/storage/v1/object/list/{bucket_name}"
        list_body = {
            "limit": 1,
            "prefix": ""  # Required property
        }
        
        list_response = requests.post(list_url, headers=headers, json=list_body, timeout=10)
        
        debug_info["bucket_test"] = {
            "list_url": list_url,
            "request_body": list_body,
            "status_code": list_response.status_code,
            "response": list_response.text[:500] if list_response.text else "Empty response",
            "headers_sent": {k: v for k, v in headers.items() if k != "Authorization"}
        }
        
        if list_response.status_code == 404:
            debug_info["bucket_test"]["error"] = f"Bucket '{bucket_name}' does not exist or is not accessible"
        elif list_response.status_code == 403:
            debug_info["bucket_test"]["error"] = "Access denied. Check your SUPABASE_ANON_KEY permissions"
        elif list_response.status_code == 401:
            debug_info["bucket_test"]["error"] = "Unauthorized. Check your SUPABASE_ANON_KEY"
        elif list_response.status_code == 400:
            debug_info["bucket_test"]["error"] = "Bad request. API format issue."
        
        # Try a test upload if bucket is accessible
        if list_response.status_code == 200:
            test_content = "Product ID,Product Name,Price\nTEST001,Test Product,10.99"
            test_filename = f"test_upload_{int(time.time())}.csv"
            
            upload_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{test_filename}"
            upload_headers = {
                "apikey": supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": "text/csv"
            }
            
            upload_response = requests.post(
                upload_url, 
                headers=upload_headers, 
                data=test_content.encode('utf-8'), 
                timeout=30
            )
            
            debug_info["upload_test"] = {
                "upload_url": upload_url,
                "status_code": upload_response.status_code,
                "response": upload_response.text[:500] if upload_response.text else "Empty response",
                "success": upload_response.status_code in [200, 201]
            }
            
            if upload_response.status_code in [200, 201]:
                public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{test_filename}"
                debug_info["upload_test"]["public_url"] = public_url
                
                # Test if the public URL is accessible
                try:
                    public_response = requests.head(public_url, timeout=10)
                    debug_info["public_url_test"] = {
                        "public_url": public_url,
                        "status_code": public_response.status_code,
                        "accessible": public_response.status_code == 200
                    }
                except Exception as e:
                    debug_info["public_url_test"] = {
                        "public_url": public_url,
                        "error": str(e)
                    }
                
                # Clean up test file
                try:
                    delete_response = requests.delete(upload_url, headers=upload_headers, timeout=10)
                    debug_info["cleanup"] = {
                        "delete_status": delete_response.status_code,
                        "deleted": delete_response.status_code in [200, 204]
                    }
                except Exception as e:
                    debug_info["cleanup"] = {
                        "error": str(e)
                    }
        
        return debug_info
        
    except Exception as e:
        logger.error(f"Storage debug failed: {str(e)}")
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "environment_variables": {
                "SUPABASE_URL": os.getenv('SUPABASE_URL', 'NOT SET'),
                "SUPABASE_ANON_KEY": "SET" if os.getenv('SUPABASE_ANON_KEY') else "NOT SET",
                "SUPABASE_STORAGE_BUCKET": os.getenv('SUPABASE_STORAGE_BUCKET', 'NOT SET')
            }
        }

@app.get("/debug/storage")
async def debug_storage():
    """Debug storage configuration and test upload"""
    try:
        # Check environment variables
        supabase_url = os.getenv('SUPABASE_URL', 'https://fbiqlsoheofdmgqmjxfc.supabase.co')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        bucket_name = os.getenv('SUPABASE_STORAGE_BUCKET', 'reports')
        
        debug_info = {
            "environment_variables": {
                "SUPABASE_URL": supabase_url,
                "SUPABASE_ANON_KEY": "SET" if supabase_key else "MISSING",
                "SUPABASE_ANON_KEY_LENGTH": len(supabase_key) if supabase_key else 0,
                "SUPABASE_STORAGE_BUCKET": bucket_name,
                "ENABLE_STORAGE_UPLOADS": os.getenv('ENABLE_STORAGE_UPLOADS', 'true')
            },
            "storage_config": {
                "rest_url": supabase_url,
                "bucket_name": bucket_name,
                "enabled": bool(supabase_key)
            }
        }
        
        if not supabase_key:
            debug_info["error"] = "SUPABASE_ANON_KEY environment variable is missing"
            return debug_info
        
        # Test bucket access
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }
        
        # Try to list bucket contents
        list_url = f"{supabase_url}/storage/v1/object/list/{bucket_name}"
        list_response = requests.post(list_url, headers=headers, json={"limit": 1}, timeout=10)
        
        debug_info["bucket_test"] = {
            "list_url": list_url,
            "status_code": list_response.status_code,
            "response": list_response.text[:500] if list_response.text else "Empty response",
            "headers_sent": {k: v for k, v in headers.items() if k != "Authorization"}
        }
        
        if list_response.status_code == 404:
            debug_info["bucket_test"]["error"] = f"Bucket '{bucket_name}' does not exist or is not accessible"
        elif list_response.status_code == 403:
            debug_info["bucket_test"]["error"] = "Access denied. Check your SUPABASE_ANON_KEY permissions"
        elif list_response.status_code == 401:
            debug_info["bucket_test"]["error"] = "Unauthorized. Check your SUPABASE_ANON_KEY"
        
        # Try a test upload if bucket is accessible
        if list_response.status_code == 200:
            test_content = "Product ID,Product Name,Price\nTEST001,Test Product,10.99"
            test_filename = f"test_upload_{int(time.time())}.csv"
            
            upload_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{test_filename}"
            upload_headers = {
                "apikey": supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": "text/csv"
            }
            
            upload_response = requests.post(
                upload_url, 
                headers=upload_headers, 
                data=test_content.encode('utf-8'), 
                timeout=30
            )
            
            debug_info["upload_test"] = {
                "upload_url": upload_url,
                "status_code": upload_response.status_code,
                "response": upload_response.text[:500] if upload_response.text else "Empty response",
                "success": upload_response.status_code in [200, 201]
            }
            
            if upload_response.status_code in [200, 201]:
                public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{test_filename}"
                debug_info["upload_test"]["public_url"] = public_url
                
                # Test if the public URL is accessible
                public_response = requests.head(public_url, timeout=10)
                debug_info["public_url_test"] = {
                    "public_url": public_url,
                    "status_code": public_response.status_code,
                    "accessible": public_response.status_code == 200
                }
                
                # Clean up test file
                delete_response = requests.delete(upload_url, headers=upload_headers, timeout=10)
                debug_info["cleanup"] = {
                    "delete_status": delete_response.status_code,
                    "deleted": delete_response.status_code in [200, 204]
                }
        
        return debug_info
        
    except Exception as e:
        logger.error(f"Storage debug failed: {str(e)}")
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "environment_variables": {
                "SUPABASE_URL": os.getenv('SUPABASE_URL', 'NOT SET'),
                "SUPABASE_ANON_KEY": "SET" if os.getenv('SUPABASE_ANON_KEY') else "NOT SET",
                "SUPABASE_STORAGE_BUCKET": os.getenv('SUPABASE_STORAGE_BUCKET', 'NOT SET')
            }
        }

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
    """Generate a comprehensive report with analytics and optional storage upload - NO LIMITS"""
    try:
        if report_type not in REPORT_CONFIGS:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid report type. Available: {list(REPORT_CONFIGS.keys())}"
            )
        
        report_config = REPORT_CONFIGS[report_type]
        logger.info(f"Generating {report_config['name']} - fetching ALL data")
        
        # Special handling for competitive threat analysis (needs GROUP BY)
        if report_type == "competitive_threat":
            # This one needs special SQL handling, we'll process it differently
            df = get_supabase_data_all(table="mbm_price_comparison")
            df = process_competitive_threat_analysis(df)
        else:
            # For other reports, get all data and filter
            df = get_supabase_data_all(sql_query=report_config["sql"], table="mbm_price_comparison")
            
            # Apply column transformations
            if 'overpricing_pounds' not in df.columns and 'Price Difference' in df.columns:
                df['overpricing_pounds'] = df['Price Difference'] / 100
            if 'mbm_price_pounds' not in df.columns and 'Your Price' in df.columns:
                df['mbm_price_pounds'] = df['Your Price'] / 100
            if 'competitor_price_pounds' not in df.columns and 'Seller Total Price' in df.columns:
                df['competitor_price_pounds'] = df['Seller Total Price'] / 100
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="Report query returned no results")
        
        logger.info(f"Report generated with {len(df)} total rows (no limits applied)")
        
        # Generate analytics
        analytics = {}
        if include_analytics:
            analytics = generate_report_analytics(df, report_type)
        
        # Create enhanced CSV content
        csv_content = create_enhanced_csv(df, analytics, report_config)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mbm_{report_type}_report_all_data_{timestamp}.csv"
        
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
            "download_url": f"/reports/download/{filename}" if public_url else None,
            "data_completeness": {
                "total_database_records": "35,354+",
                "report_records": len(df),
                "percentage_coverage": f"{(len(df)/35354)*100:.1f}%" if len(df) <= 35354 else "100%",
                "note": "All matching records included - no artificial limits"
            }
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
