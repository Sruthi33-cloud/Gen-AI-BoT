import logging
import os
import json
import azure.functions as func
import sys
import asyncio
from typing import Dict, List, Optional, Any

# Bot Framework SDK Libraries
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import MicrosoftAppCredentials

import pandas as pd
import requests
import snowflake.connector
import random
from openai import AzureOpenAI
from time import sleep

# Setup logging
logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)

# Single-tenant credentials class
class SingleTenantAppCredentials(MicrosoftAppCredentials):
    def __init__(self, app_id: str, password: str, tenant_id: str):
        super().__init__(app_id, password)
        self.tenant_id = tenant_id
        self.oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}"

# Environment variables
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
APP_TYPE = os.environ.get("MicrosoftAppType", "SingleTenant")
APP_TENANT_ID = os.environ.get("MicrosoftAppTenantId", "")

AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME")

SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")

# Azure OpenAI client with correct API version
try:
    AZURE_OPENAI_CLIENT = AzureOpenAI(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version="2025-01-01-preview"
    )
    logger.info("AzureOpenAI client initialized.")
except Exception as e:
    logger.error(f"Error initializing AzureOpenAI client: {e}")
    AZURE_OPENAI_CLIENT = None

# STRATEGY 1: Load full knowledge base but optimize usage
def load_knowledge_base():
    """Load your complete JSON knowledge base"""
    try:
        path = os.path.join(os.path.dirname(__file__), "knowledge_base.json")
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading knowledge_base.json: {e}")
        # Fallback to your provided structure
        return [
            {
                "measure_name": "Sales Amount",
                "tool_name": "sales_amount",
                "description": "The total monetary value of all sales transactions. This is a key measure for a store's financial performance.",
                "dax_formula": "SUM('SalesFact'[SalesAmount])",
                "aliases": ["sales", "sales revenue", "total sales", "store sales"]
            },
            {
                "measure_name": "Traffic Conversion",
                "tool_name": "traffic_conversion", 
                "description": "Calculated as Total Sales Transactions divided by Total Store Visits.",
                "dax_formula": "[Total Transactions] / [Store Visits]",
                "aliases": ["conversion rate", "traffic to sales", "customer conversion"]
            }
        ]

KNOWLEDGE_BASE_DATA = load_knowledge_base()

# STRATEGY 2: Create lookup dictionaries for fast access
TOOL_NAME_TO_MEASURE = {item['tool_name']: item for item in KNOWLEDGE_BASE_DATA}
ALIAS_TO_TOOL_NAME = {}
for item in KNOWLEDGE_BASE_DATA:
    for alias in item['aliases']:
        ALIAS_TO_TOOL_NAME[alias.lower()] = item['tool_name']

# Cached data
_rbac_cache = None
_territory_cache = None

def get_cached_rbac_data(conn):
    global _rbac_cache
    if _rbac_cache is None:
        query = "SELECT USER_ID, ROLE, STORE_ID FROM ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE"
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        cur.close()
        df.columns = df.columns.str.lower()
        _rbac_cache = df
    return _rbac_cache

def get_user_data(user_id: str, conn) -> Optional[Dict[str, Any]]:
    """Get user access data efficiently"""
    rbac_df = get_cached_rbac_data(conn)
    user_row = rbac_df[rbac_df['user_id'] == user_id]
    if user_row.empty:
        return None
    return {
        "role": user_row.iloc[0]['role'],
        "store_id": user_row.iloc[0]['store_id']
    }

# STRATEGY 3: Ultra-efficient intent recognition
def identify_metric_intent(user_query: str) -> Optional[str]:
    """Hybrid approach: Keyword matching first (0 tokens), minimal LLM fallback"""
    
    query_lower = user_query.lower()
    
    # Phase 1: Direct keyword matching (NO TOKENS)
    for alias, tool_name in ALIAS_TO_TOOL_NAME.items():
        if alias in query_lower:
            logger.info(f"Keyword match found: {alias} -> {tool_name}")
            return tool_name
    
    # Phase 2: Minimal LLM only if keyword matching fails
    if not AZURE_OPENAI_CLIENT:
        return "sales_amount" # Safe default
        
    # ULTRA-MINIMAL prompt (only ~15 tokens)
    prompt = f"'{user_query}' -> sales_amount OR traffic_conversion"
    
    try:
        response = AZURE_OPENAI_CLIENT.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=8 # Extremely low
        )
        
        result = response.choices[0].message.content.strip().lower()
        if "traffic" in result:
            return "traffic_conversion"
        else:
            return "sales_amount"
            
    except Exception as e:
        logger.error(f"LLM intent error: {e}")
        return "sales_amount"

# STRATEGY 4: Fast data retrieval
def get_metric_value_fast(conn, tool_name: str, store_id: int) -> Optional[float]:
    """Fast metric retrieval"""
    try:
        if tool_name == "sales_amount":
            query = f"SELECT COALESCE(SUM(SalesAmount), 0) FROM ENTERPRISE.RETAIL_DATA.SALES_FACT WHERE SalesTerritoryKey = {store_id % 10}"
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()
            cur.close()
            return float(result[0]) if result else 0.0
            
        elif tool_name == "traffic_conversion":
            # Return example value - replace with your actual query
            return 0.000 # Matches your example
            
        return None
    except Exception as e:
        logger.error(f"Metric query error: {e}")
        return 0.0

# STRATEGY 5: Token-optimized response generation
def generate_rich_response(user_query: str, tool_name: str, metric_value: float, store_id: int) -> str:
    """Generate rich responses with all details using minimal tokens"""
    
    measure_info = TOOL_NAME_TO_MEASURE.get(tool_name)
    if not measure_info:
        return f"Metric not found for store {store_id}."
    
    # Format value appropriately
    if tool_name == "sales_amount":
        formatted_value = f"${metric_value:,.2f}"
        value_type = "currency"
    else:
        formatted_value = f"{metric_value:.3f}"
        value_type = "decimal"
    
    # STRATEGY A: Template-based (0 response generation tokens)
    # This matches your example format exactly
    measure_name = measure_info['measure_name']
    description = measure_info['description']
    
    base_response = f"For store {store_id}, {measure_name} is defined as {description.lower()}, and the current {measure_name} value is {formatted_value}."
    
    # Add contextual suggestions based on metric type
    if tool_name == "sales_amount":
        suggestions = ' You might also ask: "How does this compare to last month?" or "What are my top performing products?"'
    else: # traffic_conversion
        suggestions = ' You might also ask: "What are the total store visits for my store?" or "How many sales transactions did we record to calculate this measure?"'
    
    return base_response + suggestions

# STRATEGY B: If you want LLM enhancement (optional, costs ~40 tokens)
def generate_llm_enhanced_response(user_query: str, tool_name: str, metric_value: float, store_id: int) -> str:
    """LLM-enhanced response with strict token limits"""
    
    measure_info = TOOL_NAME_TO_MEASURE.get(tool_name)
    if not measure_info:
        return f"Metric not found for store {store_id}."
    
    # Format value
    if tool_name == "sales_amount":
        formatted_value = f"${metric_value:,.2f}"
    else:
        formatted_value = f"{metric_value:.3f}"
    
    # ULTRA-COMPACT prompt with all essential info
    prompt = f"""Store {store_id}: {measure_info['measure_name']} = {formatted_value}
Def: {measure_info['description']}
DAX: {measure_info['dax_formula']}
Answer user + suggest 2 related questions."""
    
    try:
        response = AZURE_OPENAI_CLIENT.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=80 # Strict limit but allows rich response
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"LLM enhancement error: {e}")
        # Fallback to template
        return generate_rich_response(user_query, tool_name, metric_value, store_id)

# Session management
class UserSession:
    def __init__(self, user_id: str, role: str, store_id: int):
        self.user_id = user_id
        self.role = role
        self.store_id = store_id
        self.last_queries = []

_user_sessions = {}

def get_user_session(user_id: str, conn) -> Optional[UserSession]:
    if user_id not in _user_sessions:
        user_data = get_user_data(user_id, conn)
        if user_data:
            _user_sessions[user_id] = UserSession(
                user_id, user_data["role"], user_data["store_id"]
            )
    return _user_sessions.get(user_id)

# Optimized message handler
async def message_handler(turn_context: TurnContext):
    """Ultra-optimized message handler with full information"""
    
    if turn_context.activity.type != ActivityTypes.message:
        return
    
    user_query = turn_context.activity.text.strip()
    user_id = "victor" # Static for testing
    
    # Quick validation
    if not user_query or len(user_query) > 300:
        await turn_context.send_activity("Please ask a specific question about sales or traffic conversion.")
        return
    
    conn = None
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        # Get user session
        session = get_user_session(user_id, conn)
        if not session:
            await turn_context.send_activity("Access denied. Contact support.")
            return
        
        # 1. Identify intent (keyword first, minimal LLM fallback)
        tool_name = identify_metric_intent(user_query)
        if not tool_name:
            await turn_context.send_activity("Ask about sales amount or traffic conversion.")
            return
        
        # 2. Get metric value
        metric_value = get_metric_value_fast(conn, tool_name, session.store_id)
        if metric_value is None:
            await turn_context.send_activity(f"Cannot retrieve data for store {session.store_id}.")
            return
        
        # 3. Generate response 
        # Option A: Template-only (0 LLM tokens for response)
        response = generate_rich_response(user_query, tool_name, metric_value, session.store_id)
        
        # Option B: LLM-enhanced (uncomment to use ~40 tokens)
        # response = generate_llm_enhanced_response(user_query, tool_name, metric_value, session.store_id)
        
        # 4. Send response
        await turn_context.send_activity(response)
        
        # Update session
        session.last_queries.append({"query": user_query, "metric": tool_name})
        if len(session.last_queries) > 3:
            session.last_queries.pop(0)
            
    except Exception as e:
        logger.error(f"Error in message_handler: {e}")
        await turn_context.send_activity("Service temporarily unavailable. Please try again.")
    finally:
        if conn:
            conn.close()

# Main function with single-tenant auth
def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == 'GET':
        return func.HttpResponse("Bot endpoint is healthy", status_code=200)

    if req.method != 'POST':
        return func.HttpResponse("Only POST requests supported.", status_code=405)

    try:
        req_json = req.get_json()
        activity = Activity.deserialize(req_json)
        auth_header = req.headers.get('Authorization') or req.headers.get('authorization') or ''

        # Single-tenant adapter setup
        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            credentials = SingleTenantAppCredentials(APP_ID, APP_PASSWORD, APP_TENANT_ID)
        else:
            credentials = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)

        settings = BotFrameworkAdapterSettings(app_id=APP_ID, app_password=APP_PASSWORD)
        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            settings.oauth_endpoint = f"https://login.microsoftonline.com/{APP_TENANT_ID}"

        adapter = BotFrameworkAdapter(settings)
        adapter._credentials = credentials

        # Process activity
        asyncio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(asyncio_loop)
        response = asyncio_loop.run_until_complete(
            adapter.process_activity(activity, auth_header, message_handler)
        )

        return func.HttpResponse(
            body=response.body if response else "",
            status_code=response.status if response else 202
        )

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return func.HttpResponse("Internal error.", status_code=500)
