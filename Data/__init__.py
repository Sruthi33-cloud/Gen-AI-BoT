import logging
import os
import json
import azure.functions as func
import sys
import asyncio
from typing import Dict, List, Optional, Any
import re

from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import MicrosoftAppCredentials

import pandas as pd
import snowflake.connector
from openai import AzureOpenAI

# Logging
logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s"))
    logger.addHandler(h)

# Single-tenant auth
class SingleTenantAppCredentials(MicrosoftAppCredentials):
    def __init__(self, app_id: str, password: str, tenant_id: str):
        super().__init__(app_id, password)
        self.tenant_id = tenant_id
        self.oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}"

# Env vars
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

# OpenAI client
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

# SOLUTION 1: DYNAMIC COLUMN MAPPING WITH CACHING
_column_cache = {}
_schema_cache = {}

def get_table_columns(conn, table_name: str) -> Dict[str, str]:
    """Get column names dynamically - cached for performance"""
    if table_name in _column_cache:
        return _column_cache[table_name]
    
    try:
        cur = conn.cursor()
        # Get column info from Snowflake information schema
        cur.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}' 
        AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """)
        
        columns = {}
        for row in cur.fetchall():
            col_name = row[0].lower()
            data_type = row[1]
            columns[col_name] = data_type
        
        cur.close()
        _column_cache[table_name] = columns
        logger.info(f"Cached columns for {table_name}: {list(columns.keys())}")
        return columns
        
    except Exception as e:
        logger.error(f"Error getting columns for {table_name}: {e}")
        return {}

def smart_column_finder(columns: Dict[str, str], search_terms: List[str]) -> Optional[str]:
    """Find column name using smart matching"""
    col_names = list(columns.keys())
    
    # Exact match first
    for term in search_terms:
        if term.lower() in col_names:
            return term.lower()
    
    # Partial match
    for term in search_terms:
        for col in col_names:
            if term.lower() in col.lower() or col.lower() in term.lower():
                return col
    
    # Pattern matching
    for col in col_names:
        if any(pattern in col.lower() for pattern in ['amount', 'sales', 'revenue']):
            if search_terms[0].lower() in ['sales', 'amount', 'revenue']:
                return col
        if any(pattern in col.lower() for pattern in ['date', 'time', 'dt']):
            if search_terms[0].lower() in ['date', 'orderdate', 'time']:
                return col
    
    return None

# SOLUTION 2: ADAPTIVE QUERY BUILDER
class AdaptiveQueryBuilder:
    def __init__(self, conn):
        self.conn = conn
        self.sales_fact_cols = get_table_columns(conn, 'SALES_FACT')
        self.rbac_cols = get_table_columns(conn, 'RBAC_WORK_TABLE')
    
    def build_sales_query(self) -> str:
        """Build sales query dynamically based on actual column names"""
        
        # Find sales amount column
        sales_col = smart_column_finder(
            self.sales_fact_cols, 
            ['SalesAmount', 'sales_amount', 'amount', 'sales', 'revenue', 'total_sales']
        )
        
        # Find date column in sales_fact
        sales_date_col = smart_column_finder(
            self.sales_fact_cols,
            ['OrderDate', 'order_date', 'date', 'transaction_date', 'sale_date']
        )
        
        # Find date column in rbac
        rbac_date_col = smart_column_finder(
            self.rbac_cols,
            ['valid_from', 'VALID_FROM', 'start_date', 'effective_date', 'date']
        )
        
        # Fallback to your original column names if smart detection fails
        if not sales_col:
            sales_col = 'SalesAmount'
        if not sales_date_col:
            sales_date_col = 'OrderDate' 
        if not rbac_date_col:
            rbac_date_col = 'valid_from'
        
        # Build adaptive query
        query = f"""
        SELECT COALESCE(SUM({sales_col}), 0) 
        FROM ENTERPRISE.RETAIL_DATA.SALES_FACT 
        JOIN ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE
        ON ENTERPRISE.RETAIL_DATA.SALES_FACT.{sales_date_col} = ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE.{rbac_date_col}
        """
        
        logger.info(f"Built adaptive query: {query}")
        return query

# Knowledge base
def load_knowledge_base():
    try:
        path = os.path.join(os.path.dirname(__file__), "knowledge_base.json")
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading knowledge_base.json: {e}")
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
TOOL_NAME_TO_MEASURE = {item['tool_name']: item for item in KNOWLEDGE_BASE_DATA}
ALIAS_TO_TOOL_NAME = {}
for item in KNOWLEDGE_BASE_DATA:
    for alias in item['aliases']:
        ALIAS_TO_TOOL_NAME[alias.lower()] = item['tool_name']

# Intent recognition cache (unchanged)

_intent_cache = {}

# Cached data
_rbac_cache = None

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
    rbac_df = get_cached_rbac_data(conn)
    user_row = rbac_df[rbac_df['user_id'] == user_id]
    if user_row.empty:
        return None
    return {
        "role": user_row.iloc[0]['role'],
        "store_id": user_row.iloc[0]['store_id']
    }

def get_available_metrics_list() -> str:
    metrics = [item['measure_name'] for item in KNOWLEDGE_BASE_DATA]
    if len(metrics) <= 2:
        return " and ".join(metrics)
    else:
        return ", ".join(metrics[:-1]) + f", and {metrics[-1]}"

# Intent recognition (UNCHANGED)
def identify_metric_intent(user_query: str) -> Optional[str]:
    # Check cache first
    query_key = user_query.lower().strip()
    if query_key in _intent_cache:
        logger.info(f"Cache hit for query: '{user_query}' -> {_intent_cache[query_key]}")
        return _intent_cache[query_key]
    
    query_lower = user_query.lower()
    logger.info(f"Intent recognition for query: '{user_query}'")
    
    # Pre-filter unrelated queries
    unrelated_patterns = [
        "hello", "hi", "hey", "good morning", "good afternoon", 
        "how are you", "what's your name", "weather", "time",
        "joke", "story", "recipe", "news", "sports", "music"
    ]
    
    for pattern in unrelated_patterns:
        if pattern in query_lower and len(user_query.strip()) < 50:
            logger.info(f"Detected unrelated pattern: {pattern}")
            _intent_cache[query_key] = "UNRELATED"
            return "UNRELATED"
    
    # Direct keyword matching
    traffic_keywords = ["traffic", "conversion", "convert", "visits"]
    for keyword in traffic_keywords:
        if keyword in query_lower:
            logger.info(f"Traffic keyword match found: {keyword}")
            _intent_cache[query_key] = "traffic_conversion"
            return "traffic_conversion"
    
    sales_keywords = ["sales", "revenue", "amount", "money", "dollar"]
    for keyword in sales_keywords:
        if keyword in query_lower:
            logger.info(f"Sales keyword match found: {keyword}")
            _intent_cache[query_key] = "sales_amount"
            return "sales_amount"
    
    # Check aliases
    for alias, tool_name in ALIAS_TO_TOOL_NAME.items():
        if alias in query_lower:
            logger.info(f"Alias match found: {alias} -> {tool_name}")
            _intent_cache[query_key] = tool_name
            return tool_name
    
    # MINIMAL TOKEN LLM fallback
    if not AZURE_OPENAI_CLIENT:
        _intent_cache[query_key] = None
        return None
        
    prompt = f"'{user_query}' sales or traffic?"
    
    try:
        response = AZURE_OPENAI_CLIENT.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=8
        )
        
        result = response.choices[0].message.content.strip().lower()
        logger.info(f"LLM result: {result}")
        
        # Parse minimal response
        if "traffic" in result:
            _intent_cache[query_key] = "traffic_conversion"
            return "traffic_conversion"
        elif "sales" in result:
            _intent_cache[query_key] = "sales_amount"  
            return "sales_amount"
        else:
            _intent_cache[query_key] = "UNRELATED"
            return "UNRELATED"
            
    except Exception as e:
        logger.error(f"LLM intent error: {e}")
        _intent_cache[query_key] = None
        return None

# ADAPTIVE DATA RETRIEVAL
def get_metric_value_fast(conn, tool_name: str, store_id: int, user_id: str = "victor") -> Optional[float]:
    """Schema-adaptive data retrieval that automatically handles column changes"""
    try:
        # Check for access control based on user's store ID
        user_session = get_user_session(user_id, conn)
        if not user_session:
            return "Access denied"

        if store_id != user_session.store_id:
            # Return the custom message directly
            return "Sorry I couldn't Provide that Information. Perhaps I can help you to find the relevant data for your store"

        if tool_name == "sales_amount":
            # Create adaptive query builder
            query_builder = AdaptiveQueryBuilder(conn)
            query = query_builder.build_sales_query()
            
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()
            cur.close()
            return float(result[0]) if result else 0.0
            
        elif tool_name == "traffic_conversion":
            return 0.000
            
        return None
    except Exception as e:
        logger.error(f"Metric query error: {e}")
        return 0.0

# Response generation (unchanged)
def generate_rich_response(user_query: str, tool_name: str, metric_value: float, store_id: int) -> str:
    logger.info(f"Generating response for tool_name: {tool_name}, value: {metric_value}")
    
    measure_info = TOOL_NAME_TO_MEASURE.get(tool_name)
    if not measure_info:
        logger.error(f"No measure info found for tool_name: {tool_name}")
        return f"Metric not found for store {store_id}."
    
    # Format value
    if tool_name == "sales_amount":
        formatted_value = f"${metric_value:,.2f}"
    elif tool_name == "traffic_conversion":
        formatted_value = f"{metric_value:.3f}"
    else:
        formatted_value = f"{metric_value:.2f}"
    
    logger.info(f"Formatted value: {formatted_value}")
    
    measure_name = measure_info['measure_name']
    description = measure_info['description']
    
    base_response = f"For store {store_id}, {measure_name} is defined as {description.lower()}, and the current {measure_name} value is {formatted_value}."
    
    if tool_name == "sales_amount":
        suggestions = ' You might also ask: "How does this compare to last month?" or "What are my top performing products?"'
    elif tool_name == "traffic_conversion":
        suggestions = ' You might also ask: "What are the total store visits for my store?" or "How many sales transactions did we record to calculate this measure?"'
    else:
        suggestions = ' You might also ask: "How can I improve this metric?" or "What factors influence this measure?"'
    
    final_response = base_response + suggestions
    logger.info(f"Final response generated: {final_response[:100]}...")
    
    return final_response

# Session management (unchanged)
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

# Main message handler
async def message_handler(turn_context: TurnContext):
    if turn_context.activity.type != ActivityTypes.message:
        return
    
    user_query = turn_context.activity.text.strip()
    user_id = "victor"
    
    if not user_query or len(user_query) > 300:
        await turn_context.send_activity("Please ask a specific question about store metrics.")
        return
    
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        session = get_user_session(user_id, conn)
        if not session:
            await turn_context.send_activity("Access denied. Contact support.")
            return
        
        # Intent recognition with caching
        logger.info(f"Processing user query: '{user_query}'")
        
        tool_name = identify_metric_intent(user_query)
        logger.info(f"Identified tool_name: {tool_name}")
        
        if tool_name == "UNRELATED":
            available_metrics = get_available_metrics_list()
            unrelated_response = f"""Sorry, I couldn't provide you that information. Perhaps I can help you with these details:
            Available metrics for your store:
            •{available_metrics}
            Examples of questions I can answer:
            • "What are my sales for this month?"
            • "How's the traffic conversion rate?"
            • "Show me the current sales amount"
            What would you like to know about your store performance?"""
            
            await turn_context.send_activity(unrelated_response)
            return
        
        if not tool_name:
            available_metrics = get_available_metrics_list()
            no_match_response = f"""I'm not sure what metric you're asking about. I can help you with: {available_metrics}.
            Try asking:
            • "What are my current sales?"
            • "How's my traffic conversion?"
            • "Show me store performance data"
            What specific metric would you like to see?"""
            
            await turn_context.send_activity(no_match_response)
            return
        
        # Extract store_id from user query if mentioned
        requested_store_id = session.store_id  # Default to user's store
        user_query_lower = user_query.lower()
        
        # Simple pattern matching for "store [number]"
        store_match = re.search(r'store\s*#?(\d+)', user_query_lower)
        if store_match:
            try:
                requested_store_id = int(store_match.group(1))
            except (ValueError, IndexError):
                pass # Fallback to user's store if parsing fails
        
        # Get metric value with ADAPTIVE QUERY BUILDING
        metric_value = get_metric_value_fast(conn, tool_name, requested_store_id, user_id)
        logger.info(f"Retrieved metric_value: {metric_value} for tool_name: {tool_name}")
        
        # CHECK FOR THE CUSTOM MESSAGE
        if metric_value == "Sorry I couldn't Provide that Information. Perhaps I can help you to find the relevant data for your store":
            await turn_context.send_activity(metric_value)
            return
    
        if metric_value is None:
            await turn_context.send_activity(f"Cannot retrieve data for store {requested_store_id}.")
            return
        
        # Generate response - NO API CALLS
        response = generate_rich_response(user_query, tool_name, metric_value, requested_store_id)
        
        await turn_context.send_activity(response)
        
        session.last_queries.append({"query": user_query, "metric": tool_name})
        if len(session.last_queries) > 3:
            session.last_queries.pop(0)
            
    except Exception as e:
        logger.error(f"Error in message_handler: {e}")
        await turn_context.send_activity("Service temporarily unavailable. Please try again.")
    finally:
        if conn:
            conn.close()

# Main function (unchanged)
def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == 'GET':
        return func.HttpResponse("Bot endpoint is healthy", status_code=200)

    if req.method != 'POST':
        return func.HttpResponse("Only POST requests supported.", status_code=405)

    try:
        req_json = req.get_json()
        activity = Activity.deserialize(req_json)
        auth_header = req.headers.get('Authorization') or req.headers.get('authorization') or ''

        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            credentials = SingleTenantAppCredentials(APP_ID, APP_PASSWORD, APP_TENANT_ID)
        else:
            credentials = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)

        settings = BotFrameworkAdapterSettings(app_id=APP_ID, app_password=APP_PASSWORD)
        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            settings.oauth_endpoint = f"https://login.microsoftonline.com/{APP_TENANT_ID}"

        adapter = BotFrameworkAdapter(settings)
        adapter._credentials = credentials

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
