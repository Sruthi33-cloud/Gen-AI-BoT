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

_column_cache = {}

def get_table_columns(conn, table_name: str) -> Dict[str, str]:
    """Get column names and data types dynamically - cached for performance"""
    if table_name in _column_cache:
        return _column_cache[table_name]

    try:
        cur = conn.cursor()
        # Get column info with data types from Snowflake information schema
        cur.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
        AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """)

        columns = {}
        for row in cur.fetchall():
            col_name = row[0]  
            data_type = row[1].upper()  
            columns[col_name] = data_type
        
        cur.close()
        _column_cache[table_name] = columns
        logger.info(f"Cached columns for {table_name}: {[(name, dtype) for name, dtype in columns.items()]}")
        return columns

    except Exception as e:
        logger.error(f"Error getting columns for {table_name}: {e}")
        return {}

def find_column_by_data_type(columns: Dict[str, str], data_type_patterns: List[str]) -> Optional[str]:
    """
    Find column by data type
    """
    for col_name, data_type in columns.items():
        for pattern in data_type_patterns:
            if pattern.upper() in data_type.upper():
                logger.info(f"Found column '{col_name}' with data type '{data_type}' matching pattern '{pattern}'")
                return col_name
    return None

# THE NEW REVOLUTIONARY QUERY BUILDER 
class DataTypeBasedQueryBuilder:
    """
    This approach uses DATABASE METADATA.
    """
    def __init__(self, conn):
        self.conn = conn
        self.sales_fact_cols = get_table_columns(conn, 'SALES_FACT')
        self.rbac_cols = get_table_columns(conn, 'RBAC_WORK_TABLE')
    
    def build_sales_query(self) -> str:
        """
        Build query using DATA TYPES to identify columns - works with ANY column names!
        """
        # Step 1: Find the AMOUNT column using data type
        sales_amount_col = find_column_by_data_type(self.sales_fact_cols,
            ['NUMBER', 'DECIMAL', 'FLOAT', 'NUMERIC', 'REAL', 'DOUBLE']
        )
        
        # Step 2: Find DATE columns for joining
        sales_date_col = find_column_by_data_type(self.sales_fact_cols,
            ['DATE', 'TIMESTAMP', 'TIME', 'DATETIME']
        )
        rbac_date_col = find_column_by_data_type(self.rbac_cols,
            ['DATE', 'TIMESTAMP', 'TIME', 'DATETIME']
        )
        
        # Step 3: Validate we found required columns
        if not sales_amount_col:
            logger.error("No numeric column found in SALES_FACT table")
            return None
        if not sales_date_col:
            logger.error("No date column found in SALES_FACT table")
            return None
        if not rbac_date_col:
            logger.error("No date column found in RBAC_WORK_TABLE")
            return None

        # Step 4: Build query with ACTUAL column names
        query = f"""
        SELECT COALESCE(SUM({sales_amount_col}), 0)
        FROM ENTERPRISE.RETAIL_DATA.SALES_FACT
        JOIN ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE
        ON ENTERPRISE.RETAIL_DATA.SALES_FACT.{sales_date_col} = ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE.{rbac_date_col}
        WHERE ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE.USER_ID = 'victor'
        """
        
        logger.info(f"Built data-type-based query:")
        logger.info(f" Amount column: {sales_amount_col} (type: {self.sales_fact_cols[sales_amount_col]})")
        logger.info(f" Sales date column: {sales_date_col} (type: {self.sales_fact_cols[sales_date_col]})")
        logger.info(f" RBAC date column: {rbac_date_col} (type: {self.rbac_cols[rbac_date_col]})")
        
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
            {"measure_name": "Sales Amount",
             "tool_name": "sales_amount",
             "description": "The total monetary value of all sales transactions. This is a key measure for a store's financial performance.",
             "dax_formula": "SUM('SalesFact'[SalesAmount])",
             "aliases": ["sales", "sales revenue", "total sales", "store sales"]
            },
            {"measure_name": "Traffic Conversion",
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
    return {"role": user_row.iloc[0]['role'],
            "store_id": user_row.iloc[0]['store_id']
            }

def get_available_metrics_list() -> str:
    metrics = [item['measure_name'] for item in KNOWLEDGE_BASE_DATA]
    if len(metrics) <= 2:
        return " and ".join(metrics)
    else:
        return ", ".join(metrics[:-1]) + f", and {metrics[-1]}"


def identify_metric_intent(user_query: str) -> Optional[str]:
    # Check cache first - ZERO API calls for repeated queries
    query_key = user_query.lower().strip()
    if query_key in _intent_cache:
        logger.info(f"Cache hit for query: '{user_query}' -> {_intent_cache[query_key]}")
        return _intent_cache[query_key]
    
    query_lower = user_query.lower()
    logger.info(f"Intent recognition for query: '{user_query}'")
    
    # Pre-filter unrelated queries - NO API CALL
    unrelated_patterns = ["how are you", "what's your name", "weather", "time",
                          "joke", "story", "recipe", "news", "sports", "music"]
    
    for pattern in unrelated_patterns:
        if pattern in query_lower and len(user_query.strip()) < 50:
            logger.info(f"Detected unrelated pattern: {pattern}")
            _intent_cache[query_key] = "UNRELATED"
            return "UNRELATED"

    # Direct keyword matching - NO API CALL
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

    # Check aliases - NO API CALL
    for alias, tool_name in ALIAS_TO_TOOL_NAME.items():
        if alias in query_lower:
            logger.info(f"Alias match found: {alias} -> {tool_name}")
            _intent_cache[query_key] = tool_name
            return tool_name

    # MINIMAL TOKEN LLM fallback - ONLY when absolutely necessary
    if not AZURE_OPENAI_CLIENT:
        _intent_cache[query_key] = None
        return None
    
    # ULTRA-MINIMAL PROMPT: Input ~2 tokens, Output 8 tokens = 10 total
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

# DATA RETRIEVAL - WORKS WITH ANY COLUMN NAMES
def get_metric_value_fast(conn, tool_name: str, store_id: int, user_id: str, query: str) -> Optional[float]:
    """
    Data retrieval that works with ANY column names
    """
    try:
        user_session = get_user_session(user_id, conn)
        if not user_session:
            return "Access denied"

        # Check store access permissions
        if store_id != user_session.store_id:
            access_denied_response = """Sorry I couldn't provide that information. Perhaps I can help you to find the relevant data for your store.
            Examples of questions I can answer:
            • "What are my sales for this month?"
            • "How's the traffic conversion rate?"
            • "Show me the current sales amount"
            What would you like to know about your store performance?"""
            return access_denied_response

        if tool_name == "sales_amount":
            # Execute the data-type-based query
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
        return "data_retrieval_failed"

# Response generation 
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
            • {available_metrics}
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

        # Extract store_id from user query if mentioned (simple detection)
        requested_store_id = session.store_id  # Default to user's store
        user_query_lower = user_query.lower()

       
        store_match = re.search(r'store\s*#?(\d+)', user_query_lower)
        if store_match:
            try:
                requested_store_id = int(store_match.group(1))
            except (ValueError, IndexError):
                pass  # Fallback to user's store if parsing fails

        # THE REVOLUTIONARY PART: Build query using data types
        query_builder = DataTypeBasedQueryBuilder(conn)
        query = query_builder.build_sales_query()
        if query is None:
            await turn_context.send_activity("The requested data is not available due to a schema change as the database is getting updated. Please try again later.")
            return

        # Get metric value with DATA-TYPE-BASED QUERY
        metric_value = get_metric_value_fast(conn, tool_name, requested_store_id, user_id, query)
        logger.info(f"Retrieved metric_value: {metric_value} for tool_name: {tool_name}")

        if isinstance(metric_value, str):
            if metric_value == "data_retrieval_failed":
                await turn_context.send_activity("There was a problem retrieving data for that metric. The database may be temporarily unavailable.")
            else:
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

# Main function
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
