import logging
import os
import json
import azure.functions as func

# Bot Framework SDK Libraries
from botbuilder.core import BotFrameworkAdapter, TurnContext
from botbuilder.schema import Activity, ActivityTypes

# Your Existing Code's Libraries
import pandas as pd
import requests
import snowflake.connector
import random
from openai import AzureOpenAI
from time import sleep

# --- Configuration: Get environment variables from Azure Function App settings ---
APP_ID = os.environ.get("MicrosoftAppId")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword")

# Azure OpenAI Parameters
AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME")

# Snowflake Connection Parameters
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")

# --- Initialize the Bot Framework Adapter ---
ADAPTER = BotFrameworkAdapter(APP_ID, APP_PASSWORD)

# --- Initialize the Azure OpenAI Client ---
AZURE_OPENAI_CLIENT = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version="2024-02-15-preview"
)

# --- Dynamic knowledge base loading from a file ---
def load_knowledge_base():
    try:
        # Assumes knowledge_base.json is in the same directory as the function code
        with open(os.path.join(os.path.dirname(__file__), "knowledge_base.json"), "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("knowledge_base.json not found. Please ensure it is in the same folder.")
        return []

KNOWLEDGE_BASE_DATA = load_knowledge_base()

# --- Snowflake Connection & Data Utilities ---
def get_rbac_table(conn):
    query = "SELECT USERNAME, ROLE, LOCATION_ID FROM ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE"
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'username': 'user_id', 'role': 'role', 'location_id': 'store_id'})
    return df

def get_sales_territory_keys(conn):
    query = "SELECT DISTINCT SalesTerritoryKey FROM ENTERPRISE.RETAIL_DATA.SALES_FACT ORDER BY SalesTerritoryKey"
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    return df['SALESTERRITORYKEY'].tolist()

def create_location_to_territory_mapping(rbac_df, territory_keys):
    all_locations = sorted(rbac_df['store_id'].unique().tolist())
    location_to_territory_map = {}
    shuffled_territory_keys = territory_keys[:]
    random.shuffle(shuffled_territory_keys)
    territory_count = len(shuffled_territory_keys)
    for i, location_id in enumerate(all_locations):
        assigned_territory_key = shuffled_territory_keys[i % territory_count]
        location_to_territory_map[location_id] = assigned_territory_key
    return location_to_territory_map

def get_user_access(user_id, rbac_df):
    user_row = rbac_df[rbac_df['user_id'] == user_id]
    if user_row.empty:
        return None
    return {
        "role": user_row.iloc[0]['role'],
        "store_id": user_row.iloc[0]['store_id']
    }

def get_metric_value(conn, measure, store_id, location_to_territory_map):
    sales_territory_key = location_to_territory_map.get(store_id)
    if sales_territory_key is None:
        return {}
    if measure['measure_name'].lower() == "sales amount":
        query = f"""
            SELECT SUM(SalesAmount) AS total_sales_amount
            FROM ENTERPRISE.RETAIL_DATA.SALES_FACT
            WHERE SalesTerritoryKey = {sales_territory_key}
        """
        try:
            cur = conn.cursor()
            cur.execute(query)
            df_sales = cur.fetch_pandas_all()
            cur.close()
            df_sales.columns = df_sales.columns.str.lower()
            total_sales = df_sales['total_sales_amount'][0] if not df_sales.empty else 0
            return {"Sales Amount": total_sales}
        except Exception as e:
            logging.error(f"Error querying Snowflake for Sales Amount: {e}")
            return {"Sales Amount": 0.0}
    if measure['measure_name'].lower() == "traffic conversion":
        return {"Traffic Conversion": 0.0}
    return {}

def find_measure_with_llm(user_query, knowledge_base):
    measure_names = [item['measure_name'] for item in knowledge_base]
    prompt = f"""
    The user's query is: "{user_query}"
    Based on the query, identify the most relevant measure from the following list.
    If a measure is clearly mentioned or implied, respond ONLY with the measure name, nothing else.
    If no measure is found, respond ONLY with the text "NO_MEASURE_FOUND".
    Available measures: {', '.join(measure_names)}
    Example 1:
    Query: "What is my total sales amount for the month?"
    Response: Sales Amount
    Your response for the user's query:
    """
    try:
        identified_measure = call_azure_openai(prompt, temperature=0.0, max_tokens=100).strip().replace('.', '')
        if identified_measure.lower() in [m.lower() for m in measure_names]:
            for measure in knowledge_base:
                if measure['measure_name'].lower() == identified_measure.lower():
                    return measure
    except Exception as e:
        logging.error(f"Error during LLM intent recognition: {e}")
    return None

def build_llm_prompt(user, access, user_query, measure, sf_data=None, location_to_territory_map=None):
    if not measure or not access or location_to_territory_map is None:
        return "Sorry, I couldn't find the measure, your access info, or a valid data map."
    sales_territory_key = location_to_territory_map.get(access['store_id'])
    prompt_template = f"""
    You are an AI assistant for a retail data team. Your user is a {access['role']} for store {access['store_id']} (which maps to sales territory {sales_territory_key}).
    The user's query is: '{user_query}'
    Here is the official definition of the requested measure and its current value for the user's store:
    Description: {measure['description']}
    DAX Formula: {measure['dax_formula']}
    """
    if sf_data and measure['measure_name'] in sf_data:
        data_value = sf_data[measure['measure_name']]
        if measure['measure_name'] == "Sales Amount":
            data_string = f"${data_value:,.2f}"
        else:
            data_string = f"{data_value:.3f}"
        prompt_template += f"\nFor your store ({access['store_id']}), the current {measure['measure_name']} is {data_string}."

    prompt_template += f"""
    Based ONLY on this information, provide a clear and concise answer to the user's question. After your answer, suggest 2-3 similar questions with insights and relevant measures that the user might want to ask next. Do not use any bolding, formatting, or headers like 'Answer:' or 'Suggestions:'. Just provide the response as plain text.
    Your response:
    """
    return prompt_template.strip()

def call_azure_openai(prompt, temperature=0.7, max_tokens=500):
    if not AZURE_OPENAI_KEY or not AZURE_OPENAI_ENDPOINT or not AZURE_OPENAI_DEPLOYMENT_NAME:
        logging.error("Azure OpenAI configuration missing.")
        return None
    
    try:
        response = AZURE_OPENAI_CLIENT.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Azure OpenAI API Error: {e}")
        return None

# --- The Core Bot Logic Handler ---
async def message_handler(turn_context: TurnContext):
    user_query = turn_context.activity.text
    # Get dynamic user ID from the Teams user
    teams_user_id = turn_context.activity.from_property.id
    final_answer = ""
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
        
        rbac_df = get_rbac_table(conn)
        territory_keys = get_sales_territory_keys(conn)
        location_to_territory_map = create_location_to_territory_mapping(rbac_df, territory_keys)
        
        # --- DYNAMIC USER MAPPING ---
        # Look up the Snowflake username from the Teams user ID
        # For a simple demo, you can use a hardcoded map
        user_id_mapping = {
            "29:1f77d853-90d5-4554-b5b5-f55e5898d9c5": "saisri", # Example Teams ID for user 'saisri'
            "29:1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d": "dev" # Example Teams ID for user 'dev'
        }
        
        rbac_user_id = user_id_mapping.get(teams_user_id, None)
        
        if not rbac_user_id:
            final_answer = "Sorry, your access could not be verified. Please ensure your account is registered."
        else:
            access = get_user_access(rbac_user_id, rbac_df)
            
            if not access:
                final_answer = "Sorry, your access information could not be found in the RBAC table."
            else:
                measure = find_measure_with_llm(user_query, KNOWLEDGE_BASE_DATA)
                if measure:
                    sf_data = get_metric_value(conn, measure, access['store_id'], location_to_territory_map)
                    llm_prompt = build_llm_prompt(
                        {"user_id": rbac_user_id},
                        access,
                        user_query,
                        measure,
                        sf_data=sf_data,
                        location_to_territory_map=location_to_territory_map
                    )
                    final_answer = call_azure_openai(llm_prompt)
                else:
                    final_answer = "Sorry, I can't find that measure. Please ask about sales, traffic, or basket size."

    except Exception as e:
        logging.error(f"Error processing bot request: {e}")
        final_answer = "Sorry, an internal error occurred while processing your request. Please try again later."
    finally:
        if conn:
            conn.close()

    await turn_context.send_activity(Activity(text=final_answer, type=ActivityTypes.message))


# --- The Main Azure Functions Entry Point ---
# The new recommended Azure Functions for Python V2 approach
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="Data", methods=["POST"])
async def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function received a request.')
    
    try:
        response = await ADAPTER.process_activity(
            req.get_json(),
            req.headers,
            message_handler
        )
        return func.HttpResponse(
            body=response.body,
            mimetype=response.content_type,
            status_code=response.status
        )
    except Exception as e:
        logging.error(f"Error processing bot request: {e}")
        return func.HttpResponse(
            "An error occurred while processing the request.",
            status_code=500
        )
