import azure.functions as func
import os
import asyncio
import base64
import json
import jwt
import logging
import sys
import aiohttp
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import MicrosoftAppCredentials
from botframework.connector import ConnectorClient

# Setup logging
logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)

# Get environment variables
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
APP_TYPE = os.environ.get("MicrosoftAppType", "SingleTenant")
APP_TENANT_ID = os.environ.get("MicrosoftAppTenantId", "")

logger.info(f"Bot configured: AppId={APP_ID[:8]}..., Type={APP_TYPE}, TenantId={APP_TENANT_ID[:8] if APP_TENANT_ID else 'None'}...")

def decode_jwt_payload(token):
    """Decode JWT payload without verification for debugging"""
    try:
        if not token or not token.startswith('Bearer '):
            return None
            
        jwt_token = token.replace('Bearer ', '')
        
        # Decode header and payload (without verification)
        header = jwt.get_unverified_header(jwt_token)
        payload = jwt.decode(jwt_token, options={"verify_signature": False})
        
        return {
            "header": header,
            "payload": payload
        }
    except Exception as e:
        logger.error(f"Error decoding JWT: {e}")
        return None

async def diagnose_token_issue(activity, auth_header):
    """Diagnose token validation issues"""
    logger.info("=== DIAGNOSTIC ANALYSIS ===")
    
    # 1. Check incoming auth header
    if auth_header:
        logger.info("Auth header present: True")
        decoded = decode_jwt_payload(auth_header)
        if decoded:
            logger.info(f"Incoming token audience: {decoded['payload'].get('aud')}")
            logger.info(f"Incoming token issuer: {decoded['payload'].get('iss')}")
            logger.info(f"Incoming token app id: {decoded['payload'].get('appid')}")
            logger.info(f"Incoming token service url: {decoded['payload'].get('serviceurl')}")
    else:
        logger.info("Auth header present: False")
    
    # 2. Check service URL
    logger.info(f"Activity service URL: {activity.service_url}")
    
    # 3. Check our app credentials
    logger.info(f"Our App ID: {APP_ID}")
    logger.info(f"Our App Type: {APP_TYPE}")
    
    # 4. Try to get our own token for comparison
    try:
        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            token_url = f"https://login.microsoftonline.com/{APP_TENANT_ID}/oauth2/v2.0/token"
        else:
            token_url = "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token"
            
        data = {
            'grant_type': 'client_credentials',
            'client_id': APP_ID,
            'client_secret': APP_PASSWORD,
            'scope': 'https://api.botframework.com/.default'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, data=data) as response:
                if response.status == 200:
                    token_response = await response.json()
                    decoded_our = decode_jwt_payload(f"Bearer {token_response.get('access_token')}")
                    if decoded_our:
                        logger.info("Successfully got our access token")
                        logger.info(f"Our token audience: {decoded_our['payload'].get('aud')}")
                        logger.info(f"Our token issuer: {decoded_our['payload'].get('iss')}")
                        logger.info(f"Our token app id: {decoded_our['payload'].get('appid')}")
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get our token: {response.status} - {error_text}")
                    
    except Exception as e:
        logger.error(f"Exception getting our token: {e}")
    
    logger.info("=== END DIAGNOSTIC ===")

# Error handler
async def on_error(context: TurnContext, error: Exception):
    logger.error(f"Error in bot: {str(error)}", exc_info=True)
    try:
        await context.send_activity("Sorry, an error occurred. Please try again.")
    except Exception as send_error:
        logger.error(f"Error sending error message: {str(send_error)}")

async def bot_logic(turn_context: TurnContext):
    try:
        # Log ALL activity types for debugging
        logger.info(f"Activity type: {turn_context.activity.type}")
        logger.info(f"Channel: {turn_context.activity.channel_id}")
        
        if turn_context.activity.type == ActivityTypes.message:
            user_message = turn_context.activity.text or ""
            logger.info(f"Received message: {user_message}")
            
            # Log adapter credentials info
            if hasattr(turn_context.adapter, '_credentials') and turn_context.adapter._credentials:
                creds = turn_context.adapter._credentials
                logger.info(f"Adapter credentials App ID: {creds.microsoft_app_id}")
                logger.info(f"Credentials type: {type(creds)}")
            else:
                logger.error("CRITICAL: Bot adapter credentials not found or are None")
                
            # Try to send response
            response_text = f"Echo: {user_message}"
            logger.info(f"Preparing to send response: {response_text}")
            
            try:
                await turn_context.send_activity(response_text)
                logger.info(f"Successfully sent response: {response_text}")
            except Exception as send_error:
                logger.error(f"Send failed with error: {str(send_error)}", exc_info=True)
                
                # Additional debugging
                logger.info(f"Service URL being used: {turn_context.activity.service_url}")
                logger.info(f"Conversation ID: {turn_context.activity.conversation.id}")
                logger.info(f"Channel ID: {turn_context.activity.channel_id}")
                
                raise send_error
        
        elif turn_context.activity.type == ActivityTypes.members_added:
            # Handle welcome messages
            if hasattr(turn_context.activity, 'members_added') and turn_context.activity.members_added:
                for member in turn_context.activity.members_added:
                    if member.id != turn_context.activity.recipient.id:
                        welcome_text = "Hello! I'm your echo bot. Send me a message and I'll echo it back!"
                        await turn_context.send_activity(welcome_text)
                        logger.info(f"Sent welcome message to new member: {member.id}")
                    
        else:
            # Log other activity types for debugging
            logger.info(f"Received non-message activity: {turn_context.activity.type}")
                
    except Exception as e:
        logger.error(f"Error in bot_logic: {str(e)}", exc_info=True)
        raise

# MAIN FUNCTION
async def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main Azure Function entry point
    """
    try:
        logger.info(f"Processing bot request... Method: {req.method}")
        
        if req.method == 'GET':
            return func.HttpResponse("Bot endpoint is healthy", status_code=200)
        
        if req.method != 'POST':
            return func.HttpResponse("Only GET and POST requests are supported", status_code=405)
        
        body = req.get_json()
        if not body:
            return func.HttpResponse("Request body is required", status_code=400)
        
        activity = Activity.deserialize(body)
        auth_header = req.headers.get('Authorization') or req.headers.get('authorization') or ''

        # CRITICAL FIX: Create adapter with explicit credentials and custom connector client
        try:
            # Validate environment variables first
            if not APP_ID or not APP_PASSWORD:
                raise ValueError("MicrosoftAppId and MicrosoftAppPassword must be set")
            
            # Create explicit credentials with proper tenant configuration
            credentials = MicrosoftAppCredentials(
                app_id=APP_ID,
                password=APP_PASSWORD,
                channel_auth_tenant=APP_TENANT_ID if APP_TYPE == "SingleTenant" else None
            )
            
            logger.info(f"Created credentials for App ID: {credentials.microsoft_app_id}")
            logger.info(f"Channel auth tenant: {APP_TENANT_ID if APP_TYPE == 'SingleTenant' else 'None (MultiTenant)'}")
            
            # Force token refresh to validate credentials
            try:
                token = credentials.get_access_token()
                logger.info("Successfully validated credentials with token refresh")
                logger.info(f"Token acquired: {token[:20]}..." if token else "No token received")
            except Exception as token_error:
                logger.error(f"Failed to get access token: {str(token_error)}")
                raise token_error
            
            # Create adapter settings
            settings = BotFrameworkAdapterSettings(
                app_id=APP_ID,
                app_password=APP_PASSWORD
            )
            
            # Create adapter
            adapter = BotFrameworkAdapter(settings)
            
            # CRITICAL: Override the connector client creation to use our validated credentials
            original_create_connector_client = adapter.create_connector_client
            
            def create_connector_client_override(service_url: str):
                logger.info(f"Creating connector client for service URL: {service_url}")
                return ConnectorClient(credentials, base_url=service_url)
            
            adapter.create_connector_client = create_connector_client_override
            
            # Explicitly set credentials on adapter as backup
            adapter._credentials = credentials
            
            # Set error handler
            adapter.on_turn_error = on_error
            
            logger.info("Adapter created successfully with custom connector client")
            
        except Exception as adapter_error:
            logger.error(f"Failed to create adapter: {str(adapter_error)}", exc_info=True)
            return func.HttpResponse(f"Adapter creation failed: {str(adapter_error)}", status_code=500)
        
        logger.info(f"Activity type: {activity.type}, Channel: {activity.channel_id}")
        logger.info(f"Service URL: {activity.service_url}")
        
        # Run diagnostics
        await diagnose_token_issue(activity, auth_header)
        
        # Process the activity
        try:
            invoke_response = await adapter.process_activity(activity, auth_header, bot_logic)
            
            if invoke_response:
                logger.info(f"Bot returned response with status: {invoke_response.status}")
                return func.HttpResponse(
                    body=invoke_response.body,
                    status_code=invoke_response.status,
                    headers={"Content-Type": "application/json"}
                )
            else:
                logger.info("Bot processing completed successfully (no response)")
                return func.HttpResponse(status_code=202)
                
        except Exception as process_error:
            logger.error(f"Error processing activity: {str(process_error)}", exc_info=True)
            return func.HttpResponse(f"Processing error: {str(process_error)}", status_code=500)
            
    except Exception as error:
        logger.error(f"Unhandled error in main: {str(error)}", exc_info=True)
        return func.HttpResponse(f"Internal server error: {str(error)}", status_code=500)
