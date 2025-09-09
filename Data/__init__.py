import azure.functions as func
import os
import asyncio
import base64
import json
import jwt
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import MicrosoftAppCredentials
import logging
import sys
import aiohttp

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
BOT_REGION = os.environ.get("MicrosoftAppRegion", "canadacentral")

logger.info(f"Bot configured: AppId={APP_ID[:8]}..., Type={APP_TYPE}, Tenant={APP_TENANT_ID}, Region={BOT_REGION}")

class SingleTenantAppCredentials(MicrosoftAppCredentials):
    """Custom credentials class that forces single-tenant OAuth endpoint"""
    
    def __init__(self, app_id: str, password: str, tenant_id: str):
        super().__init__(app_id, password)
        self.tenant_id = tenant_id
        # Override the OAuth endpoint for single-tenant
        self.oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        logger.info(f"SingleTenantAppCredentials initialized with endpoint: {self.oauth_endpoint}")

def decode_jwt_payload(token, label=""):
    """Decode JWT payload without verification for debugging"""
    try:
        if not token or not token.startswith('Bearer '):
            logger.info(f"{label} No bearer token present for decoding.")
            return None
        jwt_token = token.replace('Bearer ', '')

        header = jwt.get_unverified_header(jwt_token)
        payload = jwt.decode(jwt_token, options={"verify_signature": False})

        logger.info(f"{label} JWT Header: {json.dumps(header)}")
        logger.info(f"{label} JWT Payload: {json.dumps(payload)}")
        return {
            "header": header,
            "payload": payload
        }
    except Exception as e:
        logger.error(f"{label} Error decoding JWT: {e}")
        return None

async def diagnose_token_issue(activity, auth_header):
    """Diagnose token validation issues"""
    logger.info("==== TOKEN DIAGNOSTICS ====")
    # 1. Check incoming auth header
    if auth_header:
        logger.info("Incoming Auth header present.")
        decode_jwt_payload(auth_header, label="INCOMING")
    else:
        logger.info("Incoming Auth header missing.")

    # 2. Service URL
    logger.info(f"Activity service URL: {activity.service_url}")

    # 3. App credentials
    logger.info(f"Our App ID: {APP_ID}")
    logger.info(f"Our App Type: {APP_TYPE}")
    logger.info(f"Our Tenant ID: {APP_TENANT_ID}")
    logger.info(f"Our Region: {BOT_REGION}")

    # 4. Get outgoing token for comparison
    try:
        token_url = f"https://login.microsoftonline.com/{APP_TENANT_ID}/oauth2/v2.0/token" if APP_TYPE == "SingleTenant" else "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': APP_ID,
            'client_secret': APP_PASSWORD,
            'scope': 'https://api.botframework.com/.default'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, data=data) as response:
                token_response = await response.json()
                logger.info(f"Raw outgoing token response: {json.dumps(token_response)}")
                if response.status == 200 and "access_token" in token_response:
                    token = token_response.get('access_token')
                    logger.info("Successfully got outgoing access token from Azure AD.")
                    decode_jwt_payload(f"Bearer {token}", label="OUTGOING")
                else:
                    logger.error(f"Token response missing 'access_token' or error. Status: {response.status}, Full response: {json.dumps(token_response)}")
    except Exception as e:
        logger.error(f"Exception getting outgoing token: {e}", exc_info=True)

    logger.info("==== END TOKEN DIAGNOSTICS ====")

async def bot_logic(turn_context: TurnContext):
    try:
        if turn_context.activity.type == ActivityTypes.message:
            user_message = turn_context.activity.text or ""
            logger.info(f"Received message: {user_message}")
            response_text = f"Echo: {user_message}"
            logger.info(f"Preparing to send response: {response_text}")
            await turn_context.send_activity(response_text)
            logger.info(f"Successfully sent response: {response_text}")
        elif turn_context.activity.type == ActivityTypes.members_added:
            if hasattr(turn_context.activity, 'members_added') and turn_context.activity.members_added:
                for member in turn_context.activity.members_added:
                    if member.id != turn_context.activity.recipient.id:
                        await turn_context.send_activity("Hello! I'm an echo bot. Send me a message!")
                        logger.info("Sent welcome message to new member")
        else:
            logger.info(f"Received activity type: {turn_context.activity.type} - no action taken")
    except Exception as e:
        logger.error(f"Error in bot_logic: {str(e)}", exc_info=True)
        raise

async def main(req: func.HttpRequest) -> func.HttpResponse:
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

        # CRITICAL FIX: Use custom credentials class for single-tenant
        try:
            if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
                # Use our custom single-tenant credentials
                credentials = SingleTenantAppCredentials(APP_ID, APP_PASSWORD, APP_TENANT_ID)
                logger.info("Using SingleTenantAppCredentials")
            else:
                # Use default multi-tenant credentials
                credentials = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)
                logger.info("Using standard MicrosoftAppCredentials")

            # Create adapter settings with tenant-specific OAuth endpoint
            settings = BotFrameworkAdapterSettings(
                app_id=APP_ID,
                app_password=APP_PASSWORD
            )
            
            # Override OAuth endpoint for single-tenant
            if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
                settings.oauth_endpoint = f"https://login.microsoftonline.com/{APP_TENANT_ID}/oauth2/v2.0/token"
                logger.info(f"Set OAuth endpoint to: {settings.oauth_endpoint}")

            adapter = BotFrameworkAdapter(settings)
            
            # Force use our custom credentials
            adapter._credentials = credentials
            
            logger.info("Adapter created with single-tenant credentials")

        except Exception as adapter_error:
            logger.error(f"Failed to create adapter: {str(adapter_error)}", exc_info=True)
            return func.HttpResponse(f"Adapter creation failed: {str(adapter_error)}", status_code=500)

        logger.info(f"Activity type: {activity.type}, Channel: {activity.channel_id}")
        logger.info(f"Service URL: {activity.service_url}")

        # Token diagnostics for both incoming and outgoing tokens
        await diagnose_token_issue(activity, auth_header)

        # Process the activity
        try:
            invoke_response = await adapter.process_activity(activity, auth_header, bot_logic)

            if invoke_response:
                logger.info("Bot processing completed with response")
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
