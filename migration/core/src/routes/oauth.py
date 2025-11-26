from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from typing import Optional
import base64
from utils.logger import logger
from utils.oauth import OAuth2Client

router = APIRouter()

@router.get("/authorize", response_class=HTMLResponse)
async def auth_page():
    """Show authorization page with available auth URLs"""
    try:
        oauth_client = OAuth2Client()
        auth_items = oauth_client.get_auth_items()
        return OAuth2Client.get_auth_page_html(auth_items)

    except Exception as e:
        logger.error(f"Error in auth page: {str(e)}")
        return OAuth2Client.get_error_html(str(e))

@router.get("/callback", response_class=HTMLResponse)
async def oauth_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None
):
    """OAuth2 callback handler"""
    try:
        if error:
            error_msg = f"OAuth error: {error}"
            if error_description:
                error_msg += f" - {error_description}"
            logger.error(error_msg)
            return OAuth2Client.get_error_html(error_msg)

        if not code or not state:
            return OAuth2Client.get_error_html("Missing required parameters (code or state)")

        try:
            client_id = base64.b64decode(state).decode('utf-8')
        except Exception as e:
            logger.error(f"Invalid state parameter: {str(e)}")
            return OAuth2Client.get_error_html("Invalid state parameter")

        # Create temporary client for finding config
        temp_client = OAuth2Client()
        auth_config = await temp_client.find_auth_config(client_id)
        if not auth_config:
            return OAuth2Client.get_error_html(f"Invalid client configuration: {client_id}")

        oauth_client = OAuth2Client({
            'client_id': auth_config['client_id'],
            'client_secret': auth_config['client_secret'],
            'auth_endpoint': f"{auth_config['domain']}/oauth/authorize",
            'token_endpoint': f"{auth_config['domain']}/oauth/token",
            'domain': auth_config['domain']
        })

        token_data = await oauth_client.exchange_code_for_token(code)
        if not token_data:
            error_msg = "Failed to exchange code for token. Please check the logs for details."
            logger.error(f"Token exchange failed for client: {client_id}")
            return OAuth2Client.get_error_html(error_msg)

        await oauth_client.save_token(client_id, token_data)
        return OAuth2Client.get_success_html()

    except Exception as e:
        logger.error(f"Callback error: {str(e)}")
        return OAuth2Client.get_error_html(f"An unexpected error occurred: {str(e)}")