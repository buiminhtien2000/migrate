import os
import json
import base64
import time
import aiohttp
from typing import Optional, Dict, List, Any
from utils.logger import logger
from config import config
from urllib.parse import urlparse

class OAuth2Client:
    def __init__(self, info: Dict = {}):
        # Init auth directories and files
        self.auth_dir = config.AUTH_DIR
        self.source_auth_file = os.path.join(self.auth_dir, 'source-auth.json')
        self.target_auth_file = os.path.join(self.auth_dir, 'target-auth.json')
        self.tokens_file = os.path.join(self.auth_dir, 'tokens.json')
        
        # Ensure files exist
        self._ensure_auth_files()
        
        # Init OAuth2 params
        self.client_id = info.get('client_id')
        self.client_secret = info.get('client_secret')
        self.auth_endpoint = info.get('auth_endpoint')
        self.token_endpoint = info.get('token_endpoint')
        self.domain = self._normalize_domain(info.get('domain', ''))
        self.redirect_uri = f"https://{config.DOMAIN}/oauth/callback"

        self.extra_headers = info.get('extra_headers', {})

        if self.auth_endpoint is not None:
            self.auth_endpoint = self.auth_endpoint.replace('{domain}', self.domain)

        if self.token_endpoint is not None:    
            self.token_endpoint = self.token_endpoint.replace('{domain}', self.domain)

    def _normalize_domain(self, domain: str) -> str:
        """
        Normalize domain string to return only domain name
        Examples:
            - 'demo.com' -> 'demo.com'
            - 'https://demo.com' -> 'demo.com'
            - 'https://demo.com/' -> 'demo.com'
            - 'http://demo.com' -> 'demo.com'
        """
        
        if not domain:
            return ''
        
        # Remove trailing slashes
        domain = domain.rstrip('/')
        
        # Parse URL if it has scheme
        if '://' in domain:
            parsed = urlparse(domain)
            domain = parsed.netloc
        
        return domain

    def _ensure_auth_files(self):
        """Ensure all required auth files exist"""
        try:
            if not os.path.exists(self.auth_dir):
                os.makedirs(self.auth_dir)

            for file_path in [self.source_auth_file, self.target_auth_file, self.tokens_file]:
                if not os.path.exists(file_path):
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump([] if file_path != self.tokens_file else {}, f, indent=2)

        except Exception as e:
            logger.error(f"Error ensuring auth files: {str(e)}")
            raise
    
    def get_token_info(self) -> Optional[Dict]:
        """
        Get token information from token file by client_id
        Args:
            client_id: client id to get token info
        Returns:
            Dict: token info if found
            None: if not found or error
        """
        try:
            # Check if file exists
            if not os.path.exists(self.tokens_file):
                return None
                
            # Read tokens file
            with open(self.tokens_file, 'r') as f:
                tokens = json.load(f)
                
            # Return token info if exists
            return tokens.get(self.client_id)
                
        except Exception as e:
            print(f"Error getting token info: {str(e)}")
            return None
        
    async def get_access_token(self) -> Optional[str]:
        """Get access token through complete OAuth flow"""
        try:
            # Check existing token
            current_token = self.get_token_info()
            
            if current_token:
                # Use existing valid token
                if current_token['expires_at'] > time.time() + 300:
                    logger.debug(f"Using existing valid token for {self.client_id}")
                    return current_token['access_token']
                
                # Try refresh token first
                if 'refresh_token' in current_token:
                    logger.debug(f"Attempting token refresh for {self.client_id}")
                    refreshed = await self._refresh_token(current_token['refresh_token'])
                    if refreshed:
                        return refreshed['access_token']

            return None

        except Exception as e:
            logger.error(f"Error in get_access_token: {str(e)}")
            return None
    
    async def _refresh_token(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        """Refresh access token using GET method"""
        try:
            params = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.token_endpoint,
                    params=params,
                    headers=self.extra_headers,
                    ssl=False
                ) as resp:
                    
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Token refresh failed ({resp.status}): {error_text}")
                        return None

                    token_data = await resp.json()

                    if 'error' in token_data:
                        logger.error(f"OAuth error in refresh: {token_data} {params}")
                        return None

                    if 'access_token' not in token_data:
                        logger.error(f"Missing access_token in refresh: {token_data}")
                        return None

                    token_data['expires_at'] = time.time() + token_data.get('expires_in', 3600)
                    
                    await self.save_token(self.client_id, token_data)

                    return token_data

        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return None  
        
    def get_authorization_url(self) -> str:
        """Generate authorization URL"""
        state = base64.b64encode(self.client_id.encode()).decode()
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'response_type': 'code',
            'state': state
        }
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.auth_endpoint}?{query}"

    async def exchange_code_for_token(self, code: str) -> Optional[Dict]:
        """Exchange authorization code for token using GET method"""
        try:
            params = {
                'grant_type': 'authorization_code',
                'code': code,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'redirect_uri': self.redirect_uri
            }

            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            token_url = f"{self.token_endpoint}?{query_string}"

            headers = {'Accept': 'application/json'}

            async with aiohttp.ClientSession() as session:
                async with session.get(token_url, headers=headers, ssl=False) as resp:
                    return await self._process_response(resp)

        except Exception as e:
            logger.error(f"Token exchange error: {str(e)}")
            return None

    async def _process_response(self, resp) -> Optional[Dict]:
        """Process token exchange response"""
        response_text = await resp.text()

        if resp.status != 200:
            logger.error(f"Token exchange failed - Status: {resp.status}")
            return None

        try:
            data = json.loads(response_text)
        except json.JSONDecodeError:
            logger.error("Invalid JSON response")
            return None

        if 'access_token' not in data:
            logger.error("No access token in response")
            return None

        if 'expires_in' in data and 'expires_at' not in data:
            data['expires_at'] = time.time() + float(data['expires_in'])

        return data

    def _process_config(self, config: Dict, is_source: bool) -> Dict:
        """Process single auth config and return auth item"""
        client_id = config.get('client_id')
        if not client_id:
            return None
        
        domain = self._normalize_domain(config.get('domain', ''))

        auth_endpoint = config.get('auth_endpoint')
        if auth_endpoint is not None:
            auth_endpoint = auth_endpoint.replace('{domain}', domain)
            
        token_endpoint = config.get('token_endpoint')
        if token_endpoint is not None:
            token_endpoint = token_endpoint.replace('{domain}', domain)
            
        tokens = self.load_tokens()
        has_token = client_id in tokens
        token_info = tokens.get(client_id, {})
        
        expires_at = token_info.get('expires_at')
        remaining_time = None
        token_status = 'Pending'
        
        if has_token:
            if expires_at:
                current_time = int(time.time())
                remaining_seconds = expires_at - current_time
                
                if remaining_seconds > 0:
                    total_minutes = int(remaining_seconds // 60)
                    remaining_time = f"{total_minutes}m"
                    token_status = 'Authorized'
                else:
                    remaining_time = ''
                    token_status = 'Expired'
            else:
                token_status = 'Authorized'
                remaining_time = ''
        
        client = OAuth2Client({
            'client_id': client_id,
            'client_secret': config['client_secret'],
            'auth_endpoint': auth_endpoint or f"https://{domain}/oauth/authorize",
            'token_endpoint': token_endpoint or f"https://{domain}/oauth/token",
            'domain': domain
        })

        return {
            'type': 'Source' if is_source else 'Target',
            'client_id': client_id,
            'domain': domain,
            'auth_url': client.get_authorization_url(),
            'has_token': has_token,
            'status': token_status,
            'expires_in': remaining_time
        }

    def get_auth_items(self) -> List[Dict]:
        """Get all auth items for both source and target configs"""
        auth_items = []
        
        # Process source configs
        source_configs = self.load_auth_config(is_source=True)
        for config in source_configs:
            item = self._process_config(config, is_source=True)
            if item:
                auth_items.append(item)

        # Process target configs
        target_configs = self.load_auth_config(is_source=False)
        for config in target_configs:
            item = self._process_config(config, is_source=False)
            if item:
                auth_items.append(item)

        return auth_items

    def load_tokens(self) -> Dict:
        """Load tokens with empty file handling"""
        try:
            if not os.path.exists(self.tokens_file):
                return {}
                
            with open(self.tokens_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return json.loads(content) if content else {}
                
        except Exception as e:
            logger.error(f"Error reading tokens: {str(e)}")
            return {}

    def load_auth_config(self, is_source: bool = True) -> List[Dict]:
        """Load auth config with empty file handling"""
        file_path = self.source_auth_file if is_source else self.target_auth_file
        try:
            if not os.path.exists(file_path):
                return []
                
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return json.loads(content) if content else []
                
        except Exception as e:
            logger.error(f"Error reading config: {str(e)}")
            return []

    async def find_auth_config(self, client_id: str) -> Optional[Dict]:
        """Find auth config by client_id"""
        try:
            # Check source configs
            source_configs = self.load_auth_config(is_source=True)
            for config in source_configs:
                if config.get('client_id') == client_id:
                    return config

            # Check target configs
            target_configs = self.load_auth_config(is_source=False)
            for config in target_configs:
                if config.get('client_id') == client_id:
                    return config

            return None

        except Exception as e:
            logger.error(f"Error finding auth config: {str(e)}")
            return None

    async def save_token(self, client_id: str, token_data: Dict):
        """Save token data with backup"""
        try:
            data = self.load_tokens()

            if os.path.exists(self.tokens_file) and os.path.getsize(self.tokens_file) > 0:
                os.replace(self.tokens_file, f"{self.tokens_file}.bak")

            data[client_id] = token_data
            
            with open(self.tokens_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        except Exception as e:
            logger.error(f"Error saving token: {str(e)}")
            raise

    def remove_token(self) -> bool:
        """
        Remove token information from tokens.json by client_id
        Args:
            client_id: client id to remove
        Returns:
            bool: True if removed successfully, False if not found or error
        """
        try:
            # Check if file exists
            if not os.path.exists(self.tokens_file):
                return False
                
            # Read current tokens
            with open(self.tokens_file, 'r') as f:
                tokens = json.load(f)
                
            # Check if client_id exists
            if self.client_id not in tokens:
                return False
                
            # Remove token
            tokens.pop(self.client_id)
            
            # Write back to file
            with open(self.tokens_file, 'w') as f:
                json.dump(tokens, f, indent=2)
                
            return True
            
        except Exception as e:
            logger.error(f"Error removing token: {str(e)}")
            return False

    @staticmethod
    def get_auth_page_html(auth_items: list) -> str:
        """Generate HTML for auth page"""
        items_html = ""
        for item in auth_items:
            # Xác định màu status
            if item['status'] == 'Authorized':
                status_color = "#4CAF50"  # Xanh lá
            elif item['status'] == 'Expired':
                status_color = "#FF0000"  # Đỏ
            else:
                status_color = "#FFA500"  # Cam (Pending)
                
            # Hiển thị button authorize nếu không có token hoặc token đã hết hạn
            button_style = "display: none;" if item['status'] == 'Authorized' else ""
            
            # Chuẩn bị expires text
            expires_text = ""
            if item['has_token'] and item['expires_in']:
                expires_text = f"<p><strong>Expires in:</strong> {item['expires_in']}</p>"
                
            items_html += f"""
                <div class="auth-item">
                    <div class="auth-info">
                        <h3>{item['type']} Client</h3>
                        <p><strong>Domain:</strong> {item['domain']}</p>
                        <p><strong>Client ID:</strong> {item['client_id']}</p>
                        <p><strong>Status:</strong> <span style="color: {status_color}">{item['status']}</span></p>
                        {expires_text}
                    </div>
                    <div class="auth-action" style="{button_style}">
                        <a href="{item['auth_url']}" class="auth-button" target="_blank">
                            Authorize Now
                        </a>
                    </div>
                </div>
            """

        return f"""
            <!DOCTYPE html>
            <html>
                <head>
                    <title>Authorization Manager</title>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 0;
                            padding: 20px;
                            background-color: #f5f5f5;
                        }}
                        .container {{
                            max-width: 800px;
                            margin: 0 auto;
                        }}
                        .header {{
                            text-align: center;
                            margin-bottom: 30px;
                        }}
                        .auth-item {{
                            background: white;
                            border-radius: 8px;
                            padding: 20px;
                            margin-bottom: 20px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                            display: flex;
                            justify-content: space-between;
                            align-items: center;
                        }}
                        .auth-info {{
                            flex: 1;
                        }}
                        .auth-info h3 {{
                            margin: 0 0 15px 0;
                            color: #333;
                        }}
                        .auth-info p {{
                            margin: 5px 0;
                            color: #666;
                        }}
                        .auth-action {{
                            margin-left: 20px;
                        }}
                        .auth-button {{
                            display: inline-block;
                            background-color: #4CAF50;
                            color: white;
                            padding: 10px 20px;
                            text-decoration: none;
                            border-radius: 4px;
                            transition: background-color 0.3s;
                        }}
                        .auth-button:hover {{
                            background-color: #45a049;
                        }}
                        .note {{
                            text-align: center;
                            margin-top: 30px;
                            color: #666;
                        }}
                        .refresh-button {{
                            display: block;
                            margin: 20px auto;
                            padding: 10px 20px;
                            background-color: #2196F3;
                            color: white;
                            border: none;
                            border-radius: 4px;
                            cursor: pointer;
                            text-decoration: none;
                            text-align: center;
                            width: fit-content;
                        }}
                        .refresh-button:hover {{
                            background-color: #1976D2;
                        }}
                        .empty-state {{
                            text-align: center;
                            padding: 40px;
                            color: #666;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>Authorization Manager</h1>
                            <p>Authorize your applications below</p>
                        </div>
                        
                        {items_html if items_html else '<div class="empty-state"><p>All clients are authorized!</p></div>'}
                        
                        <a href="/oauth/authorize" class="refresh-button">
                            Refresh Status
                        </a>
                        
                        <div class="note">
                            <p>Note: After authorizing, close the popup window and click "Refresh Status"</p>
                        </div>
                    </div>
                </body>
            </html>
        """

    @staticmethod
    def get_success_html() -> str:
        """Return success page HTML"""
        return """
            <!DOCTYPE html>
            <html>
                <head>
                    <title>Authorization Successful</title>
                    <style>
                        body {
                            font-family: Arial, sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background-color: #f5f5f5;
                        }
                        .container {
                            text-align: center;
                            padding: 30px;
                            background: white;
                            border-radius: 8px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        }
                        h1 {
                            color: #4CAF50;
                            margin-bottom: 20px;
                        }
                        p {
                            color: #666;
                            line-height: 1.6;
                        }
                    </style>
                    <script>
                        window.onload = function() {
                            window.close();
                        };
                    </script>
                </head>
                <body>
                    <div class="container">
                        <h1>Authorization Successful!</h1>
                        <p>The authentication process has been completed.</p>
                        <p>You may close this window now.</p>
                    </div>
                </body>
            </html>
        """

    @staticmethod
    def get_error_html(error_message: str) -> str:
        """Generate error page HTML"""
        return f"""
            <!DOCTYPE html>
            <html>
                <head>
                    <title>Error</title>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background-color: #f5f5f5;
                        }}
                        .container {{
                            text-align: center;
                            padding: 30px;
                            background: white;
                            border-radius: 8px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        }}
                        h1 {{
                            color: #f44336;
                            margin-bottom: 20px;
                        }}
                        .error-message {{
                            color: #666;
                            line-height: 1.6;
                        }}
                        .back-button {{
                            display: inline-block;
                            margin-top: 20px;
                            padding: 10px 20px;
                            background-color: #2196F3;
                            color: white;
                            text-decoration: none;
                            border-radius: 4px;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>Error</h1>
                        <p class="error-message">{error_message}</p>
                        <a href="/oauth/authorize" class="back-button">Try Again</a>
                    </div>
                </body>
            </html>
        """