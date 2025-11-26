from typing import Dict, List, Optional, Tuple
import json
import os
from utils.oauth import OAuth2Client
from utils.logger import logger


class AuthManager:
    """
    Manages authentication configs and token rotation for source/target systems
    Integrates with OAuth2Client for token handling
    """

    def __init__(self, auth_dir: str = ".auths", is_source: bool = True):
        """
        Initialize auth manager
        Args:
            auth_dir: Folder containing auth configs
            is_source: Whether this is source system
        """
        self.auth_dir = auth_dir
        self.is_source = is_source
        
        # System identifier
        self.system = "source" if is_source else "target"
        
        # Initialize indexes
        self.webhook_index = -1
        self.auth_index = -1
        
        # Load configurations 
        self.webhooks = self._load_auth_file("webhooks")
        self.auth_list = self._load_auth_file("auth")
        
        # OAuth2 clients map
        self.oauth_clients: Dict[str, OAuth2Client] = {}

    @staticmethod
    def create_configs(auth_dir: str) -> None:
        """Create empty configuration files"""
        os.makedirs(auth_dir, exist_ok=True)
        
        files = [
            "source-webhooks.json",
            "source-auth.json", 
            "target-webhooks.json",
            "target-auth.json"
        ]
        
        for filename in files:
            file_path = os.path.join(auth_dir, filename)
            if not os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    json.dump([], f, indent=2)
                    
    def _load_auth_file(self, auth_type: str) -> List:
        """Load auth configuration file"""
        try:
            filename = f"{self.system}-{auth_type}.json"
            file_path = os.path.join(self.auth_dir, filename)
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {auth_type} config: {str(e)}")
        return []

    def _get_oauth_client(self, auth_config: Dict) -> OAuth2Client:
        """Get or create OAuth2Client for auth config"""
        client_id = auth_config['client_id']
        if client_id not in self.oauth_clients:
            self.oauth_clients[client_id] = OAuth2Client(auth_config)
        return self.oauth_clients[client_id]

    async def get_initial_auth(self) -> Tuple[Optional[str], Optional[Dict]]:
        """Get initial webhook and auth configuration"""
        webhook_url = self.webhooks[0] if self.webhooks else None
        auth_info = await self._get_auth_with_token(0) if self.auth_list else None
        self.webhook_index = 0 if webhook_url else -1
        self.auth_index = 0 if auth_info else -1
        return webhook_url, auth_info

    async def _get_auth_with_token(self, index: int) -> Optional[Dict]:
        """Get auth config with valid access token"""
        try:
            if 0 <= index < len(self.auth_list):
                auth_config = self.auth_list[index].copy()
                
                auth_config['auth_endpoint'] = self.auth_endpoint
                auth_config['token_endpoint'] = self.token_endpoint
                
                # Get token via OAuth2Client
                oauth_client = self._get_oauth_client(auth_config)
                token = await oauth_client.get_access_token()
                
                if token:
                    auth_config['token'] = token
                    return auth_config
                    
        except Exception as e:
            logger.error(f"Error getting auth with token: {str(e)}")
        return None

    def webhook_next(self) -> Optional[str]:
        """Get next webhook URL"""
        if not self.webhooks:
            return None
            
        self.webhook_index = (self.webhook_index + 1) % len(self.webhooks)
        return self.webhooks[self.webhook_index]

    async def auth_next(self) -> Optional[Dict]:
        """Get next auth configuration"""
        if not self.auth_list:
            return None
            
        self.auth_index = (self.auth_index + 1) % len(self.auth_list)
        return await self._get_auth_with_token(self.auth_index)