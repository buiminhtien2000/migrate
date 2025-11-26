# src/api/bitrix_base.py
import os
import aiohttp # type: ignore
import asyncio
import base64
import json
from urllib.parse import urlencode
from typing import Dict, Any, Optional, List, Union
from utils.auth_manager import AuthManager
from urllib.parse import urlparse
import mimetypes
import time

class BitrixBase:
    def __init__(self, logger: 'CustomLogger', auth_dir: str, is_source: bool = True, rate_limit: float = 0.5): # type: ignore
        """
        Initialize BitrixAPI client
        
        Args:
            is_source: Bitrix24 source
            rate_limit: Rate limit in seconds between requests
        """
        
        self.rate_limit = rate_limit
        self.session = None
        self.last_request_time = 0
        self.logger = logger
        self.max_file_size_mb = None

        self.auth_manager = AuthManager(auth_dir=auth_dir, is_source=is_source)
        self.auth_manager.auth_endpoint = 'https://{domain}/oauth/authorize'
        self.auth_manager.token_endpoint = 'https://oauth.bitrix.info/oauth/token'

        self.webhook_url = None
        self.auth_info = None

    async def url_to_base64(self, url: str, file_name: Optional[str] = None) -> Optional[List[str]]:
        """
        Async function to download file from URL and convert to base64
        """
        self.logger.debug('Starting download file...')

        timeout = aiohttp.ClientTimeout(total=300)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            await self._ensure_session()

            now = asyncio.get_event_loop().time()
            if now - self.last_request_time < self.rate_limit:
                await asyncio.sleep(self.rate_limit - (now - self.last_request_time))

            async with self.session.get(url, timeout=timeout, headers=headers) as response:
                self.last_request_time = asyncio.get_event_loop().time()

                if response.status != 200:
                    self.logger.error(f"HTTP error {response.status} for URL: {url}")
                    return None

                if self.max_file_size_mb:
                    content_length = response.headers.get('content-length')
                    if content_length:
                        size_mb = int(content_length) / (1024 * 1024)
                        if size_mb > self.max_file_size_mb:
                            self.logger.error(f"File too large ({size_mb:.2f}MB) for URL: {url}")
                            return None

                if not file_name:
                    file_name = ""
                    content_disposition = response.headers.get('content-disposition')
                    if content_disposition and 'filename=' in content_disposition:
                        file_name = content_disposition.split('filename=')[-1].strip('"\'')
                    
                    if not file_name:
                        file_name = os.path.basename(urlparse(url).path)
                    
                    if not file_name:
                        content_type = response.headers.get('content-type', '')
                        ext = mimetypes.guess_extension(content_type) or ''
                        file_name = f"file_{int(time.time())}{ext}"

                content = await response.read()
                
                if not content:
                    self.logger.error(f"Empty content received for URL: {url}")
                    return None

                try:
                    base64_content = base64.b64encode(content).decode('utf-8')
                    return [file_name, base64_content]
                except Exception as e:
                    self.logger.error(f"Base64 encoding failed for {url}: {str(e)}")
                    return None

        except Exception as e:
            self.logger.error(f"Failed to download {url}: {str(e)}")
            return None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def _fetch_all(self, method: str, params: Dict[str, Any], initial_data: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch all pages of data"""
        data = initial_data.copy()
        _next = initial_data.get('next')
        
        while _next:
            try:
                next_url = f"{method}{'&' if '?' in method else '?'}start={_next}"
                async with self.session.post(f"{self.webhook_url}{next_url}", json=params or {}) as response:
                    self.last_request_time = asyncio.get_event_loop().time()
                    next_data = await response.json()
                    
                    if not next_data or 'result' not in next_data:
                        break
                        
                    data['result'].extend(next_data['result'])
                    _next = next_data.get('next')
                    
                    # Rate limiting
                    now = asyncio.get_event_loop().time()
                    if now - self.last_request_time < self.rate_limit:
                        await asyncio.sleep(self.rate_limit - (now - self.last_request_time))
                        
            except Exception as e:
                self.logger.error(f"Error fetching next page: {str(e)}")
                break
                
        return data

    async def handle_auth_retry(self, method: str = None, params: Dict = None, is_batch: bool = False, fullResponse: bool = False, fullData: bool = False):
        """Handle auth retry logic"""
        auth_info = await self.auth_manager.auth_next()
        webhook_url = self.auth_manager.webhook_next()

        self.logger.info(f"handle_auth_retry(): webhook_url: {webhook_url} auth_info: {auth_info}")

        if not auth_info and not webhook_url:
            self.logger.error("No more auth configurations or webhooks available")
            return None
        
        if auth_info:
            self.auth_info = auth_info
        if webhook_url:    
            self.webhook_url = webhook_url
        
        if is_batch:
            return await self.batch(params, fullResponse, fullData)
        else:
            return await self.call(method, params, fullResponse, fullData)

    async def call(self, method: str, params: Dict[str, Any] = None, fullResponse: bool = False, fullData: bool = False) -> Optional[Dict[str, Any]]:
        """Make API call with retry logic"""
        max_retries = max(
            len(self.auth_manager.webhooks) if self.auth_manager.webhooks else 0,
            len(self.auth_manager.auth_list) if self.auth_manager.auth_list else 0
        )
        retry_count = 0

        if not self.webhook_url or not self.auth_info:
            self.logger.debug("Missing auth credentials - attempting to get new auth")
            self.webhook_url, self.auth_info = await self.auth_manager.get_initial_auth()

            if not self.webhook_url or not self.auth_info:
                self.logger.error("Failed to obtain required auth credentials")
                return None
                
            self.logger.info("Successfully obtained new auth credentials")

        while retry_count < max_retries:
            try:
                
                await self._ensure_session()

                now = asyncio.get_event_loop().time()
                if now - self.last_request_time < self.rate_limit:
                    await asyncio.sleep(self.rate_limit - (now - self.last_request_time))
                
                url = f"{self.webhook_url}{method}"
                
                async with self.session.post(url, json=params or {}) as response:
                    self.last_request_time = asyncio.get_event_loop().time()
                    
                    try:
                        data = await response.json()
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON response: {str(e)}")
                        retry_count += 1
                        if retry_count >= max_retries:
                            return str(e)
                        continue
                    
                    # Handle API errors
                    if 'error' in data:
                        error_info = data
                        if isinstance(error_info, dict):
                            error_msg = error_info.get('message') or error_info.get('error_description')
                            error_code = error_info.get('code') or error_info.get('error')
                            
                            if error_code in ['INVALID_TOKEN', 'expired_token', 'OPERATION_TIME_LIMIT']:
                                retry_count += 1
                                remaining = max_retries - retry_count
                                self.logger.warning(f"Retry {retry_count}/{max_retries} - Auth/Limit error: {error_code} ({remaining} retries remaining)")
                                
                                result = await self.handle_auth_retry(method, params, False, fullResponse, fullData)
                                if result:
                                    return result
                                if retry_count >= max_retries:
                                    return error_msg
                                continue
                                
                            self.logger.error(f"Bitrix API Error {error_code}: {error_msg}")
                            return error_msg

                    # Handle successful response
                    if 'result' not in data:
                        self.logger.error("Missing 'result' in Bitrix response")
                        retry_count += 1
                        if retry_count >= max_retries:
                            return "Missing 'result' in Bitrix response"
                        continue

                    if fullData:
                        data = await self._fetch_all(method, params, data)

                    if not fullResponse:
                        data = data['result']
                              
                    return data
                    
            except aiohttp.ClientError as e:
                self.logger.error(f"Bitrix connection failed: {str(e)}")
                retry_count += 1
                if retry_count >= max_retries:
                    return None

                self.logger.warning(f"Bitrix connection failed: handle_auth_retry()")
                result = await self.handle_auth_retry(method, params, False, fullResponse, fullData)
                if result:
                    return result
                continue
                
            except asyncio.TimeoutError:
                self.logger.error("Bitrix request timeout")
                retry_count += 1
                if retry_count >= max_retries:
                    return None

                self.logger.warning(f"Bitrix request timeout: handle_auth_retry()")
                result = await self.handle_auth_retry(method, params, False, fullResponse, fullData)
                if result:
                    return result
                continue
                
            except Exception as e:
                self.logger.error(f"Unexpected Bitrix API error: {str(e)}")
                return None

        self.logger.error("All retries exhausted")
        return None
      
    def get_batch_result(self, response: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Extract batch result from response"""
        if not isinstance(response, dict):
            return None
        
        result = response.get('result', {}).get('result')
        if not result:
            return None
            
        return result

    def encode(self, endpoint: str, data: Dict[str, Any]) -> str:
        """
        Encode data into URL query parameters
        """
        if not data:
            return endpoint

        def encode_value(parent_key: str, value: Any) -> Dict[str, Any]:
            if not isinstance(value, (dict, list)):
                return {parent_key: str(value)}
            
            result = {}
            
            if isinstance(value, dict):
                for k, v in value.items():
                    new_key = f"{parent_key}[{k}]" if parent_key else k
                    result.update(encode_value(new_key, v))
                        
            elif isinstance(value, list):
                if not value:
                    array_key = f"{parent_key}[]"
                    result[array_key] = []
                elif isinstance(value[0], (list, dict)):
                    for i, item in enumerate(value):
                        new_key = f"{parent_key}[{i}]"
                        result.update(encode_value(new_key, item))
                else:
                    array_key = f"{parent_key}[]"
                    result[array_key] = [str(v) for v in value]
                        
            return result

        encoded = {}
        for key, value in data.items():
            encoded.update(encode_value(key, value))

        params = []
        for key, value in encoded.items():
            if isinstance(value, list):
                if not value:
                    params.append((key, ''))
                else:
                    for v in value:
                        params.append((key, str(v)))
            else:
                params.append((key, str(value)))

        query = urlencode(params)
        return f'{endpoint}?{query}' if query else endpoint

    async def batch(
        self, 
        data: Union[Dict[str, str], List[Dict[str, str]]], 
        fullResponse: bool = False, 
        fullData: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Execute batch request with retry logic"""

        if isinstance(data, list):
            data = {str(i): item for i, item in enumerate(data)}

        max_retries = max(
            len(self.auth_manager.webhooks) if self.auth_manager.webhooks else 0,
            len(self.auth_manager.auth_list) if self.auth_manager.auth_list else 0
        )
        retry_count = 0
        
        if not self.webhook_url or not self.auth_info:
            self.logger.error("Missing auth credentials - attempting to get new auth")
            self.webhook_url, self.auth_info = await self.auth_manager.get_initial_auth()

            if not self.webhook_url or not self.auth_info:
                self.logger.error("Failed to obtain required auth credentials")
                return None
                
            self.logger.info("Successfully obtained new auth credentials")

        while retry_count < max_retries:
            try:
                
                await self._ensure_session()

                now = asyncio.get_event_loop().time()
                if now - self.last_request_time < self.rate_limit:
                    await asyncio.sleep(self.rate_limit - (now - self.last_request_time))

                batch_commands = {}
                for key, item in data.items():
                    batch_commands[f"{key}"] = self.encode(item['endpoint'], item['data'])

                if hasattr(self, 'logBatch'):
                    self.logger.debug(f"batch_commands: {batch_commands}")

                params = {"cmd": batch_commands}
                url = f"{self.webhook_url}batch"
                
                async with self.session.post(url, json=params) as response:
                    self.last_request_time = asyncio.get_event_loop().time()
                    
                    try:
                        response_data = await response.json()
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON response in batch: {str(e)}")
                        retry_count += 1
                        if retry_count >= max_retries:
                            return None
                        continue

                    if 'result' not in response_data:
                        self.logger.error(f"Missing 'result' in Bitrix batch response: {response_data}")
                        retry_count += 1
                        if retry_count >= max_retries:
                            return None
                        continue
                      
                    # Handle API errors
                    result_error = response_data.get('result', {}).get('result_error', {})
                    if result_error:
                        first_error = next(iter(result_error.values()), None)
                        if first_error:
                            error_code = first_error.get('error')
                            if error_code in ['INVALID_TOKEN', 'expired_token', 'OPERATION_TIME_LIMIT']:
                                retry_count += 1
                                remaining = max_retries - retry_count
                                self.logger.warning(f"Retry {retry_count}/{max_retries} - Auth/Limit error: {error_code} ({remaining} retries remaining)")
                                
                                result = await self.handle_auth_retry(None, data, True, fullResponse, fullData)
                                if result:
                                    return result
                                if retry_count >= max_retries:
                                    return None
                                continue

                    if fullData:
                        result_next = response_data.get('result', {}).get('result_next', {})
                        if result_next:
                            for key in result_next:
                                _run = True
                                start = result_next[key]
                                while(_run):
                                    _result = await self.call(
                                        f"{data[key]['endpoint']}{'&' if '?' in data[key]['endpoint'] else '?'}start={start}", 
                                        data[key]['data']
                                    )
                                    
                                    if _result and isinstance(_result, dict):
                                        response_data['result']['result'][key].extend(_result.get('result', []))
                                    else:
                                        break    

                                    if _result.get('next'):
                                        start = _result.get('next')
                                    else:
                                        _run = False

                    if not fullResponse:
                        response_data = self.get_batch_result(response_data)
                                     
                    return response_data
                    
            except aiohttp.ClientError as e:
                self.logger.error(f"Bitrix batch connection failed: {str(e)}")
                retry_count += 1
                if retry_count >= max_retries:
                    return None

                self.logger.warning(f"Bitrix batch connection failed: handle_auth_retry()")    
                result = await self.handle_auth_retry(None, data, True, fullResponse, fullData)
                if result:
                    return result
                continue
                
            except asyncio.TimeoutError:
                self.logger.error("Bitrix batch request timeout")
                retry_count += 1
                if retry_count >= max_retries:
                    return None

                self.logger.warning(f"Bitrix batch request timeout: handle_auth_retry()")    
                result = await self.handle_auth_retry(None, data, True, fullResponse, fullData)
                if result:
                    return result
                continue
                
            except Exception as e:
                self.logger.error(f"Unexpected Bitrix batch API error: {str(e)}")
                return None

        self.logger.error("All batch retries exhausted")
        return None

    async def check_connection(self) -> bool:
        """
        Check connection to Bitrix24 API
        Returns: True if connection is successful, False otherwise
        """
        try:
            if not self.webhook_url: 
                return False
            
            url = f"{self.webhook_url}/user.current"

            async with self.session.get(url) as response:
                if response.status != 200:
                    self.logger.warning(f"Bitrix API connection failed with status {response.status}")
                    return False
                    
                data = await response.json()
                if not data.get('result'):
                    self.logger.warning("Invalid response from Bitrix API")
                    return False

                return True

        except Exception as e:
            self.logger.error(f"Failed to check Bitrix connection: {str(e)}")
            return False
        
    async def close(self):
        """Close aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()      