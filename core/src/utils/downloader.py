import aiohttp # type: ignore
import asyncio
import base64
import re
from datetime import datetime
from urllib.parse import unquote
from typing import Dict, Optional, List, Union, Tuple

class Downloader:
    def __init__(self, config: 'Config', logger: 'CustomLogger'): # type: ignore
        """
        Initialize Downloader
        
        Args:
            max_concurrent (int): Maximum number of concurrent downloads
        """
        self.session: Optional[aiohttp.ClientSession] = None
        self.max_concurrent = 5
        self.logger = logger
        self.config = config
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def download_file(self, url: str) -> Optional[bytes]:
        """
        Download a single file
        
        Args:
            url (str): URL to download
            
        Returns:
            Optional[bytes]: File content or None if error
        """
        try:
            await self._ensure_session()
            async with self.session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    self.logger.error(f"HTTP error {response.status} for URL: {url}")
                    return None
                    
                content = await response.read()
                if not content:
                    self.logger.error(f"Empty content received: {url}")
                    return None
                    
                return content

        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return None

    def to_base64(self, content: bytes) -> str:
        """Convert bytes to base64 string"""
        return base64.b64encode(content).decode('utf-8')

    async def download_batch(self, files_map: Dict[str, List[Dict[str, str]]]) -> Dict[str, List[Optional[List[str]]]]:
        """
        Download multiple files concurrently
        
        Args:
            files_map: {
                "3322_1": [
                    {"name": "aaa.png", "url": "https://example.com/aa"},
                    {"name": "bbb.png", "url": "https://example.com/bb"}
                ]
            }
            
        Returns:
            {
                "3322_1": [
                    ["aaa.png", base64_content],
                    ["bbb.png", base64_content]
                ]
            }
        """
        self.logger.debug(f'Starting batch download')
        semaphore = asyncio.Semaphore(self.max_concurrent)
        results: Dict[str, List[Optional[List[str]]]] = {}
        
        async def download_single(url: str, name: str) -> Optional[List[str]]:
            async with semaphore:
                try:
                    content = await self.download_file(url)
                    if content:
                        base64_content = self.to_base64(content)
                        return [name, base64_content]
                    return None
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {str(e)}")
                    return None

        try:
            await self._ensure_session()
            
            for key, file_list in files_map.items():
                tasks = []
                for file_info in file_list:
                    url = file_info.get('url')
                    name = file_info.get('name')
                    if url and name:
                        task = asyncio.create_task(
                            download_single(url, name)
                        )
                        tasks.append(task)
                
                # Wait for all files in this key to complete
                key_results = await asyncio.gather(*tasks)
                results[key] = key_results

            # Log completion
            success_count = sum(1 for key_results in results.values() 
                              for result in key_results if result is not None)
            total_count = sum(len(file_list) for file_list in files_map.values())
            self.logger.debug(f'Batch download completed. Success: {success_count}/{total_count}')

            return results

        except Exception as e:
            self.logger.error(f"Batch download failed: {str(e)}")
            return {key: [None] * len(files) for key, files in files_map.items()}

    async def download_url_batch(self, urls_data: Dict[str, Dict[str, Union[str, List[str]]]]) -> Dict[str, Dict[str, Union[List[str], List[List[str]]]]]:
        """
        Download multiple files concurrently from urls
        
        Args:
            urls_data: {
                "0": {
                    "field2": "http://example2.com",
                    "field3": "http://example3.com",
                    "field4": ["http://example4.com", "http://example5.com"]
                }
            }
                
        Returns:
            {
                "0": {
                    "field2": ["file_1698225678.jpg", base64_content],
                    "field3": ["file_1698225679.jpg", base64_content],
                    "field4": [
                        ["file_1698225680.jpg", base64_content],
                        ["file_1698225681.jpg", base64_content]
                    ]
                }
            }
        """
        self.logger.debug(f"Starting batch download: {urls_data}")

        def flatten_urls(data: Dict[str, Dict]) -> Tuple[Dict[str, str], Dict[str, Tuple]]:
            """Flatten nested URL structure and create mapping"""
            flat_urls, mapping = {}, {}
            
            for record_idx, record in data.items():
                for field, urls in record.items():
                    if isinstance(urls, str):
                        key = f"{record_idx}_{field}"
                        flat_urls[key] = urls
                        mapping[key] = (record_idx, field, "single")
                    elif isinstance(urls, list):
                        for i, url in enumerate(urls):
                            key = f"{record_idx}_{field}_{i}"
                            flat_urls[key] = url
                            mapping[key] = (record_idx, field, "list")
            
            return flat_urls, mapping

        async def download_single(url: str, semaphore: asyncio.Semaphore) -> Optional[List[str]]:
            """Download single URL with semaphore control"""
            async with semaphore:
                try:
                    async with self.session.get(url) as response:
                        if response.status != 200:
                            return None

                        # Get extension from Content-Type
                        content_type = response.headers.get('Content-Type', '').lower()
                        ext = content_type.split('/')[-1] if content_type else 'bin'

                        # Get filename from Content-Disposition
                        content_disposition = response.headers.get('Content-Disposition', '')
                        filename = None

                        if content_disposition:
                            # Try UTF-8 encoded filename first
                            utf8_match = re.search(r"filename\*=utf-8''(.+)", content_disposition, re.IGNORECASE)
                            if utf8_match:
                                try:
                                    filename = unquote(utf8_match.group(1))
                                except Exception as e:
                                    self.logger.warning(f"Failed to decode UTF-8 filename: {e}")
                            
                            # If no UTF-8 filename, try regular filename
                            if not filename:
                                regular_match = re.search(r'filename="?([^"]+)"?', content_disposition)
                                if regular_match:
                                    filename = regular_match.group(1)

                        # If no filename found, generate one with timestamp
                        if not filename:
                            timestamp = int(datetime.now().timestamp())
                            filename = f"file_{timestamp}.{ext}"

                        # Clean filename
                        filename = re.sub(r'[^\w\-_\.]', '_', filename)

                        content = await response.read()
                        return [filename, self.to_base64(content)]

                except Exception as e:
                    self.logger.error(f"Error downloading {url}: {str(e)}")
                    return None

        # def restructure_results(temp_results: Dict[str, Optional[List[str]]], 
        #                       url_mapping: Dict[str, Tuple]) -> Dict[str, Dict]:
        #     """Restructure flat results back to nested format"""
        #     final_results = {}
            
        #     for key, result in temp_results.items():
        #         if not result:
        #             continue

        #         record_idx, field, type_ = url_mapping[key]
        #         final_results.setdefault(record_idx, {})

        #         if type_ == "single":
        #             final_results[record_idx][field] = result
        #         else:  # type_ == "list"
        #             final_results[record_idx].setdefault(field, []).append(result)

        #     return final_results
        
        def restructure_results(temp_results: Dict[str, Optional[List[str]]], 
                       url_mapping: Dict[str, Tuple]) -> Dict[str, Dict]:
            """Restructure flat results back to nested format with fileData wrapper"""
            final_results = {}
            
            for key, result in temp_results.items():
                if not result:
                    continue

                record_idx, field, type_ = url_mapping[key]
                final_results.setdefault(record_idx, {})

                if type_ == "single":
                    # Wrap single result in fileData
                    final_results[record_idx][field] = {"fileData": result}
                else:  # type_ == "list"
                    # Initialize list in fileData if not exists
                    if field not in final_results[record_idx]:
                        final_results[record_idx][field] = {"fileData": []}
                    # Append to existing list in fileData
                    final_results[record_idx][field]["fileData"].append(result)

            return final_results

        try:
            await self._ensure_session()
            
            # Flatten URLs and create mapping
            flattened_urls, url_mapping = flatten_urls(urls_data)
            
            # Create and execute download tasks
            semaphore = asyncio.Semaphore(self.max_concurrent)
            tasks = {
                key: asyncio.create_task(download_single(url, semaphore))
                for key, url in flattened_urls.items() if url
            }
            
            # Gather results
            temp_results = {
                key: await task
                for key, task in tasks.items()
            }
            
            # Restructure results
            final_results = restructure_results(temp_results, url_mapping)
            
            # Log statistics
            success_count = sum(1 for result in temp_results.values() if result is not None)
            self.logger.debug(f'Batch download completed. Success: {success_count}/{len(flattened_urls)}')
            
            return final_results

        except Exception as e:
            self.logger.error(f"Batch download failed: {str(e)}")
            return {idx: None for idx in urls_data}