# src/api/bitrix_api.py
import os
import json
from config import config
from typing import Dict, Any, Optional, List
from api.bitrix_base import BitrixBase

class BitrixAPI(BitrixBase):
    def __init__(self, logger: 'CustomLogger', auth_dir: str, is_source: bool, rate_limit: float = 0.5): # type: ignore
        super().__init__(logger, auth_dir, is_source, rate_limit)

    def _chunk_dict(self, data: dict, chunk_size: int = 50) -> list[dict]:
        """
        Split a large dictionary into smaller ones, each containing chunk_size properties
        
        Example:
        data = {
            'key1': value1,
            'key2': value2,
            ...  # 100 keys
        }
        Returns: [
            {key1->key50}, # dict with first 50 keys
            {key51->key100} # dict with next 50 keys
        ]
        """
        items = list(data.items())  # convert dict to list of (key, value) pairs
        return [
            dict(items[i:i + chunk_size]) 
            for i in range(0, len(items), chunk_size)
        ]

    def _check_next_page(result_total, result_next):
        """
        Check if there is a next page based on result_total and result_next
        """
        if isinstance(result_total, list) and isinstance(result_next, list):
            return len(result_next) == len(result_total) and len(result_next) > 0
        elif isinstance(result_total, dict) and isinstance(result_next, dict):
            return len(result_next) == len(result_total) and len(result_next) > 0
        elif isinstance(result_total, list) and isinstance(result_next, dict):
            # Handle mixed case: list and dict
            return bool(result_next)  # True if dict not empty
        else:
            return False
    
    def _to_camel_case(self, s: str) -> str:
        words = s.lower().split('_')
        result = words[0]
        for word in words[1:]:
            if word.isdigit():
                result += word
            else:
                result += word.capitalize()
                
        return result

    def _fast_update_target_ids(self, data, results):
        # Create lookup map once
        success_map = {
            r['source_id']: r['target_id'] 
            for r in results 
            if r['success']
        }
        
        # Single pass update
        for logo in data:
            sid = logo['source_id']
            if sid in success_map:
                logo['target_id'] = success_map[sid]
        
        return data
    
    async def update_field_files(self, entityTypeId: str) -> None:
        """
        Process all unprocessed files, call batch API and rename processed files
        """
        try:
            self.logger.info(f"Start process update field files")

            if not entityTypeId:
                return None
            
            endpoints = {
                "1": 'crm.lead.update',
                "2": 'crm.deal.update',
                "3": 'crm.contact.update', 
                "4": 'crm.company.update',
                "7": 'crm.quote.update',
                "8": 'crm.address.update',
            }

            params = {}
            key = str(entityTypeId)
            if key not in endpoints:
                endpoint = 'crm.item.update'
                params['entityTypeId'] = entityTypeId
            else:
                endpoint = endpoints[key]
            
            
            data_files = []
            for file in os.listdir(config.PROGRESS_PATH):
                if (file.startswith(f"{entityTypeId}-fieldFiles") and file.endswith('.txt')):
                    data_files.append(os.path.join(config.PROGRESS_PATH, file))

            if not data_files:
                self.logger.info(f"No unprocessed files found for processor: {entityTypeId}")
                return

            self.logger.info(f"Found {len(data_files)} files to process")

            for data_file in data_files:
                try:
                    self.logger.info(f"\nProcessing file: {data_file}")

                    with open(data_file, 'r', encoding='utf-8') as f:
                        field_datas = json.load(f)

                    if not isinstance(field_datas, list):
                        self.logger.info(f"Skip file {data_file}: not a valid list")
                        continue

                    batch = []
                    total_processed = 0

                    for item in field_datas:
                        if not item.get('target_id') or not item.get('data'):
                            continue
                        
                        params['id'] = item['target_id']
                        
                        if 'entityTypeId' in params:
                            params['fields'] = item['data']
                        else:
                            params['fields'] = {
                                k: [{'fileData': x} for x in v] if v and isinstance(v[0], list) else {'fileData': v}
                                for k, v in item['data'].items()
                            }

                        batch.append({
                            'endpoint': endpoint,
                            'data': params
                        })

                        if len(batch) >= 50:
                            await self.batch(batch)
                            total_processed += len(batch)
                            self.logger.info(f"Processed batch: {total_processed} items")
                            batch = []

                    # Process remaining items
                    if batch:
                        await self.batch(batch)
                        total_processed += len(batch)
                        self.logger.info(f"Processed final batch. Total: {total_processed} items")

                    if config.DEBUG_MODE:
                        processed_file = data_file.replace("-fieldFiles", "-processed-fieldFiles")
                        os.rename(data_file, processed_file)
                    else:
                        os.remove(data_file)

                except json.JSONDecodeError as e:
                    self.logger.error(f"Error reading file {data_file}: {str(e)}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing file {data_file}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Error: {str(e)}")
        
    async def get_files_data(self, urls: Dict) -> list[dict]:

        results = {}
        listUrls = {}
        fileNames = {}
        for key, items in urls.items():
            count = 1
            for item in items.values():
                _key = f"{key}_{count}"
                listUrls[_key] = {
                    'endpoint': f"disk.file.get?id={item.get('id') or item.get('FILE_ID')}",
                    'data': {}
                }
                fileNames[_key] = item.get('name') or item.get('NAME')
                count +=1

        if not listUrls:
            return None

        chunks = self._chunk_dict(listUrls)
        try:
            for chunk in chunks:
                # Execute batch request
                batch_results = await self.batch(chunk)
                
                # Process batch results
                if batch_results:

                    _results = {}

                    for index, item in batch_results.items():
                        _key = index.rsplit('_', 1)[0] if '_' in index else index
                        if _key not in _results:
                            _results[_key] = []
                        _results[_key].append({
                          'name': fileNames[index],
                          'url': item['DOWNLOAD_URL']
                        })
                        
                    results.update(_results)

                else:
                    # Handle invalid batch response
                    error_msg = "Invalid batch response structure"
                    self.logger.error(f"{error_msg}")

        except Exception as e:
              error_msg = f"Batch request failed: {str(e)}"
              self.logger.error(error_msg)
        
        return results

    async def upload_files(self, storageId: str, data: dict) -> dict:
        cmdUploads = {}
        output = {}
        for key, items in data.items():
            for index, file in enumerate(items):
                cmdUploads[f"{key}_{index}"] = {
                    'endpoint': 'disk.storage.uploadfile',
                    'data': {
                        'id': storageId,
                        'data': {'NAME': file[0]},
                        'fileContent': file[1],
                        'generateUniqueName': 1
                    }
                }

        if cmdUploads:
            results = await self.batch(cmdUploads)

        if not results:
            return None
        
        for key, item in results.items():
            parts = key.rsplit('_', 1)
            _key = parts[0]
            if _key not in output:
                output[_key] = []
            output[_key].append(item['ID'])

        return output    
      
    async def get_batch_list(
        self, 
        entityTypeId: str, 
        current_page: int, 
        pages_per_batch: int, 
        page_size: Optional[int] = None,
        standard: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get batch list of items
        
        Args:
            entityTypeId: Entity type ID
            current_page: Current page number
            pages_per_batch: Number of pages per batch
            page_size: Size of each page
            
        Returns:
            Dict containing results, total and next page info
        """
        self.logger.debug(f"Fetching records with current page {current_page}")

        data = {}
        
        if standard:
            endpoints = {
              "1": 'crm.lead.list',
              "2": 'crm.deal.list',
              "3": 'crm.contact.list',
              "4": 'crm.company.list',
              "7": 'crm.quote.list',
              "8": 'crm.address.list',
            }

            endpoint = endpoints.get(str(entityTypeId))
            data['select'] = ['*', 'UF_*', 'PHONE', 'EMAIL']
            
            if endpoint is None:
                return None
        else:
          endpoint = 'crm.item.list'
          data['entityTypeId'] = entityTypeId
          data['select'] = ['*', 'uf*', 'phone', 'email']
        
        try:
            # Build commands for batch request
            commands = {}
            
            offset = (current_page - 1) * page_size * pages_per_batch

            ignore = {}
            for i in range(pages_per_batch):

                current_data = data.copy()
                current_data['start'] = offset + (page_size * i)

                index = i
                if current_page == 1:
                   index += 1

                ignore[index] = current_data['start']   
                commands[index] = {
                    'endpoint': endpoint,
                    'data': current_data
                }

            self.logger.debug(
                f"current_page: {current_page} - offset: {offset} "
                f"| pages: {pages_per_batch} size: {page_size}"
            )

            # Make batch request
            response = await self.batch(commands, fullResponse=True)
            if not response:
                return None
            
            # Get and validate result
            result = self.get_batch_result(response)
            if not result:
                return None

            total = 0
            next_page = False

            # Collect all items
            try:
                if isinstance(result, dict):
                    result_total = response.get('result', {}).get('result_total', {})
                    total = next(iter(result_total.values()), None)
                else:
                    result_total = response.get('result', {}).get('result_total', [])
                    total = result_total[0] if isinstance(result_total, list) and result_total else None

                remaining_records = total - offset
                real_pages = min(
                    pages_per_batch,
                    (remaining_records + page_size - 1) // page_size
                )
                
                offset_target = total // page_size * page_size
                ignore_index = next((i for i, x in enumerate(ignore.values()) if x == offset_target), None)

                is_last_batch = (offset + (real_pages * page_size) >= total)
                
                if isinstance(result, dict):
                    items_source = list(result.values())[:real_pages]
                else:
                    items_source = result[:real_pages]
                    
                all_items = [
                    item
                    for idx, result_item in enumerate(items_source)
                    if ignore_index is None or idx <= ignore_index
                    for item in (result_item.get('items', []) if isinstance(result_item, dict) else result_item)
                    if item
                ]

                next_page = not is_last_batch
                
                self.logger.debug(f"Find out: {len(all_items)} - next: {next_page} - offset_target: {offset_target}")
                
            except (KeyError, AttributeError):
                self.logger.error("Failed to extract items from result")
                return None
            
            return {
                'result': all_items,
                'total': total,
                'next': next_page,
            }

        except Exception as e:
            self.logger.error(f"Error in get_batch_list: {str(e)}")
            return None
    
    async def import_batch(
        self,
        entityTypeId: str, 
        records: List[Dict[str, Any]],
        transformed_items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:

        if not transformed_items:
            self.logger.debug(f"transformed_items is None")
            return None

        results = []
        try:
            commands = {}
            record_index_map = {}
            original_records_map = {}

            for i in range(0, len(transformed_items), 20):
                end_idx = min(i + 20, len(transformed_items))
                batch_items = transformed_items[i:end_idx]
                original_records = records[i:end_idx]

                commands[i] = {
                    'endpoint': 'crm.item.batchImport',
                    'data': {
                        'entityTypeId': entityTypeId,
                        'data': batch_items
                    }
                }
                
                record_index_map[str(i)] = {
                    str(idx): str(record.get('ID') or record.get('id'))
                    for idx, record in enumerate(original_records)
                }

                original_records_map[str(i)] = {
                    str(idx): record
                    for idx, record in enumerate(original_records)
                }

            try:
                batch_results = await self.batch(commands)
                    
                if batch_results:
                    for record_idx, record_result in batch_results.items():
                        items_list = record_result.get('items', [])
                        
                        for idx, item in enumerate(items_list):
                            source_id = record_index_map[str(record_idx)][str(idx)]
                            original_record = original_records_map[str(record_idx)][str(idx)]
                            
                            result = {
                                "success": False,
                                "source_id": source_id
                            }
                            
                            try:
                                if bitrix_id := item.get('item', {}).get('id'):
                                    result.update({
                                        "success": True,
                                        "target_id": str(bitrix_id)
                                    })
                                elif error := item.get('error_description'):
                                    result["error"] = str(error)
                                    # result["data"] = original_record
                                else:
                                    result["error"] = "No Bitrix ID or error description found"
                                    # result["data"] = original_record
                                    
                            except (KeyError, IndexError) as e:
                                result["error"] = f"Failed to get Bitrix ID: {str(e)}"
                                # result["data"] = original_record
                                
                            results.append(result)
                    
                    return results
                  
                else:
                    error_msg = "Invalid batch response structure"
                    self.logger.error(f"{error_msg}")

            except Exception as e:
                error_msg = f"Batch request failed: {str(e)}"
                self.logger.error(error_msg)
                
        except Exception as e:
            error_msg = f"Fatal error in process_items: {str(e)}"
            self.logger.error(error_msg)
            
        return None

    async def final_process(
        self,
        entityTypeId: str,
        records: List[Dict[str, Any]],
        transformed_items: List[Dict[str, Any]],
        current_page: int
    ) -> Dict[str, Any]:
        
        file_data = []

        for index, transformed in enumerate(transformed_items):
            source_id = records[index].get('ID') or records[index].get('id', '')
            target_id = transformed.get('target_id', '')
            
            current_data = {}
            for field_name, field_value in transformed.items():
                if isinstance(field_value, dict) and 'fileData' in field_value:
                    current_data[field_name] = field_value['fileData']
            
            if current_data:
                file_data.append({
                    'source_id': source_id,
                    'target_id': target_id,
                    'data': current_data
                })

        results = await self.import_batch(
          entityTypeId,
          records,
          transformed_items
        )
        
        if file_data:
            file_data = self._fast_update_target_ids(file_data, results)
            filename = f"{config.PROGRESS_PATH}/{entityTypeId}-fieldFiles-{current_page}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
              json.dump(file_data, f, indent=2, ensure_ascii=False)

        return results
