from typing import Optional, List, Dict, Any
from processors.base_processor import BaseProcessor
from utils.constants import ProcessorType
import random

class CommentProcessor(BaseProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def _get_entity_type(self, processor_type: str) -> Optional[str]:
        entity_mapping = {
            ProcessorType.COMPANY.value: 'company',
            ProcessorType.CONTACT.value: 'contact',
            ProcessorType.DEAL.value: 'deal',
            ProcessorType.LEAD.value: 'lead'
        }
        return entity_mapping.get(processor_type)

    def _filter_items_after_id(self, items: List[Dict], last_processed_id: str) -> List[Dict]:
        """
        Filter items after the last processed ID based on their index in the list.
        Args:
            items: List of items to filter
            last_processed_id: Last processed index as string
        Returns:
            Filtered list of items after the last processed index
        """
        if not last_processed_id:
            return items
            
        try:
            index = int(last_processed_id)
            if 0 <= index < len(items):
                return items[index + 1:]
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid last_processed_id: {last_processed_id}")
            
        return items

    async def get_all(self, current_page) -> Optional[List[Dict[str, Any]]]:
        try:
            # Handle pagination
            page = int(current_page or 1) - 1
            offset = page * self.batch_size

            self.logger.error(f"Fetch {self.batch_size} mapping items from page: {current_page} - offset: {offset}")

            # Get records from DB
            query = """
                SELECT *
                FROM mappings
                LIMIT $1 OFFSET $2;
            """
            
            records = await self.db.fetch(
                query,
                self.batch_size,
                offset
            )

            if not records:
                return None

            # Build params for API call
            params = {}
            for record in records:
                entity_type = self._get_entity_type(record['processor_type'])
                if not entity_type:
                    continue
                
                params[record['target_id']] = {
                      'endpoint': 'crm.timeline.comment.list',
                      'data': {
                          'filter': {
                              'ENTITY_TYPE': entity_type,
                              'ENTITY_ID': record['source_id'],
                          }
                      }
                  }
                    

            if not params:
                return None

            # Make API call and track progress
            result = await self.source_api.batch(params, fullData=True)

            if self.debug_mode:
                self.progress_tracker.save_to_file(result, f"{self.processor_type}-comments-{current_page}")
                
            total_query = "SELECT COUNT(*) FROM mappings"
            total = await self.db.fetchval(total_query)
        
            return {
                'result': result or [],
                'total': total,
                'next': current_page * self.batch_size,
            }

        except Exception as e:
            self.logger.error(f"Error getting comments: {str(e)}")
          
        return None  
   
    async def process_items(self, records: List[Dict[str, Any]]) -> list[dict]: 

        self.logger.debug('Process records using Bitrix batch API with batch import')

        if not records:
            return None

        results = {}
        params = {}
        counter = 1
        file_items = {}
        for key, items in records.items():

            results[key] = {
                'success': True,
                'source_id': key
            }

            if items is None or not items:
                continue

            for item in items:

                if 'FILES' in item:
                    file_items[f"{key}_{counter}"] = item['FILES']

                params[f"{key}_{counter}"] = {
                    'endpoint': 'crm.timeline.comment.add',
                    'data': {
                        'fields': {
                            'ENTITY_ID': key,
                            'ENTITY_TYPE': item['ENTITY_TYPE'],
                            'COMMENT': f"{item['COMMENT']} \n {item['CREATED']}" ,
                            'AUTHOR_ID': self.get_mapping_users(item['AUTHOR_ID']),
                        }
                    }
                }
                counter += 1
            
        if not params:
            return list(results.values())

        filesData = {}
        if file_items:
            file_urls = await self.source_api.get_files_data(file_items)
            filesData = await self.downloader.download_batch(file_urls) 
      
        if filesData:
            for key, item in filesData.items():
                params[key]['data']['fields']['FILES'] = item

            if self.debug_mode:
                self.progress_tracker.save_to_file(params, f"{self.processor_type}-comment-params")

        chunks = self._chunk_dict(params)
        try:
            for chunk in chunks:
                # Execute batch request
                batch_results = await self.target_api.batch(chunk)

                if self.debug_mode:
                    self.progress_tracker.save_to_file(batch_results, f"{self.processor_type}-comment-add-batch")
                    
                # Process batch results
                if batch_results:
                    processed_ids = set()
                    for idx, item in batch_results.items():
                        source_id = idx.split('_')[0]
                        if source_id not in processed_ids:
                            processed_ids.add(source_id)
                            try:
                                if isinstance(item, dict):
                                    results[source_id]["error"] = str(item.get('error_description'))
                                    results[source_id]["success"] = False
                            except (KeyError, IndexError) as e:
                                results[source_id]["error"] = f"Failed to get Bitrix ID: {str(e)}"
                                results[source_id]["success"] = False

                else:
                    # Handle invalid batch response
                    error_msg = "Invalid batch response structure"
                    self.logger.error(f"{error_msg}")

            if self.debug_mode:
                  self.progress_tracker.save_to_file(results, f"{self.processor_type}-comment-add-results")
                  
            return list(results.values())      

        except Exception as e:
              error_msg = f"Batch request failed: {str(e)}"
              self.logger.error(error_msg)
        
        return None
