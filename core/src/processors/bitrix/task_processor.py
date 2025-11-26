from typing import Optional, List, Dict, Any
from processors.base_processor import BaseProcessor
from utils.constants import ProcessorType
import asyncio

class TaskProcessor(BaseProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
            
    def _get_entity_type(self, processor_type: str) -> Optional[str]:
        entity_mapping = {
            ProcessorType.COMPANY.value: '4',
            ProcessorType.CONTACT.value: '3',
            ProcessorType.DEAL.value: '2',
            ProcessorType.LEAD.value: '1'
        }
        return entity_mapping.get(processor_type)
      
    def _get_crm_entity_type(self, processor_type: str) -> Optional[str]:
        entity_mapping = {
            '4': 'CO_',
            '3': 'C_',
            '2': 'D_',
            '1': 'L_'
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

    async def _get_storage_id(self) -> Optional[str]:

        if not self.config.TARGET_STORAGE_ID:

            self.logger.debug(f"Call api")
            
            parts = self.target_api.webhook_url.split('/')
            user_id = parts[4]

            result = await self.target_api.call(
                'disk.storage.getlist',
                {
                    'filter': {
                        'ENTITY_TYPE': 'user',
                        'ENTITY_ID': user_id,
                    }
                }
            )
            
            self.config.TARGET_STORAGE_ID = result[0].get('ID') if result else None
            
        return self.config.TARGET_STORAGE_ID    

    async def _get_task_comments(self, records: List[Dict]) -> List[Dict]:

        self.logger.debug('Fetch all comments in task')

        if not records:
            return None

        results = {}
        params = {}
        counter = 1
        for key, items in records.items():

            if items is None or not items:
                continue

            for item in items:
                params[f"{key}_{counter}"] = {
                    'endpoint': 'task.commentitem.getlist',
                    'data': {
                        'id': item['ASSOCIATED_ENTITY_ID']
                    }
                }
                counter += 1

        if not params:
            return None
        
        chunks = self._chunk_dict(params)
        try:
            for chunk in chunks:
                # Execute batch request
                batch_results = await self.source_api.batch(chunk, fullData=True)

                if self.debug_mode:
                    self.progress_tracker.save_to_file(batch_results, f"{self.processor_type}-task-commentitem-getlist")
                    
                # Process batch results
                if batch_results:
                    results.update(batch_results)
                else:
                    # Handle invalid batch response
                    error_msg = "Invalid batch response structure"
                    self.logger.error(f"{error_msg}")

            if self.debug_mode:
                  self.progress_tracker.save_to_file(results, f"{self.processor_type}-task-commentitem-getlist-results")
                  
            return results

        except Exception as e:
              error_msg = f"Batch request failed: {str(e)}"
              self.logger.error(error_msg)
        
        return None
      
    async def _add_tasks(self, records: List[Dict]) -> List[Dict]:

        self.logger.debug('Add tasks')

        if not records:
            return None
        
        results = {}
        params = {}
        counter = 1
        for key, items in records.items():

            if items is None or not items:
                continue

            for item in items:
                entity_type = self._get_crm_entity_type(item['OWNER_TYPE_ID'])
                
                params[f"{key}_{counter}"] = {
                    'endpoint': 'tasks.task.add',
                    'data': {
                      'fields': {
                            'UF_CRM_TASK': [f"{entity_type}{key}"],
                            'TITLE': item['SUBJECT'],
                            'DESCRIPTION': item['DESCRIPTION'],
                            'CREATED_DATE': item['CREATED'],
                            'CHANGED_DATE': item['LAST_UPDATED'],
                            'DATE_START': item['START_TIME'],
                            'CLOSED_DATE': item['END_TIME'],
                            'DEADLINE': item['DEADLINE'],
                            'COMPLETED': item['COMPLETED'],
                            'STATUS': '5' if item['COMPLETED'] == 'Y' else item['STATUS'],
                            'RESPONSIBLE_ID': self.get_mapping_users(item['RESPONSIBLE_ID']),
                            'PRIORITY': item['PRIORITY'],
                            'CREATED_BY': self.get_mapping_users(item['AUTHOR_ID']),
                            'CHANGED_BY': self.get_mapping_users(item['EDITOR_ID']),
                            # 'AUDITORS': self.get_mapping_users(item['AUDITORS']),
                            # 'ACCOMPLICES': self.get_mapping_users(item['ACCOMPLICES']),
                      }
                    }
                }
                counter += 1

        if not params:
            return None

        if self.debug_mode:
            self.progress_tracker.save_to_file(params, f"{self.processor_type}-tasks-params")
                    
        chunks = self._chunk_dict(params)
        try:
            for chunk in chunks:
                
                # Execute batch request
                batch_results = await self.target_api.batch(chunk)
                
                if self.debug_mode:
                    self.progress_tracker.save_to_file(batch_results, f"{self.processor_type}-tasks-task-add")
                    
                # Process batch results
                if batch_results:
                    results.update(batch_results)
                else:
                    # Handle invalid batch response
                    error_msg = "Invalid batch response structure"
                    self.logger.error(f"{error_msg}")

            if self.debug_mode:
                  self.progress_tracker.save_to_file(results, f"{self.processor_type}-tasks-task-add-results")
                  
            return results

        except Exception as e:
              error_msg = f"Batch request failed: {str(e)}"
              self.logger.error(error_msg)
        
        return None
      
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
                      'endpoint': 'crm.activity.list',
                      'data': {
                          'filter': {
                              'OWNER_TYPE_ID': entity_type,
                              'OWNER_ID': record['source_id'],
                              'PROVIDER_TYPE_ID': ['TASK', 'TASKS_TASK']
                          }
                      }
                  }
             
            if not params:
                return None

            self.progress_tracker.save_to_file(params, f"{self.processor_type}-crm-activity-list-{current_page}-params")
            
            # Make API call and track progress
            result = await self.source_api.batch(params, fullData=True)

            if self.debug_mode:
                self.progress_tracker.save_to_file(result, f"{self.processor_type}-crm-activity-list-{current_page}-results")

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

        tasks, comments = await asyncio.gather(
            self._add_tasks(records),
            self._get_task_comments(records)
        )

        if self.debug_mode:
            self.progress_tracker.save_to_file(tasks, f"{self.processor_type}-task-commentitem-tasks")
            self.progress_tracker.save_to_file(comments, f"{self.processor_type}-task-commentitem-comments")
            self.progress_tracker.save_to_file(records, f"{self.processor_type}-task-commentitem-records")
        
        results = {}
        params = {}
        file_items = {}
        for key, items in records.items():

            results[key] = {
                'success': True,
                'source_id': key
            }

            if items is None or not items:
                continue

            for i, item in enumerate(items):
                index = f"{key}_{i + 1}"
                
                if tasks.get(index) is None or comments.get(index) is None:
                    continue

                for x, comment in enumerate(comments.get(index)):
   
                    if 'ATTACHED_OBJECTS' in comment:
                        file_items[f"{index}_{x}"] = comment.get('ATTACHED_OBJECTS')
                        
                    params[f"{index}_{x}"] = {
                        'endpoint': 'task.commentitem.add',
                        'data': {
                            'TASKID': tasks.get(index, {}).get('task', {}).get('id'),
                            'FIELDS': {
                                'AUTHOR_ID': self.get_mapping_users(comment.get('AUTHOR_ID')),
                                'POST_MESSAGE': f"{comment.get('POST_MESSAGE')} \n {comment.get('POST_DATE')}",
                                'UF_FORUM_MESSAGE_DOC': [],
                            }
                        }
                    }

        if self.debug_mode:
            self.progress_tracker.save_to_file(params, f"{self.processor_type}-task.commentitem.add")
            
        if not params:
            return list(results.values())

        filesData = {}
        if file_items:
            file_urls = await self.source_api.get_files_data(file_items)
            filesData = await self.downloader.download_batch(file_urls) 

        if self.debug_mode:
            self.progress_tracker.save_to_file(filesData, f"{self.processor_type}-task-commentitem-filesData")

        if filesData:
            storage_id = await self._get_storage_id()
            if storage_id:
                file_results = await self.target_api.upload_files(storage_id, filesData)
                if file_results:
                    for key, items in file_results.items():
                        files = [f'n{num}' for num in items]
                        params[key]['data']['FIELDS']['UF_FORUM_MESSAGE_DOC'] = files

        if self.debug_mode:
            self.progress_tracker.save_to_file(params, f"{self.processor_type}-task-commentitem-add-params")
        
        chunks = self._chunk_dict(params)
        try:
            for ind, chunk in enumerate(chunks):

                # Execute batch request
                batch_results = await self.target_api.batch(chunk)

                if self.debug_mode:
                    self.progress_tracker.save_to_file(batch_results, f"{self.processor_type}-task-commentitem-add-batch-results-{ind}")
        
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
                  self.progress_tracker.save_to_file(results, f"{self.processor_type}-task-commentitem-add-results-{ind}")
                  
            return list(results.values())      

        except Exception as e:
              error_msg = f"Batch request failed: {str(e)}"
              self.logger.error(error_msg)
        
        return None
