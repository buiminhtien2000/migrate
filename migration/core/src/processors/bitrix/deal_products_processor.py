from typing import Optional, List, Dict, Any
from processors.base_processor import BaseProcessor
from math import isnan

class DealProductsProcessor(BaseProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
     
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

    def _safe_convert_to_int(self, value):
        try:
            float_val = float(value)
            # Kiểm tra NaN
            if isinstance(float_val, float) and isnan(float_val):
                return 0
            return int(float_val)
        except (ValueError, TypeError):
            return 0
        
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
                WHERE processor_type = 'DealProcessor'
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
                key = f"{record['source_id']}_{record['target_id']}"
                params[key] = {
                      'endpoint': 'crm.deal.productrows.get',
                      'data': {
                          'id': record['source_id']
                      }
                  }
                    

            if not params:
                return None

            # Make API call and track progress
            result = await self.source_api.batch(params, fullData=True)

            if self.debug_mode:
                self.progress_tracker.save_to_file(result, f"{self.processor_type}-products-{current_page}")
                
            total_query = "SELECT COUNT(*) FROM mappings WHERE processor_type='DealProcessor'"
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
    
        products = self.mapping_fields['products'] or {}
        
        results = {}
        params = {}
        for key, items in records.items():
            
            source_id, target_id = key.split('_')

            results[target_id] = {
                'success': True,
                'source_id': source_id
            }

            if items is None or not items:
                continue
            
            rows = []
            for item in items:
                product_id = item.get('PRODUCT_ID')
                keys_to_remove = ['ID', 'OWNER_ID', 'OWNER_TYPE', 'TYPE', 'STORE_ID', 'RESERVE_ID', 'DATE_RESERVE_END']
                row = {k: v for k, v in item.items() if k not in keys_to_remove}
                row['PRODUCT_ID'] = self._safe_convert_to_int(products.get(str(product_id), '')) if product_id is not None else 0
                rows.append(row)

            params[target_id] = {
                'endpoint': 'crm.deal.productrows.set',
                'data': {
                    'id': target_id,
                    'rows': rows
                }
            }    
            
        if not params:
            return None
        
        if self.debug_mode:
            self.progress_tracker.save_to_file(params, f"{self.processor_type}-products-params")
            
        chunks = self._chunk_dict(params)
        try:
            for chunk in chunks:
                # Execute batch request
                batch_results = await self.target_api.batch(chunk)

                if self.debug_mode:
                    self.progress_tracker.save_to_file(batch_results, f"{self.processor_type}-products-add-batch")
                    
                # Process batch results
                if batch_results:
                    for idx, item in batch_results.items():
                        try:
                            if isinstance(item, dict):
                                results[idx]["error"] = str(item.get('error_description'))
                                results[idx]["success"] = False
                        except (KeyError, IndexError) as e:
                            results[idx]["error"] = f"Failed to get Bitrix ID: {str(e)}"
                            results[idx]["success"] = False
                            

                else:
                    # Handle invalid batch response
                    error_msg = "Invalid batch response structure"
                    self.logger.error(f"{error_msg}")

            if self.debug_mode:
                  self.progress_tracker.save_to_file(results, f"{self.processor_type}-products-add-results")
                  
            return list(results.values())      

        except Exception as e:
              error_msg = f"Batch request failed: {str(e)}"
              self.logger.error(error_msg)
        
        return None
