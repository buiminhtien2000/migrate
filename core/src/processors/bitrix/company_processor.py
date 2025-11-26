# src/processors/company_processor.py
import os
import json
from typing import Dict, Any, List, Optional
from processors.base_processor import BaseProcessor

class CompanyProcessor(BaseProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.entityTypeId = 4
        self._current_page = 1

    async def get_all(self, current_page: Optional[str] = None) -> Dict[str, Any]:
        self._current_page = current_page
        return await self.source_api.get_batch_list(
          self.entityTypeId,
          current_page,
          self.pages_per_batch,
          self.page_size,
          True
        )
    
    async def update_company_logo(self) -> None:
        """
        Process all unprocessed logo files, call batch API and rename processed files
        """
        try:
            # Tìm tất cả file chưa processed
            logo_files = []
            for file in os.listdir(self.config.PROGRESS_PATH):
                if (file.startswith(f"{self.processor_type}-logo") and file.endswith('.txt')):
                    logo_files.append(os.path.join(self.config.PROGRESS_PATH, file))

            if not logo_files:
                self.logger.info(f"No unprocessed logo files found for processor type: {self.processor_type}")
                return

            self.logger.info(f"Found {len(logo_files)} files to process")

            # Xử lý từng file
            for logo_file in logo_files:
                try:
                    self.logger.info(f"\nProcessing file: {logo_file}")
                    
                    # Đọc và xử lý file
                    with open(logo_file, 'r', encoding='utf-8') as f:
                        logo_data = json.load(f)

                    if not isinstance(logo_data, list):
                        self.logger.info(f"Skip file {logo_file}: not a valid list")
                        continue

                    # Process in batches
                    batch = []
                    total_processed = 0

                    for item in logo_data:
                        if not item.get('target_id') or not item.get('data'):
                            continue

                        batch.append({
                            'endpoint': 'crm.company.update',
                            'data': {
                                'id': item['target_id'],
                                'fields': {'LOGO': item['data']}
                            }
                        })

                        if len(batch) >= 50:
                            await self.target_api.batch(batch)
                            total_processed += len(batch)
                            self.logger.info(f"Processed batch: {total_processed} items")
                            batch = []

                    # Process remaining items
                    if batch:
                        await self.target_api.batch(batch)
                        total_processed += len(batch)
                        self.logger.info(f"Processed final batch. Total: {total_processed} items")

                    # Rename processed file
                    processed_file = logo_file.replace("-logo", "-processed-logo")
                    os.rename(logo_file, processed_file)
                    self.logger.info(f"File renamed to: {processed_file}")

                except json.JSONDecodeError as e:
                    self.logger.error(f"Error reading file {logo_file}: {str(e)}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing file {logo_file}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Error in update_company_logo: {str(e)}")

    async def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform data to include only title and email fields
        """
        field_mapping = self.mapping_fields['company'] or {}
        
        transformed = self.get_mapping_data(data, field_mapping)

        if data.get('CREATED_BY_ID'):
            transformed['CREATED_BY_ID'] =  self.get_mapping_users(data.get('CREATED_BY_ID'))

        if data.get('MODIFY_BY_ID'):
            transformed['MODIFY_BY_ID'] =  self.get_mapping_users(data.get('MODIFY_BY_ID'))

        # Handle EMAIL
        email_data = data.get('EMAIL', [])
        if email_data and isinstance(email_data, list):
            transformed['EMAIL'] = [
                {
                    'VALUE_TYPE': item.get('VALUE_TYPE', ''),
                    'VALUE': item.get('VALUE', '').strip()
                }
                for item in email_data
                if item and isinstance(item, dict) 
                and item.get('VALUE') not in ("0", "") 
                and item.get('VALUE', '').strip()
            ]

        # Handle PHONE
        phone_data = data.get('PHONE', [])
        if phone_data and isinstance(phone_data, list):
            transformed['PHONE'] = [
                {
                    'VALUE_TYPE': item.get('VALUE_TYPE', ''),
                    'VALUE': item.get('VALUE', '').strip()
                }
                for item in phone_data
                if item and isinstance(item, dict)
                and item.get('VALUE') not in ("0", "")
                and item.get('VALUE', '').strip()
            ]

        transformed.pop('ID', None)
        transformed.pop('CURRENCY_ID', None)

        return transformed

    async def process_items(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process records using Bitrix batch API with batch import
        
        Args:
            records (List[Dict[str, Any]]): List of records to process
            
        Returns:
            Dict[str, Any]: Results of processing with success/failure status
        """
        self.logger.debug('Process records using Bitrix batch API with batch import')

        if not records:
          return None

        try:
            
            transformed_items = [await self._transform_data(record) for record in records]

            if self.debug_mode:
                self.logger.debug(f"record[0]: {records[0] if records else None}")
                self.progress_tracker.save_to_file(transformed_items, f"{self.processor_type}-transformed_items")
                
            transformed_items = await self.handle_file_fields(transformed_items)

            logo_data = []
            for index, transformed in enumerate(transformed_items):
                if transformed.get('LOGO') and len(transformed['LOGO']) > 1:
                    logo_data.append({
                        'source_id': records[index].get('ID') or records[index].get('id', ''),
                        'target_id': '',
                        'data': {'fileData': transformed['LOGO']}
                    })
            
            results = await self.target_api.final_process(
              self.entityTypeId,
              records,
              transformed_items,
              self._current_page
            )

            logo_data = self.target_api._fast_update_target_ids(logo_data, results)
            self.progress_tracker.save_to_file(logo_data, f"{self.processor_type}-logo-{self._current_page}")

            return results
        
        except Exception as e:
            error_msg = f"Transform data failed: {str(e)}"
            self.logger.error(error_msg)

        return None
    
