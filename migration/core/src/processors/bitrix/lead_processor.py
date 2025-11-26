# src/processors/lead_processor.py
from typing import Dict, Any, List, Optional
from processors.base_processor import BaseProcessor

class LeadProcessor(BaseProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.entityTypeId = 1
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

    async def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform data to include only title and email fields
        """

        field_mapping = self.mapping_fields['lead'] or {}
        status_mapping = self.mapping_fields['status'] or {}
        
        transformed = self.get_mapping_data(data, field_mapping)

        if data.get('CREATED_BY_ID'):
            transformed['CREATED_BY_ID'] =  self.get_mapping_users(data.get('CREATED_BY_ID'))

        if data.get('MODIFY_BY_ID'):
            transformed['MODIFY_BY_ID'] =  self.get_mapping_users(data.get('MODIFY_BY_ID'))

        if data.get('COMPANY_ID'):
            transformed['COMPANY_ID'] = await self.get_mapping_id('CompanyProcessor', data.get('COMPANY_ID'))
                
        if data.get('CONTACT_IDS'):
            transformed['CONTACT_IDS'] =  await self.get_mapping_id('ContactProcessor', data.get('CONTACT_IDS'))
        elif data.get('CONTACT_ID'):
            transformed['CONTACT_ID'] = await self.get_mapping_id('ContactProcessor', data.get('CONTACT_ID'))

        # if data.get('observers'):
            # transformed['observers'] =  self.get_mapping_users(data.get('observers'))
            
        transformed['STATUS_ID'] = status_mapping.get(data.get('STATUS_ID'), data.get('STATUS_ID'))

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
            
            return await self.target_api.final_process(
              self.entityTypeId,
              records,
              transformed_items,
              self._current_page
            )
        except Exception as e:
            error_msg = f"Transform data failed: {str(e)}"
            self.logger.error(error_msg)

        return None  