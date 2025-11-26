# src/processors/product_processor.py
from typing import Dict, Any, List, Optional
from processors.base_processor import BaseProcessor

class ProductProcessor(BaseProcessor):

    async def get_all(self, current_page: Optional[str] = None) -> Dict[str, Any]:
        return None
        
        # return await self.source_api.get_batch_list(
        #   self.entityTypeId,
        #   current_page,
        #   self.pages_per_batch,
        #   self.page_size,
        # )  

    async def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        
        field_mapping = self.mapping_fields['lead'] or {}
        
        transformed = self.get_mapping_data(data, field_mapping)

        return transformed

    async def _process_product_properties(self, product: Dict[str, Any], new_product_id: str):
        """Handle product properties migration"""
        try:
            if not product.get('PROPERTY_VALUES'):
                return

            for prop in product['PROPERTY_VALUES']:
                prop_data = {
                    'PRODUCT_ID': new_product_id,
                    'PROPERTY_ID': prop['PROPERTY_ID'],
                    'VALUE': prop['VALUE']
                }
                
                await self.target_api.call(
                    'crm.product.property.add',
                    {'fields': prop_data}
                )
                
        except Exception as e:
            self.logger.error(f"Error processing product properties: {str(e)}")

    async def process_items(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        return []