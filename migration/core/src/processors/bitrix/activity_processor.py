# src/processors/activity_processor.py
from typing import Dict, Any, List
from processors.base_processor import BaseProcessor

class ActivityProcessor(BaseProcessor):
    async def get_all(self) -> List[Dict[str, Any]]:
        self.logger.debug("Fetching activities")
        try:
            activities = await self.source_api.call(
                'crm.activity.list',
                {
                    'select': ['*'],
                },
            )
            
            return activities

        except Exception as e:
            self.logger.error(f"{str(e)}")
            raise

    async def process_item(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        self.logger.debug(f"Processing activity {activity.get('ID')}")
        
        try:
            transformed = await self._transform_data(activity)
            
            # Map related entities
            for field, processor_name in [
                ('OWNER_ID', 'user'),
                ('ASSOCIATED_ENTITY_ID', activity.get('OWNER_TYPE_ID', '').lower())
            ]:
                if activity.get(field):
                    mapped_id = 1
                    if mapped_id:
                        transformed[field] = mapped_id

            result = await self.target_api.call(
                'crm.activity.add',
                {'fields': transformed}
            )

            # Process activity files if any
            if activity.get('FILES'):
                await self._process_activity_files(activity['ID'], result['result'])

            return result

        except Exception as e:
            self.logger.error(str(e))

        return False 

    async def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        self.logger.debug(f"Transforming activity {data.get('ID')}")
        
        skip_fields = ['ID', 'DATE_CREATE', 'DATE_MODIFY', 'CREATED_BY_ID']
        transformed = {
            k: v for k, v in data.items() 
            if k not in skip_fields and not k.startswith('~')
        }

        field_mapping = self.config.ACTIVITY_FIELD_MAPPING or {}
        for old_field, new_field in field_mapping.items():
            if old_field in transformed:
                transformed[new_field] = transformed.pop(old_field)

        return transformed

    async def _process_activity_files(self, old_activity_id: str, new_activity_id: str):
        """Handle activity attachments migration"""
        try:
            files = await self.source_api.call(
                'crm.activity.file.list',
                {'activityId': old_activity_id}
            )

            if files is None:
              return
            
            for file in files:
                file_content = await self.source_api.download_file(file['fileId'])
                await self.target_api.upload_file(
                    new_activity_id,
                    file['fileName'],
                    file_content
                )

        except Exception as e:
            self.logger.error(f"Error processing activity files: {str(e)}")
            await self.backup_manager.create_backup(
                'activity_file_error',
                {
                    'activity_id': old_activity_id,
                    'error': str(e)
                }
            )