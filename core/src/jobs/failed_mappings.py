import os
from datetime import datetime
import aiofiles # type: ignore
from jobs.base import BaseJob

class MappingRetryJob(BaseJob):
    def __init__(self, db: 'Database', config: 'Config', logger: 'CustomLogger') -> None: # type: ignore
        super().__init__(db, config, logger)
        self.failed_mappings_dir = self.config.PROGRESS_PATH
        
    async def setup(self):
        if not os.path.exists(self.failed_mappings_dir):
            os.makedirs(self.failed_mappings_dir)

    def get_failed_mapping_filename(self):
        return os.path.join(
            self.failed_mappings_dir,
            f"failed_mappings_{datetime.now().strftime('%Y%m%d')}.csv"
        )

    async def save_failed_mapping(self, processor_type: str, source_id: str, target_id: str):
        filename = self.get_failed_mapping_filename()
        async with aiofiles.open(filename, mode='a') as f:
            await f.write(f"{processor_type},{source_id},{target_id}\n")
        self.logger.info(f"Saved failed mapping to retry file: {filename}")

    async def retry_failed_mappings(self):
        """Retry importing failed mappings from file"""
        self.logger.info(f"start")
        # ... code xử lý retry ...