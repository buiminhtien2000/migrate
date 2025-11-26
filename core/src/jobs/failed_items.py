from jobs.base import BaseJob

class FixedItemsJob(BaseJob):
    
    async def hourly_process(self):
        """Job xử lý fixed items hàng giờ"""
        try:
            self.logger.info("Starting hourly fixed items processing...")
            await self.process_fixed_items()
            self.logger.info("Completed hourly fixed items processing")
        except Exception as e:
            self.logger.error(f"Error in hourly processing: {str(e)}")

    async def end_of_day_process(self):
        """Job xử lý cuối ngày"""
        try:
            self.logger.info("Starting end of day processing...")
            await self.process_fixed_items(end_of_day=True)
            self.logger.info("Completed end of day processing")
        except Exception as e:
            self.logger.error(f"Error in end of day processing: {str(e)}")

    async def save_batch(self, items):
        pass   
         
    async def process_fixed_items(self):
        pass      