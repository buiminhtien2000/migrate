from typing import Any

class BaseJob:
    def __init__(self, db: 'Database', config: 'Config', logger: 'CustomLogger'): # type: ignore
        self.db = db
        self.config = config
        self.logger = logger
        
    async def setup(self):
        """Setup job if needed"""
        pass

    async def cleanup(self):
        """Cleanup resources when job stops"""
        pass  