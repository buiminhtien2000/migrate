import json
import os
from datetime import datetime
from typing import Dict, Any

class BackupManager:
    def __init__(self, config: 'Config', logger: 'CustomLogger'): # type: ignore
        self.backup_path = config.BACKUP_DIR
        self.logger = logger
        os.makedirs(self.backup_path, exist_ok=True)

    def create_backup(self, processor_name: str, data: Dict[str, Any]):
        """Create a backup for entity data"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(
            self.backup_path, 
            f"{processor_name}_backup_{timestamp}.json"
        )
        
        with open(filename, 'w') as f:
            json.dump(data, f)

    def restore_backup(self, backup_file: str) -> Dict[str, Any]:
        """Restore data from a backup file"""
        filepath = os.path.join(self.backup_path, backup_file)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Backup file not found: {backup_file}")
            
        with open(filepath, 'r') as f:
            return json.load(f)

    def list_backups(self, processor_name: str = None):
        """List all available backups"""
        files = os.listdir(self.backup_path)
        if processor_name:
            return [f for f in files if f.startswith(f"{processor_name}_backup_")]
        return files

    async def clear_all_backups(self) -> None:
        """Clear all backup files"""
        try:
            if os.path.exists(self.backup_path):
                import shutil
                shutil.rmtree(self.backup_path)
                os.makedirs(self.backup_path, exist_ok=True)
            self.logger.info("All backups cleared")
        except Exception as e:
            self.logger.error(f"Error clearing backups: {str(e)}")
            raise  