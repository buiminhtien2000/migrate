import os
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from utils.excel import Excel
from utils.constants import ProcessingStatus, ItemStatus

class ProgressTracker:
    def __init__(self, db: 'Database', config: 'Config', logger: 'CustomLogger'): # type: ignore
        """
        Initialize ProgressTracker
        
        Args:
            config (Dict[str, Any]): Configuration containing:
                - progress_dir: Directory to store progress file
        """
        self.db = db
        self.config = config
        self.logger = logger
        self.processor_type = None
        
    def _validate_processor_type(self):
        if not self.processor_type:
            raise ValueError("processor_type not set")
            
    async def _read_progress(self) -> Dict:
        """Read current progress from database"""
        try:
            query = """
                SELECT 
                    p.processor_type,
                    p.total_items,
                    p.batch_size,
                    p.current_page,
                    p.status,
                    p.start_time,
                    p.last_updated,
                    p.metadata,
                    (
                        SELECT ARRAY_AGG(item_id) 
                        FROM processed_items pi 
                        WHERE pi.processor_type = p.processor_type 
                        AND pi.status = 'processed'
                    ) as processed_items,
                    (
                        SELECT json_agg(
                            json_build_object(
                                'id', item_id,
                                'error', error,
                                'time', processed_at
                            )
                        )
                        FROM processed_items pi
                        WHERE pi.processor_type = p.processor_type 
                        AND pi.status = 'failed'
                    ) as failed_items
                FROM progress_trackers p
            """
            rows = await self.db.fetch(query)

            progress = {}
            for row in rows:
                progress[row['processor_type']] = {
                    'total_items': row['total_items'],
                    'batch_size': row['batch_size'],
                    'current_page': row['current_page'],
                    'processed_items': row['processed_items'] or [],
                    'failed_items': row['failed_items'] or [],
                    'status': row['status'],
                    'start_time': row['start_time'].isoformat(),
                    'last_updated': row['last_updated'].isoformat(),
                    'metadata': row['metadata']
                }
            return progress

        except Exception as e:
            self.logger.error(f"Error reading progress from database: {str(e)}")
            return {}

    async def initialize_processing(
        self,
        total_items: int,
        current_page: int,
        batch_size: int,
        metadata: Optional[Dict] = None
    ) -> None:
        """Initialize or resume processing"""
        self._validate_processor_type()
        
        query = """
            INSERT INTO progress_trackers (
                processor_type,
                total_items,
                current_page,
                batch_size,
                status,
                start_time,
                last_updated,
                metadata
            ) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $6)
            ON CONFLICT (processor_type) DO NOTHING
        """
        await self.db.execute(
            query,
            self.processor_type,
            total_items,
            current_page,
            batch_size, 
            ProcessingStatus.IN_PROGRESS.value,
            metadata or {}
        )

    async def mark_item_processed(self, item_id: str) -> None:
        """Mark an item as processed"""
        self._validate_processor_type()
        
        query = """
            INSERT INTO processed_items (
                processor_type,
                item_id,
                status,
                processed_at
            ) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            ON CONFLICT (processor_type, item_id) 
            DO UPDATE SET 
                status = $3,
                processed_at = CURRENT_TIMESTAMP
        """
        await self.db.execute(
            query,
            self.processor_type,
            item_id,
            ProcessingStatus.COMPLETED.value
        )

        update_query = """
            UPDATE progress_trackers 
            SET last_updated = CURRENT_TIMESTAMP
            WHERE processor_type = $1
        """
        await self.db.execute(update_query, self.processor_type)
        
    async def mark_item_failed(
        self,
        item_id: str,
        error: str,
        data: Optional[Dict] = None
    ) -> None:
        """Mark an item as failed and save info"""
        self._validate_processor_type()
        
        try:
            query = """
                INSERT INTO processed_items (
                    processor_type,
                    item_id,
                    status,
                    error,
                    metadata,
                    processed_at
                ) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (processor_type, item_id) 
                DO UPDATE SET 
                    status = $3,
                    error = $4,
                    metadata = $5,
                    processed_at = CURRENT_TIMESTAMP
            """
            await self.db.execute(
                query,
                self.processor_type,
                item_id,
                ProcessingStatus.FAILED.value,
                error,
                json.dumps(data or {})
            )

            update_query = """
                UPDATE progress_trackers 
                SET last_updated = CURRENT_TIMESTAMP
                WHERE processor_type = $1
            """
            await self.db.execute(update_query, self.processor_type)

            failed_item_data = {
                'timestamp': datetime.now().isoformat(),
                'processor': self.processor_type,
                'error': str(error) if error else '',
                'status': 'pending',
                'item': json.dumps(data) if data else ''
            }
            
            excel_handler = Excel(self.logger, self.config.DOWNLOAD_DIR)
            excel_file = os.path.join(self.config.DOWNLOAD_DIR, "failed-items.xlsx")
            await excel_handler.save_failed_items(failed_item_data, excel_file)

        except Exception as e:
            self.logger.error(f"Error in mark_item_failed: {str(e)}", exc_info=True)
            raise

    async def complete_process(self) -> None:
        """Mark a process as completed"""
        self._validate_processor_type()
        
        progress = await self._read_progress()
        
        if self.processor_type in progress:
            progress[self.processor_type]['status'] = ProcessingStatus.COMPLETED.value
            progress[self.processor_type]['last_updated'] = datetime.now().isoformat()

            query = """
                UPDATE progress_trackers 
                SET status = $1
                WHERE processor_type = $2
            """
            await self.db.execute(query, ProcessingStatus.COMPLETED.value, self.processor_type)

    async def get_process_status(self) -> Optional[Dict]:
        """Get current status of the process"""
        self._validate_processor_type()
        
        query = """
            SELECT 
                processor_type,
                total_items,
                batch_size,
                status,
                start_time,
                last_updated,
                metadata
            FROM progress_trackers
            WHERE processor_type = $1
        """
        result = await self.db.fetchrow(query, self.processor_type)
        
        if result:
            return dict(result)
        return None

    async def _get_processed_count(self) -> int:
        """Get count of successfully processed items"""
        self._validate_processor_type()
        
        query = """
            SELECT COUNT(*) 
            FROM processed_items
            WHERE processor_type = $1 AND status = $2
        """
        result = await self.db.fetchval(query, self.processor_type, ItemStatus.PROCESSED.value)
        return result or 0

    async def _get_failed_count(self) -> int:
        """Get count of failed items"""
        self._validate_processor_type()
        
        query = """
            SELECT COUNT(*) 
            FROM processed_items
            WHERE processor_type = $1 AND status = $2
        """
        result = await self.db.fetchval(query, self.processor_type, ItemStatus.FAILED.value)
        return result or 0
      
    async def save_state(self, state: Dict[str, Any]) -> None:
        """Save processing state"""
        self._validate_processor_type()
        
        try:
            query = """
                UPDATE progress_trackers
                SET 
                    metadata = metadata || $2::jsonb,
                    last_updated = CURRENT_TIMESTAMP
                WHERE processor_type = $1
            """
            await self.db.execute(
                query,
                self.processor_type,
                json.dumps(state)
            )
        except Exception as e:
            self.logger.error(f"Error saving state: {str(e)}")
            raise

    async def get_state(self) -> Optional[Dict]:
        """Get saved state"""
        self._validate_processor_type()
        
        try:
            query = """
                SELECT 
                    total_items,
                    batch_size,
                    current_page,
                    status,
                    start_time,
                    last_updated,
                    metadata
                FROM progress_trackers
                WHERE processor_type = $1
            """
            result = await self.db.fetchrow(query, self.processor_type)
            
            if result:
                return {
                    'total_items': result['total_items'],
                    'batch_size': result['batch_size'],
                    'current_page': result['current_page'],
                    'status': result['status'],
                    'start_time': result['start_time'].isoformat(),
                    'last_updated': result['last_updated'].isoformat(),
                    'metadata': result['metadata']
                }
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting state: {str(e)}")
            return None

    async def cleanup_old_data(self, cutoff_date: datetime) -> None:
        """Clean up old progress data"""
        try:
            delete_items_query = """
                DELETE FROM processed_items
                WHERE processor_type IN (
                    SELECT processor_type 
                    FROM progress_trackers 
                    WHERE last_updated < $1
                )
            """
            await self.db.execute(delete_items_query, cutoff_date)

            delete_progress_query = """
                DELETE FROM progress_trackers
                WHERE last_updated < $1
            """
            await self.db.execute(delete_progress_query, cutoff_date)

        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {str(e)}")
            raise

    async def is_item_processed(self, item_id: str) -> bool:
        """Check if an item has been processed or failed"""
        self._validate_processor_type()
        
        try:
            query = """
                SELECT status 
                FROM processed_items
                WHERE processor_type = $1 
                AND item_id = $2
                AND status IN ($3, $4)
            """
            result = await self.db.fetchrow(
                query, 
                self.processor_type, 
                item_id,
                ProcessingStatus.COMPLETED.value,
                ProcessingStatus.FAILED.value
            )
            
            return result is not None

        except Exception as e:
            self.logger.error(f"Error checking item status: {str(e)}")
            return False

    async def should_process_next(self) -> Dict[str, Any]:
        """Check if we should proceed with this processor"""
        self._validate_processor_type()
        
        try:
            result = await self.db.fetchrow("""
                SELECT *
                FROM progress_trackers
                WHERE processor_type = $1
            """, self.processor_type)

            if not result:
                return {'run': True, 'data': {}}

            record_dict = dict(result)
            return {
                'run': record_dict.get('status') != ProcessingStatus.COMPLETED.value,
                'data': record_dict
            }

        except Exception as e:
            self.logger.error(f"Failed to check process status: {str(e)}")
            return {'run': False, 'data': {}}
          
    async def get_statistics(self) -> Dict[str, Any]:
        """Get detailed statistics for the process"""
        self._validate_processor_type()
        
        try:
            process_query = """
                SELECT total_items, status, start_time, last_updated
                FROM progress_trackers
                WHERE processor_type = $1
            """
            process = await self.db.fetchrow(process_query, self.processor_type)
            
            if not process:
                return {}

            counts_query = """
                SELECT 
                    COUNT(*) FILTER (WHERE status = $1) as processed_count,
                    COUNT(*) FILTER (WHERE status = $2) as failed_count
                FROM processed_items
                WHERE processor_type = $3
            """
            counts = await self.db.fetchrow(
                counts_query,
                ProcessingStatus.COMPLETED.value,
                ProcessingStatus.FAILED.value,
                self.processor_type
            )
            
            total_items = process['total_items']
            processed_count = counts['processed_count']
            failed_count = counts['failed_count']
            
            success_rate = (processed_count / total_items * 100) if total_items > 0 else 0
            
            processing_time = process['last_updated'] - process['start_time']
            processing_second = processing_time.total_seconds()
            
            average_speed = processed_count / processing_second if processing_second > 0 else 0
            remaining_items = total_items - processed_count
            
            return {
                'total_items': total_items,
                'processed_count': processed_count,
                'failed_count': failed_count,
                'success_rate': round(success_rate, 2),
                'processing_time': str(processing_time),
                'average_speed': round(average_speed, 2),
                'remaining_items': remaining_items,
                'estimated_time': self.calculate_estimated_time(average_speed, remaining_items),
                'status': process['status']
            }

        except Exception as e:
            self.logger.error(f"Error getting statistics: {str(e)}")
            return {}

    async def get_failed_items_report(self) -> List[Dict]:
        """Get report of failed items"""
        self._validate_processor_type()
        
        try:
            query = """
                SELECT 
                    item_id,
                    error,
                    metadata,
                    processed_at
                FROM processed_items
                WHERE processor_type = $1
                AND status = $2
                ORDER BY processed_at DESC
            """
            
            failed_items = await self.db.fetch(
                query,
                self.processor_type,
                ProcessingStatus.FAILED.value
            )
            
            return [{
                'item_id': item['item_id'],
                'error': item['error'],
                'metadata': item['metadata'],
                'timestamp': item['processed_at'].isoformat()
            } for item in failed_items]

        except Exception as e:
            self.logger.error(f"Error getting failed items report: {str(e)}")
            return []

    async def next_page(self, current_page: int) -> None:
        """Update the current page"""
        self._validate_processor_type()
        
        try:
            query = """
                UPDATE progress_trackers 
                SET 
                    current_page = $2,
                    last_updated = NOW()
                WHERE processor_type = $1
            """
            
            await self.db.execute(
                query, 
                self.processor_type,
                current_page,
            )
            
        except Exception as e:
            self.logger.error(f"Error updating progress: {str(e)}")
            raise

    async def update_status(self, status: ProcessingStatus) -> None:
        """Update the processing status"""
        self._validate_processor_type()
        
        try:
            query = """
                UPDATE progress_trackers
                SET status = $2, last_updated = NOW()
                WHERE processor_type = $1
            """
            
            await self.db.execute(query, self.processor_type, status.value)
            
            self.logger.info(f"Updated status to {status.value}")
            
        except Exception as e:
            self.logger.error(f"Error updating status: {str(e)}")
            raise

    def calculate_estimated_time(self, remaining_items: int, avg_processing_time: float) -> str:
        total_seconds = int((avg_processing_time + (self.config.API_RATE_LIMIT or 0)) * remaining_items)
        
        hours = total_seconds // 3600
        remaining = total_seconds % 3600
        mins = remaining // 60
        secs = remaining % 60
        
        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if mins > 0 or hours > 0:
            parts.append(f"{mins}m")
        if secs > 0 or not parts:
            parts.append(f"{secs}s")
            
        return " ".join(parts)
    
    def save_file_info(self, data: dict) -> None:
        """Save last processed ID to file"""
        self._validate_processor_type()
        
        try:
            last_id_file = f"{self.config.PROGRESS_PATH}/{self.processor_type}.txt"
            with open(last_id_file, 'w') as f:
                f.write(json.dumps(data))
        except Exception as e:
            self.logger.error(
                "Failed to save last processed ID",
                extra={'error': str(e)}
            )

    def get_file_info(self) -> dict:
        """Get last processed ID from file"""
        self._validate_processor_type()
        
        out = {'last_id': 0, 'total_processed': 0, 'failed': 0, 'success': 0}
        try:
            last_id_file = f"{self.config.PROGRESS_PATH}/{self.processor_type}.txt"
            if not os.path.exists(last_id_file):
                return out
          
            with open(last_id_file, 'r') as f:
                content = f.read().strip()
                return json.loads(content) if content else out
        except (FileNotFoundError, ValueError):
            return out
        except Exception as e:
            self.logger.error(
                "Failed to get last processed ID",
                extra={'error': str(e)}
            )
            return out

    def save_to_file(self, data: dict, filename: str = None) -> None:
        """Save API response data to a file"""
        self._validate_processor_type()
        
        if not filename:
            filename = f"{self.config.PROGRESS_PATH}/{self.processor_type}-debug.txt"
        else:
            filename = f"{self.config.PROGRESS_PATH}/{filename}.txt"
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)