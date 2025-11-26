# jobs/manager.py
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from utils.logger import logger
from utils.exceptions import BusinessError

class JobManager:
    _instance = None
    _status = "stopped"
    _current_task: Optional[asyncio.Task] = None
    _start_time: Optional[datetime] = None
    _progress: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JobManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    async def get_status(cls) -> str:
        """Get current job status"""
        return cls._status

    @classmethod
    async def get_progress(cls) -> Dict[str, Any]:
        """Get current progress"""
        if cls._start_time:
            duration = (datetime.now() - cls._start_time).total_seconds()
            cls._progress["duration"] = f"{duration:.2f}s"
        return cls._progress

    @classmethod
    async def start(cls):
        """Start job processing"""
        try:
            if cls._status == "running":
                raise BusinessError("Job is already running")

            cls._status = "running"
            cls._start_time = datetime.now()
            cls._progress = {
                "start_time": cls._start_time.isoformat(),
                "status": "running",
                "error": None
            }

            logger.info("Job manager started successfully")

        except Exception as e:
            cls._status = "error"
            cls._progress["error"] = str(e)
            cls._progress["status"] = "error"
            logger.error(f"Failed to start job: {str(e)}")
            raise

    @classmethod
    async def stop(cls):
        """Stop job processing"""
        try:
            if cls._status not in ["running", "processing"]:
                raise BusinessError("No running job to stop")

            # Cancel current task if exists
            if cls._current_task and not cls._current_task.done():
                cls._current_task.cancel()
                try:
                    await cls._current_task
                except asyncio.CancelledError:
                    pass

            cls._status = "stopped"
            cls._progress["status"] = "stopped"
            cls._progress["end_time"] = datetime.now().isoformat()

            if cls._start_time:
                duration = (datetime.now() - cls._start_time).total_seconds()
                cls._progress["duration"] = f"{duration:.2f}s"

            logger.info("Job manager stopped successfully")

        except Exception as e:
            cls._status = "error"
            cls._progress["error"] = str(e)
            cls._progress["status"] = "error"
            logger.error(f"Failed to stop job: {str(e)}")
            raise

    @classmethod
    async def resume(cls):
        """Resume stopped job"""
        try:
            if cls._status != "stopped":
                raise BusinessError("Can only resume stopped job")

            cls._status = "running"
            cls._progress["status"] = "running"
            cls._progress["resumed_at"] = datetime.now().isoformat()

            logger.info("Job manager resumed successfully")

        except Exception as e:
            cls._status = "error" 
            cls._progress["error"] = str(e)
            cls._progress["status"] = "error"
            logger.error(f"Failed to resume job: {str(e)}")
            raise

    @classmethod 
    async def reset_state(cls):
        """Reset job state"""
        try:
            # Stop if running
            if cls._status in ["running", "processing"]:
                await cls.stop()

            # Reset all state
            cls._status = "stopped"
            cls._current_task = None
            cls._start_time = None
            cls._progress = {}

            logger.info("Job manager state reset successfully")

        except Exception as e:
            cls._status = "error"
            cls._progress["error"] = str(e)
            cls._progress["status"] = "error"
            logger.error(f"Failed to reset job state: {str(e)}")
            raise

    @classmethod
    def update_progress(cls, progress_data: Dict[str, Any]):
        """Update job progress"""
        cls._progress.update(progress_data)

    @classmethod
    def set_current_task(cls, task: asyncio.Task):
        """Set current running task"""
        cls._current_task = task