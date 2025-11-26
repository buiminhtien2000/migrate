# utils/scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.job import Job
from typing import Optional, Dict, Callable
import logging
from datetime import datetime
import pytz
import json
import os

logger = logging.getLogger(__name__)

class TaskScheduler:
    """
    Singleton class to manage scheduled tasks
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TaskScheduler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self.scheduler = AsyncIOScheduler()
        self.jobs: Dict[str, Job] = {}
        self.status_dir = ".progress/scheduler"
        os.makedirs(self.status_dir, exist_ok=True)
        self._initialized = True
        
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")
            
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")
            
    def add_job(self, 
                job_id: str,
                func: Callable,
                schedule_type: str = "interval",
                **schedule_kwargs) -> Optional[Job]:
        """
        Add a new job to scheduler
        
        Args:
            job_id: Unique identifier for the job
            func: Function to execute
            schedule_type: Type of schedule ('interval', 'cron', 'date')
            **schedule_kwargs: Schedule parameters
        """
        try:
            if job_id in self.jobs:
                logger.warning(f"Job {job_id} already exists. Removing old job.")
                self.remove_job(job_id)
                
            # Create trigger based on schedule type
            if schedule_type == "interval":
                trigger = schedule_kwargs
            elif schedule_type == "cron":
                trigger = CronTrigger(**schedule_kwargs)
            else:
                raise ValueError(f"Unsupported schedule type: {schedule_type}")
                
            # Add job with error handling wrapper
            job = self.scheduler.add_job(
                self._job_wrapper(func, job_id),
                trigger=trigger,
                id=job_id,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=None
            )
            
            self.jobs[job_id] = job
            self._save_job_status(job_id, "scheduled")
            logger.info(f"Added job: {job_id}")
            return job
            
        except Exception as e:
            logger.error(f"Error adding job {job_id}: {str(e)}")
            return None
            
    def remove_job(self, job_id: str):
        """Remove a job from scheduler"""
        if job_id in self.jobs:
            self.scheduler.remove_job(job_id)
            del self.jobs[job_id]
            self._save_job_status(job_id, "removed")
            logger.info(f"Removed job: {job_id}")
            
    def get_job_status(self, job_id: str) -> dict:
        """Get current status of a job"""
        status_file = os.path.join(self.status_dir, f"{job_id}_status.json")
        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                return json.load(f)
        return {"status": "unknown"}
        
    def _job_wrapper(self, func: Callable, job_id: str):
        """Wrapper to handle job execution and status tracking"""
        async def wrapped_func(*args, **kwargs):
            start_time = datetime.now(pytz.UTC)
            try:
                # Update status to running
                self._save_job_status(job_id, "running", start_time=start_time)
                
                # Execute job
                result = await func(*args, **kwargs)
                
                # Update status to completed
                self._save_job_status(
                    job_id, 
                    "completed",
                    start_time=start_time,
                    end_time=datetime.now(pytz.UTC),
                    result=result
                )
                
                return result
                
            except Exception as e:
                # Update status to failed
                self._save_job_status(
                    job_id,
                    "failed",
                    start_time=start_time,
                    end_time=datetime.now(pytz.UTC),
                    error=str(e)
                )
                logger.error(f"Error in job {job_id}: {str(e)}")
                raise
                
        return wrapped_func
        
    def _save_job_status(self, 
                        job_id: str,
                        status: str,
                        **additional_info):
        """Save job status to file"""
        status_file = os.path.join(self.status_dir, f"{job_id}_status.json")
        
        status_data = {
            "job_id": job_id,
            "status": status,
            "updated_at": datetime.now(pytz.UTC).isoformat(),
            **additional_info
        }
        
        # Convert datetime objects to ISO format
        for key, value in status_data.items():
            if isinstance(value, datetime):
                status_data[key] = value.isoformat()
                
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=2)
            
    def get_all_jobs_status(self) -> Dict[str, dict]:
        """Get status of all jobs"""
        all_status = {}
        for job_id in self.jobs.keys():
            all_status[job_id] = self.get_job_status(job_id)
        return all_status