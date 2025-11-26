import os
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Request, Body
from fastapi.responses import StreamingResponse
from jobs.manager import JobManager
from utils.logger import logger
from config import config
from dependencies.auth import verify_token
from dependencies.app import get_bitrix, get_job_manager
from systems.bitrix_migration import BitrixMigration
from utils.database import Database
from collections import deque
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from io import BytesIO
import glob

router = APIRouter()

@router.post("/reset")
async def reset_app(
    request: Request,
    body: dict = Body(default={}),
    bitrix: BitrixMigration = Depends(get_bitrix),
    job_manager: JobManager = Depends(get_job_manager),
    token: str = Depends(verify_token)
):
    """Reset application state"""
    try:
        processors = body.get("processors", None)
    
        if not processors:
            return {
                "detail": {
                    "status": "error",
                    "message": "Please input processors"
                }
            }
        
        if isinstance(processors, str) and processors == "*":
            processors = ["*"]
        is_full_reset = processors == ["*"]
            
        # Stop any running jobs first
        current_status = await job_manager.get_status()
        if current_status in ["running", "processing"]:
            await job_manager.stop()
        
        # Reset database state
        await job_manager.reset_state()
        
        # Chỉ refresh Bitrix khi là full reset
        if is_full_reset:
            await bitrix.refresh(confirm=True)
            logger.info("Full Bitrix refresh completed")
        
        # Xử lý processors
        else:
            db = request.app.state.db
            # Xóa specific processors
            for processor in processors:
                file_pattern = os.path.join(config.PROGRESS_PATH, f"{processor}*")
                matching_files = glob.glob(file_pattern)
                for file_path in matching_files:
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed file: {file_path}")
                    except Exception as file_error:
                        logger.error(f"Failed to remove file {file_path}: {str(file_error)}")
            
            # Xóa specific records trong progress_trackers
            try:
                query = "DELETE FROM progress_trackers WHERE processor_type = ANY($1)"
                await db.execute(query, processors)
                logger.info(f"Removed progress tracker records for processors: {processors}")
            except Exception as db_error:
                logger.error(f"Failed to remove progress tracker records: {str(db_error)}")
                raise
        
        # Reinitialize services
        try:
            new_db = Database(config=config, logger=logger)
            await new_db.initialize()
            request.app.state.db = new_db

            # Khởi tạo lại BitrixMigration
            new_bitrix = BitrixMigration(db=request.app.state.db, config=config, logger=logger)
            request.app.state.bitrix = new_bitrix
            
            # Khởi tạo lại JobManager
            request.app.state.job_manager = JobManager()
            
            logger.info("Services reinitialized successfully")
        except Exception as init_error:
            logger.error(f"Failed to reinitialize services: {str(init_error)}")
            raise HTTPException(
                status_code=500,
                detail=f"Services reinitialization failed: {str(init_error)}"
            )
        
        logger.info("Application reset completed successfully")
        
        return {
            "status": "success",
            "message": "Application reset and reinitialized successfully",
            "reset_type": "full" if is_full_reset else "partial",
            "processors_reset": ["*"] if is_full_reset else processors
        }
        
    except Exception as e:
        logger.error(f"Failed to reset application: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e)
            }
        )
       
@router.post("/start")
async def start_migration(
    background_tasks: BackgroundTasks,
    bitrix: BitrixMigration = Depends(get_bitrix),
    job_manager: JobManager = Depends(get_job_manager),
    token: str = Depends(verify_token)
):
    """Start migration process"""
    try:
        current_status = await job_manager.get_status()
        
        if current_status == "running":
            raise HTTPException(
                status_code=400,
                detail="Migration is already running"
            )
            
        background_tasks.add_task(bitrix.run)
        logger.info("Migration started successfully")
        
        return {
            "status": "success",
            "message": "Migration started successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start migration: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e)
            }
        )

@router.post("/stop")
async def stop_migration(
    bitrix: BitrixMigration = Depends(get_bitrix),
    job_manager: JobManager = Depends(get_job_manager),
    token: str = Depends(verify_token)
):
    """Stop migration process"""
    try:
        current_status = await job_manager.get_status()
        
        if current_status not in ["running", "processing"]:
            raise HTTPException(
                status_code=400,
                detail="Migration is not running"
            )

        # Cleanup Bitrix migration
        await bitrix.cleanup()
            
        await job_manager.stop()
        logger.info("Migration stopped successfully")
        
        return {
            "status": "success",
            "message": "Migration stopped successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to stop migration: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e)
            }
        )

@router.post("/resume")
async def resume_migration(
    background_tasks: BackgroundTasks,
    bitrix: BitrixMigration = Depends(get_bitrix),
    job_manager: JobManager = Depends(get_job_manager),
    token: str = Depends(verify_token)
):
    """Resume migration process"""
    try:
        current_status = await job_manager.get_status()
        
        if current_status != "stopped":
            raise HTTPException(
                status_code=400,
                detail="Migration is not in stopped state"
            )
            
        background_tasks.add_task(bitrix.run)
        logger.info("Migration resumed successfully")
        
        return {
            "status": "success",
            "message": "Migration resumed successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resume migration: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e)
            }
        )

@router.post("/status")
async def get_status(
    bitrix: BitrixMigration = Depends(get_bitrix),
    token: str = Depends(verify_token)
):
    """Get migration status"""
    try:
        # Get status from processors
        status = {}
        for entity, processor in bitrix.processors.items():
            progress = await processor.progress_tracker.get_statistics()
            status[entity] = progress

        return {
            "status": "success",
            "data": status
        }

    except Exception as e:
        logger.error(f"Failed to get status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e)
            }
        )
    
class MappingFieldsRequest(BaseModel):
    mappings: List[Any]
    
    class Config:
        schema_extra = {
            "example": {
                "mappings": [
                    1,
                    2,
                    3,
                    4,
                    [129, 2],
                    "products",
                    "product-fields"
                ]
            }
        }

@router.post("/mapping-fields")
async def mapping_fields(
    request: MappingFieldsRequest,
    background_tasks: BackgroundTasks,
    bitrix: BitrixMigration = Depends(get_bitrix),
    token: str = Depends(verify_token)
):
    """
    Get mapping fields for specified entity mappings
    
    Request body:
    - mappings: Array of [sourceEntity, targetEntity] pairs
    """
    
    try:
        
        background_tasks.add_task(bitrix.create_mapping_file, request.mappings)
        
        return {
            "status": "success",
            "message":  f"Mapping file creation has started in the background. Please wait a few minutes and check at: https://{config.DOMAIN}/bitrix/download-list",
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create mapping fields: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": str(e)
            }
        )

@router.get("/download/{filename}")
async def download_file(
    filename: str,
    # token: str = Depends(verify_token)
):
    """
    Download temporary file by filename
    
    Args:
        request: DownloadTempRequest object containing filename
        token: JWT token for authentication
    """
    try:
        files = os.listdir(config.DOWNLOAD_DIR)
        
        matched_file = None
        for file in files:
            if file.startswith(filename):
                matched_file = file
                break
                
        if not matched_file:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "message": f"File {filename} not found"
                }
            )
        
        file_path = os.path.join(config.DOWNLOAD_DIR, matched_file)
        
        try:
            with open(file_path, 'rb') as file:
                file_content = file.read()
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "message": f"File {filename} not found"
                }
            )
        
        # Create response with generic binary content type
        return StreamingResponse(
            BytesIO(file_content),
            media_type='application/octet-stream',
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Failed to process download: {str(e)}"
            }
        )
    
class FileInfo(BaseModel):
    name: str
    download_url: str

@router.get("/download-list", response_model=List[FileInfo])
async def list_temp_files(
    token: str = Depends(verify_token)
):
    """
    List all files in temp directory
    """
    try:
        files = []
        
        # Get all files and their info
        for filename in os.listdir(config.DOWNLOAD_DIR):
            files.append(FileInfo(
                name=filename,
                download_url=f"https://{config.DOMAIN}/bitrix/download/{filename}"
            ))
                
                
        return files
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Failed to list files: {str(e)}"
            }
        )

@router.post("/run")
async def run_action(
    action: str,
    processor: str,
    background_tasks: BackgroundTasks,
    bitrix: BitrixMigration = Depends(get_bitrix),
    token: str = Depends(verify_token)
):
    """Run specific action as background task"""
    try:
        # Validate processor exists
        if processor not in bitrix.processors:
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "error",
                    "message": f"Invalid processor: {processor}"
                }
            )

        # Get processor instance
        processor_instance = bitrix.processors[processor]
        
        # Add task to background based on action
        if action == 'logo':
            background_tasks.add_task(processor_instance.update_company_logo)
        elif action == 'migrate':
            background_tasks.add_task(processor_instance.run)
        else:
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "error",
                    "message": f"Invalid action: {action}. Supported actions: logo, migrate"
                }
            )

        return {
            "status": "success", 
            "message": f"Action {action} started in background for processor {processor}"
        }

    except Exception as e:
        logger.error(f"Failed to start action {action} for processor {processor}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Failed to start action {action} for processor {processor}"
            }
        )
               
@router.post("/logs")
async def get_latest_logs(
    lines: int = 100,
    token: str = Depends(verify_token)
) -> dict:
    """
    Get the latest lines from the log file
    Args:
        lines: Number of lines to return (default 100)
    Returns:
        dict: Contains status and log lines
    """
    try:
        log_file = config.LOG_FILE
        if not os.path.exists(log_file):
            raise HTTPException(
                status_code=404,
                detail="Log file not found"
            )

        # Sử dụng deque để lưu n dòng cuối
        latest_logs = deque(maxlen=lines)

        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                latest_logs.append(line.strip())

        return {
            "status": "success",
            "data": {
                "total_lines": len(latest_logs),
                "lines": list(latest_logs)
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read log file: {str(e)}"
        )