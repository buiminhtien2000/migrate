from fastapi import Request, HTTPException
from jobs.manager import JobManager
from typing import Optional
from systems.bitrix_migration import BitrixMigration

async def get_bitrix(request: Request) -> BitrixMigration:
    bitrix = request.app.state.bitrix
    if not bitrix:
        raise HTTPException(
            status_code=500,
            detail="Bitrix migration not initialized"
        )
    return bitrix

async def get_job_manager() -> JobManager:
    return JobManager()