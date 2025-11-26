import asyncio
import signal
import uvicorn
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import config
from utils.logger import logger
from utils.database import Database
from routes.oauth import router as oauth_router
from routes.bitrix import router as bitrix_router
from systems.bitrix_migration import BitrixMigration

app = FastAPI(
    title="Migration Tool",
    description="API for data migration",
    docs_url=None if not config.DEBUG else "/docs",
    redoc_url=None if not config.DEBUG else "/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add routes
app.include_router(oauth_router, prefix="/oauth", tags=["OAuth2"])
app.include_router(bitrix_router, prefix="/bitrix", tags=["Bitrix service"])

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint returning application information"""
    return {
        "app": "Migration Tool",
        "status": "running",
        "environment": 'Production' if not config.DEBUG else 'Dev',
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        # Check database health
        db_healthy = await app.state.db.health_check() if app.state.db else False
        
        # Check Bitrix connection
        bitrix_healthy = False
        if app.state.bitrix:
            try:
                bitrix_healthy = await app.state.bitrix.check_connection()
            except:
                pass

        status = "healthy" if (db_healthy and bitrix_healthy) else "unhealthy"
        
        return {
            "status": status,
            "checks": {
                "database": "connected" if db_healthy else "disconnected",
                "bitrix": "connected" if bitrix_healthy else "disconnected"
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.on_event("startup")
async def startup_event():
    """Initialize application resources on startup"""
    try:
        logger.info("Initializing migration tool...")

        # Initialize database
        app.state.db = Database(config=config, logger=logger)
        await app.state.db.initialize()
        
        if not await app.state.db.health_check():
            raise Exception("Database health check failed")

        # Initialize Bitrix migration
        app.state.bitrix = BitrixMigration(db=app.state.db, config=config, logger=logger)
        
        # Verify Bitrix connection
        if not await app.state.bitrix.check_connection():
            logger.warning("Cannot connect to Bitrix24")

        if config.DEBUG_MODE:
            logger.info("Running in DEBUG mode")
            
        logger.info("Application startup completed successfully")

    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown"""
    logger.info("Starting cleanup process...")
    try:
        if app.state.bitrix:
            await app.state.bitrix.cleanup()
            
        if app.state.db:
            await app.state.db.close()
            
        logger.info("Cleanup completed successfully")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")

def handle_signals(loop):
    """Handle system signals"""
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(loop, sig))
        )

async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown"""
    if signal:
        logger.info(f"Received exit signal {signal.name}")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

def run_app():
    """Run the FastAPI application"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        handle_signals(loop)
        config_dict = {
            "app": "main:app",
            "host": config.APP_HOST,
            "port": config.APP_PORT,
            "loop": loop,
            "reload": config.DEBUG_MODE,
            "workers": config.WORKERS if hasattr(config, 'WORKERS') else 1,
            "log_level": "debug" if config.DEBUG_MODE else "info"
        }
        
        uvicorn.run(**config_dict)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop=loop)))
        finally:
            loop.close()
            logger.info("Event loop closed")

if __name__ == "__main__":
    run_app()