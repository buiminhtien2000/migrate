import asyncio
from typing import Any, Optional 
from datetime import datetime, timedelta
from enum import Enum

class ProcessMode(Enum):
    NORMAL = "normal"
    BACKGROUND = "background"

class ProcessLockError(Exception):
    """Base exception for ProcessLock errors"""
    pass

class LockAcquisitionError(ProcessLockError):
    """Raised when lock cannot be acquired"""
    pass

class ProcessLock:
    """
    A class to handle process locking mechanism for both normal and background tasks
    using context manager pattern
    """
    
    def __init__(self, config: 'Config', logger: 'CustomLogger'): # type: ignore
        self._locks = {}
        self._processing_states = {}
        self._lock_validity = {}  # Track lock validity periods
        self._lock_timeout = 300  # Default timeout 5 minutes
        self._current_processor_type = None  # For context manager
        self._current_mode = ProcessMode.NORMAL  # For context manager
        self.config = config
        self.logger = logger

    def __call__(self, processor_type: str, mode: ProcessMode = ProcessMode.NORMAL):
        """Setup for context manager usage"""
        self._current_processor_type = processor_type
        self._current_mode = mode
        return self

    async def __aenter__(self):
        """Enter the context manager"""
        if not self._current_processor_type:
            raise ProcessLockError("Processor type must be specified")

        # Initialize lock and state if needed
        if self._current_processor_type not in self._locks:
            self._locks[self._current_processor_type] = asyncio.Lock()
            self._processing_states[self._current_processor_type] = {
                'is_running': False,
                'mode': None,
                'start_time': None,
                'end_time': None,
                'last_error': None
            }

        if self._current_mode == ProcessMode.BACKGROUND:
            return await self._setup_background_lock()
        else:
            return await self._setup_normal_lock()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager"""
        if self._current_mode == ProcessMode.BACKGROUND:
            if hasattr(self, '_current_refresh_task'):
                self._current_refresh_task.cancel()
                try:
                    await self._current_refresh_task
                except asyncio.CancelledError:
                    pass

        await self.release(self._current_processor_type)
        self._current_processor_type = None
        self._current_mode = ProcessMode.NORMAL

    async def _setup_background_lock(self):
        """Setup for background mode"""
        if self._locks[self._current_processor_type].locked():
            self.logger.warning(
                f"Process {self._current_processor_type} is already running in background mode."
            )
            return None

        await self.acquire(self._current_processor_type)
        self._update_state_start(self._current_processor_type, ProcessMode.BACKGROUND)
        
        # Start auto refresh task
        self._current_refresh_task = asyncio.create_task(
            self._auto_refresh_lock(self._current_processor_type)
        )
        return self

    async def _setup_normal_lock(self):
        """Setup for normal mode"""
        await self.acquire(self._current_processor_type)
        self._update_state_start(self._current_processor_type, ProcessMode.NORMAL)
        return self

    async def is_valid(self, processor_type: str = None) -> bool:
        """Check if lock is still valid"""
        if not processor_type:
            processor_type = self._current_processor_type
            
        if not processor_type:
            return False
        
        validity_time = self._lock_validity.get(processor_type)
        if not validity_time:
            return False
            
        return datetime.now() < validity_time

    async def refresh(self, processor_type: str = None):
        """Refresh lock validity period"""
        if not processor_type:
            processor_type = self._current_processor_type
            
        if processor_type:
            self._lock_validity[processor_type] = datetime.now() + timedelta(seconds=self._lock_timeout)

    async def _auto_refresh_lock(self, processor_type: str):
        """Auto refresh lock periodically"""
        try:
            while True:
                await asyncio.sleep(self._lock_timeout / 2)
                await self.refresh(processor_type)
        except asyncio.CancelledError:
            pass

    def _update_state_start(self, processor_type: str, mode: ProcessMode):
        """Update state when processing starts"""
        self._processing_states[processor_type].update({
            'is_running': True,
            'mode': mode.value,
            'start_time': datetime.now(),
            'end_time': None,
            'last_error': None
        })
        # Set initial lock validity
        self._lock_validity[processor_type] = datetime.now() + timedelta(seconds=self._lock_timeout)

    def _update_state_end(self, processor_type: str):
        """Update state when processing ends"""
        self._processing_states[processor_type].update({
            'is_running': False,
            'end_time': datetime.now()
        })
        # Clear lock validity
        self._lock_validity.pop(processor_type, None)
    
    def is_running(self, processor_type: str) -> bool:
        """Check if a processor is currently running"""
        return self._processing_states.get(processor_type, {}).get('is_running', False)
    
    def get_state(self, processor_type: str) -> dict:
        """Get the current state of a processor"""
        return self._processing_states.get(processor_type, {})

    def get_running_time(self, processor_type: str) -> Optional[timedelta]:
        """Get the current running time of a processor"""
        state = self._processing_states.get(processor_type, {})
        if state.get('is_running') and state.get('start_time'):
            return datetime.now() - state['start_time']
        return None

    def set_timeout(self, timeout: int):
        """Configure lock timeout in seconds"""
        self._lock_timeout = timeout

    async def acquire(self, processor_type: str = None):
        """Acquire lock implementation"""
        if not processor_type:
            processor_type = self._current_processor_type
            
        if not processor_type:
            raise ProcessLockError("Processor type must be specified")
            
        if processor_type not in self._locks:
            self._locks[processor_type] = asyncio.Lock()
            self._processing_states[processor_type] = {
                'is_running': False,
                'mode': None,
                'start_time': None,
                'end_time': None,
                'last_error': None
            }

        try:
            await self._locks[processor_type].acquire()
        except Exception as e:
            raise LockAcquisitionError(f"Failed to acquire lock: {str(e)}")

    async def release(self, processor_type: str = None):
        """Release lock implementation"""
        if not processor_type:
            processor_type = self._current_processor_type
            
        if processor_type and processor_type in self._locks:
            self._update_state_end(processor_type)
            self._locks[processor_type].release()

    async def cleanup(self, processor_type: str = None):
        """Cleanup locks and states"""
        if processor_type:
            self._locks.pop(processor_type, None)
            self._processing_states.pop(processor_type, None)
            self._lock_validity.pop(processor_type, None)
        else:
            self._locks.clear()
            self._processing_states.clear()
            self._lock_validity.clear()