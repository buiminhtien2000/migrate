import os
import re
import json
from urllib.parse import urlparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import asyncio
from models.mapping import EntityMapping
from utils.exceptions import (
    AppException,
    APIError,
    BitrixAPIError,
    ExternalAPIError, 
    APITimeoutError,
    AuthError,
    BusinessError,
)
from decorators.circuit_breaker import CircuitBreakerError
from decorators.retry import RetryError
from decorators.process_lock import ProcessMode

class ProcessingMetrics:
    def __init__(self):
        self.start_time = datetime.now()
        self.total_items = 0
        self.total_processed = 0
        self.total_page_processed = 0
        self.current_page_failed = 0
        self.current_page = 1
        self.offset = 0
        self.last_processed_id = 0
        self.failed = 0
        self.has_run = False
        
    def update_page_metrics(self, page_processed: int, page_failed: int):
        self.total_processed += page_processed
        self.total_page_processed = page_processed
        self.current_page_failed = page_failed
        
    def get_debug_metrics(self, current_page_start: datetime, estimated_time: str) -> dict:
        current_duration = (datetime.now() - current_page_start).total_seconds()
        items_per_second = self.total_page_processed / current_duration if current_duration > 0 else 0
        
        return {
            'total_items': self.total_items,
            'total_processed': self.total_processed,
            'estimated_time': estimated_time,
            'current_page': {
                'page': self.current_page,
                'duration': f"{current_duration:.2f}s",
                'items_per_second': f"{items_per_second:.2f}",
                'processed': self.total_page_processed,
                'failed': self.current_page_failed,
                'error_rate': f"{(self.current_page_failed / self.total_page_processed * 100 if self.total_page_processed > 0 else 0):.2f}%",
            }
        }

    def get_final_metrics(self, end_time: datetime) -> dict:
        final_duration = (end_time - self.start_time).total_seconds()
        return {
            'total_duration': final_duration,
            'avg_speed': self.total_processed / final_duration if final_duration > 0 else 0,
            'error_rate': self.failed / self.total_processed if self.total_processed > 0 else 0,
            'total_pages': self.current_page,
            'total_items': self.total_processed,
            'failed_items': self.failed,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat()
        }

class BaseProcessor:
    """Base class for processing data with error handling and progress tracking."""

    REQUIRED_DEPENDENCIES = {
        'db': 'Database connection',
        'config': 'Configuration settings',
        'logger': 'Custom logger',
        'downloader': 'Downloader service',
        'notification': 'Notification service',
        'source_api': 'Source API client',
        'target_api': 'Target API client', 
        'retry': 'Retry service',
        'circuit_breaker': 'Circuit breaker service',
        'progress_tracker': 'Progress tracking service',
        'process_lock': 'Process lock service',
        'backup_manager': 'Backup management service',
        'mapping_repository': 'Mapping repository',
        'mapping_fields': 'Mapping fields',
        'excel': 'Excel service',
    }

    def __init__(self, **kwargs):
        """Initialize processor with dependencies and configuration."""
        self._validate_dependencies(kwargs)
        self._setup_configuration(kwargs.get('config'))
        # Setup dependencies and services
        for key, value in kwargs.items():
            setattr(self, key, value)
        self._setup_processing_state()
        self._set_processor_type()

    def _set_processor_type(self):
      self.processor_type = self.__class__.__name__
      self.progress_tracker.processor_type = self.processor_type
        
    def _validate_dependencies(self, dependencies: dict) -> None:
        """Validate that all required dependencies are provided."""
        missing_deps = [dep for dep in self.REQUIRED_DEPENDENCIES if dep not in dependencies]
        if missing_deps:
            missing_desc = [f"- {dep}: {self.REQUIRED_DEPENDENCIES[dep]}" for dep in missing_deps]
            raise ValueError("Missing required dependencies:\n" + "\n".join(missing_desc))

    def _setup_configuration(self, config: Dict[str, Any]) -> None:
        """Setup configuration with validation."""
        required_fields = {'PROGRESS_PATH', 'BATCH_SIZE', 'MAX_RETRIES'}
        if not all(field in config for field in required_fields):
            raise ValueError("Invalid configuration: missing required fields")
        self.config = config
        self.debug_mode = config['DEBUG_MODE']
        self.debug_sample_size = config['DEBUG_SAMPLE_SIZE']
        self.batch_size = config['BATCH_SIZE']
        self.max_retries = config['MAX_RETRIES']
        self.retry_delay = config['RETRY_DELAY']
        self.cleanup_days = config['CLEANUP_DAYS']
        self.max_buffer_size = config['MAX_BUFFER_SIZE']
        self.enable_resume_timeout = config['ENABLE_RESUME_TIMEOUT']
        self.resume_timeout_hours = config['RESUME_TIMEOUT_HOURS']
        self.exponential_backoff = config['EXPONENTIAL_BACKOFF']
        self.progress_dir = config['PROGRESS_PATH']
        self.pages_per_batch = config['PAGES_PER_BATCH']
        self.page_size = config['PAGE_SIZE']
        os.makedirs(self.progress_dir, exist_ok=True)
        self.failed_items_file = os.path.join(
            self.progress_dir,
            f"failed_items_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        self._last_save_time = datetime.now()
        self._save_interval = 300
        self._state_lock = asyncio.Lock()
        self.notify = False
        if config.NOTIFICATION_CHANNELS:
          self.notify = True

    def _setup_processing_state(self) -> None:
        """Initialize processing state variables."""
        self._processing_paused = False
        self._should_pause = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._total_items = 0
        
    async def background(self) -> None:
        """
        Background processing workflow with progress tracking and pagination support. 
        Similar to process() but optimized for background execution.
        """
        self._set_processor_type()
        
        async with self.process_lock(self.processor_type, mode=ProcessMode.BACKGROUND) as lock:
            metrics = ProcessingMetrics()
            should_stop_processing = False
            is_resume = False
            
            try:
                # Check if processor should run
                should_proceed = await self.progress_tracker.should_process_next()
                
                self.logger.debug(f"background() -> should_process_next: {should_proceed}")
                
                if not should_proceed['run']:
                    self.logger.info(f"Background processor {self.processor_type} already completed. Skipping processing.")
                    return

                metrics.has_run = True
                    
                # Initialize or resume processing
                if should_proceed['data']:
                    metrics.total_processed = should_proceed['data'].get('total_processed', 0)
                    metrics.total_items = should_proceed['data'].get('total_items', 0) 
                    metrics.current_page = should_proceed['data'].get('current_page', 1)
                    metrics.offset = self.page_size * (metrics.current_page - 1)

                    resume_state = await self._check_resume_state()
                    if resume_state:
                        info = self.progress_tracker.get_file_info()
                        metrics.last_processed_id = info.get('last_id', metrics.last_processed_id)
                        metrics.total_processed = info.get('total_processed', metrics.total_processed)
                        metrics.failed = info.get('failed', metrics.failed)
                        is_resume = True

                while True:
                    
                    self.logger.debug(f"Processing for page: {metrics.current_page}")
                    
                    if should_stop_processing:
                        break
                        
                    # Sử dụng lock trực tiếp thay vì self.process_lock
                    if not await lock.is_valid():
                        self.logger.warning(f"Lock for {self.processor_type} has expired")
                        break
                        
                    # Check pause event
                    await self._pause_event.wait()
                    
                    # Refresh lock during processing 
                    await lock.refresh()
                    
                    # Fetch items for batch processing
                    batch_items = []
                    current_batch_size = 0
                    
                    response = await self._get_and_validate_items(metrics.current_page)
                    
                    if not response:
                        break

                    batch_items = response['result']

                    if not batch_items:
                        break

                    current_batch_size += len(batch_items)

                    # Initialize processing metrics for first batch
                    if metrics.total_processed == 0 or is_resume:
                        metrics.total_items = response.get('total', len(batch_items))
                        await self.progress_tracker.initialize_processing(
                            metrics.total_items,
                            metrics.current_page,
                            self.page_size
                        )

                    if is_resume:
                        batch_items = self._filter_items_after_id(batch_items, metrics.last_processed_id)
                        is_resume = False
                    
                    # Process current batch
                    metrics.total_page_processed = 0
                    current_batch_start = datetime.now()
                    metrics.current_page_failed = 0

                    try:
                        results = await self._process_batch_items(batch_items)
                        
                        if results:
                            self.logger.debug(f"_process_batch_items: start update data")
                            
                            # Process each item result in the batch
                            for idx, item in enumerate(results):
                                item_id = item['source_id']
                                metrics.total_processed += 1
                                metrics.total_page_processed += 1

                                try:
                                    if item.get('success'):
                                        # Successful processing
                                        await self.progress_tracker.mark_item_processed(item_id)
                                    else:
                                        # Failed processing
                                        metrics.failed += 1
                                        metrics.current_page_failed += 1
                                        error_msg = item.get('error', 'No result in batch response')
                                        await self.handle_failed_item(batch_items[idx], error_msg)
                                        
                                except Exception as e:

                                    self.logger.error(f"Error processing batch item {item_id}: {str(e)}")

                                    metrics.failed += 1
                                    metrics.current_page_failed += 1
                                    await self.handle_failed_item(batch_items[idx], str(e))

                            self.logger.debug(f"_process_batch_items: complete updated data")
                            
                        else:
                            # Handle failed batch
                            _total_batch = len(batch_items)
                            metrics.failed += _total_batch
                            metrics.total_processed += _total_batch
                            metrics.total_page_processed += _total_batch
                            metrics.current_page_failed += _total_batch
                            for item in batch_items:
                                await self.handle_failed_item(item, "Invalid batch result")

                    except Exception as e:
                        self.logger.error(f"Batch processing error: {str(e)}")
                        
                        metrics.failed += len(batch_items)
                        metrics.current_page_failed += len(batch_items)
                        for item in batch_items:
                            await self.handle_failed_item(item, str(e))

                    finally:
                        # Save progress state
                        if batch_items:
                            last_item_id = list(batch_items.keys())[-1]
                            
                            state_data = {
                                'last_id': last_item_id,
                                'total_processed': metrics.total_processed,
                                'failed': metrics.failed,
                                'success': metrics.total_processed - metrics.failed
                            }
                            
                            await self._save_processing_state(state_data)

                    # Debug logging for background process
                    if self.debug_mode:
                        current_duration = (datetime.now() - current_batch_start).total_seconds()
                        items_per_second = metrics.total_page_processed / current_duration if current_duration > 0 else 0
                        estimated_time = self.progress_tracker.calculate_estimated_time(
                            metrics.total_items - metrics.total_processed,
                            items_per_second
                        )
                        
                        batch_metrics = metrics.get_debug_metrics(current_batch_start, estimated_time)

                        self.logger.debug(f"Background processed batch for {self.processor_type}: {json.dumps(batch_metrics, indent=3)}")

                        if self.notify:
                            await self.notification.send(batch_metrics)

                        # Check debug mode limits
                        if metrics.total_processed >= self.debug_sample_size:
                            should_stop_processing = True
                            self.logger.debug(f'Debug mode: Reached sample size of {self.debug_sample_size}. Stopping processing...')
                            break

                    metrics.current_page += 1
                    await self.progress_tracker.next_page(metrics.current_page)
                    await asyncio.sleep(1)  # Slightly longer sleep for background processing
                    
                await self._display_processing_statistics()

            except Exception as e:
                error = f"Background processing error in {self.processor_type}: {str(e)}"
                self.logger.error(error)
                if self.notify:
                    await self.notification.send(error)
                raise
                
            finally:
                if metrics.has_run:
                    processed_at = datetime.now()
                    final_metrics = metrics.get_final_metrics(processed_at)
                    
                    await self.progress_tracker.save_state(final_metrics)

                    if self.notify:
                        await self.notification.send(final_metrics) 
                        
    async def process(self) -> None:
        """
        Main batch processing workflow with progress tracking and pagination support.
        
        Processes items in batches for better performance while maintaining error handling
        and progress tracking capabilities.
        """
        metrics = ProcessingMetrics()
        self._set_processor_type()
        should_stop_processing = False
        is_resume = False
        
        try:
            # Check if processor should run
            should_proceed = await self.progress_tracker.should_process_next()

            self.logger.debug(f"should_process_next: {should_proceed}")
            
            if not should_proceed['run']:
                self.logger.info(f"Processor {self.processor_type} already completed. Skipping processing.")
                return

            metrics.has_run = True
                
            # Initialize or resume processing
            if should_proceed['data']:
                metrics.total_processed = should_proceed['data'].get('total_processed', 0)
                metrics.total_items = should_proceed['data'].get('total_items', 0)
                metrics.current_page = should_proceed['data'].get('current_page', 1)
                metrics.offset = self.page_size * (metrics.current_page - 1)

                resume_state = await self._check_resume_state()
                if resume_state:
                    info = self.progress_tracker.get_file_info()
                    metrics.last_processed_id = info.get('last_id', metrics.last_processed_id)
                    metrics.total_processed = info.get('total_processed', metrics.total_processed)
                    metrics.failed = info.get('failed', metrics.failed)
                    is_resume = True

            while True:
                if should_stop_processing:
                    break
                    
                # Fetch items for batch processing
                batch_items = []
                has_more = False
                current_batch_size = 0
                
                response = await self._get_and_validate_items(metrics.current_page)
                
                if not response:
                    break

                batch_items = response['result']
                has_more = response['next']

                if not batch_items:
                    break

                current_batch_size += len(batch_items)
                
                if is_resume:
                    batch_items = self._filter_items_after_id(batch_items, metrics.last_processed_id)
                    is_resume = False

                # Initialize processing metrics for first batch
                if metrics.total_processed == 0:
                    metrics.total_items = response.get('total', len(batch_items))
                    await self.progress_tracker.initialize_processing(
                        metrics.total_items,
                        metrics.current_page,
                        self.page_size
                    )

                # Process current batch
                metrics.total_page_processed = 0
                current_batch_start = datetime.now()
                metrics.current_page_failed = 0

                try:
                    results = await self._process_batch_items(batch_items)
                    
                    if results:

                        self.logger.debug(f"_process_batch_items: done -> update data")

                        if self.debug_mode:
                            self.progress_tracker.save_to_file(batch_items, f"{self.processor_type}-batch_items")
                            self.progress_tracker.save_to_file(results, f"{self.processor_type}-batch_items-results")

                        # Process each item result in the batch
                        for idx, item in enumerate(results):
                            item_id = batch_items[idx].get('ID') or batch_items[idx].get('id')
                            metrics.total_processed += 1
                            metrics.total_page_processed += 1

                            try:

                                if item.get('success'):
                                    # Successful processing
                                    await asyncio.gather(
                                        self.progress_tracker.mark_item_processed(item_id),
                                        self._save_mapping(item_id, str(item['target_id']))
                                    )
                                else:
                                    # Failed processing
                                    metrics.failed += 1
                                    metrics.current_page_failed += 1
                                    error_msg = item.get('error', 'No result in batch response')
                                    await self.handle_failed_item(batch_items[idx], error_msg)
                                    
                            except Exception as e:

                                self.logger.error(f"Error processing batch item {item_id}: {str(e)}")

                                metrics.failed += 1
                                metrics.current_page_failed += 1
                                await self.handle_failed_item(batch_items[idx], str(e))

                    else:
                        # Handle failed batch
                        _total_batch = len(batch_items)
                        metrics.failed += _total_batch
                        metrics.total_processed += _total_batch
                        metrics.total_page_processed += _total_batch
                        metrics.current_page_failed += _total_batch
                        for item in batch_items:
                            await self.handle_failed_item(item, "Invalid batch result")

                except Exception as e:
                    self.logger.error(f"Batch processing error: {str(e)}")
                    
                    metrics.failed += len(batch_items)
                    metrics.current_page_failed += len(batch_items)
                    for item in batch_items:
                        await self.handle_failed_item(item, str(e))

                finally:
                    # Save progress state
                    if batch_items:
                        last_item = batch_items[-1]
                        last_item_id = str(last_item.get('id') or last_item.get('ID'))
                        await self._save_processing_state({
                            'last_id': last_item_id,
                            'total_processed': metrics.total_processed,
                            'failed': metrics.failed,
                            'success': metrics.total_processed - metrics.failed
                        })

                # Debug logging
                if self.debug_mode:
                    current_duration = (datetime.now() - current_batch_start).total_seconds()
                    items_per_second = metrics.total_page_processed / current_duration if current_duration > 0 else 0
                    estimated_time = self.progress_tracker.calculate_estimated_time(
                        metrics.total_items - metrics.total_processed,
                        items_per_second
                    )
                    
                    batch_metrics = metrics.get_debug_metrics(current_batch_start, estimated_time)

                    self.logger.debug(f"Background processed batch for {self.processor_type}: {json.dumps(batch_metrics, indent=3)}")

                    if self.notify:
                        await self.notification.send(batch_metrics)

                    # Check debug mode limits
                    if metrics.total_processed >= self.debug_sample_size:
                        should_stop_processing = True
                        self.logger.debug(f'Debug mode: Reached sample size of {self.debug_sample_size}. Stopping processing...')
                        break

                # Check for more pages
                if not has_more:
                    break

                metrics.current_page += 1
                await self.progress_tracker.next_page(metrics.current_page)
                await asyncio.sleep(0.5)

            await self.progress_tracker.complete_process()
            await self._display_processing_statistics()

        except Exception as e:
            self.logger.error(f"Processing error in {self.processor_type}: {str(e)}")
            raise
            
        finally:
            if metrics.has_run:
                processed_at = datetime.now()
                final_metrics = metrics.get_final_metrics(processed_at)
                
                await self.progress_tracker.save_state(final_metrics)

                if self.notify:
                    await self.notification.send(final_metrics)

    async def _get_and_validate_items(self, current_page: None) -> List[Dict]:
        """Get and validate items for processing."""
        async with self.circuit_breaker:
            try:
                items = await self.get_all(current_page)
                
                if not isinstance(items, dict) or not items.get('result'):
                    self.logger.warning(f"No valid items found for {self.processor_type}")
                    return []
                
                self.logger.info(f"Found {len(items['result'])} items to process for {self.processor_type}")
                return items

            except (BitrixAPIError, ExternalAPIError) as api_error:
                self.logger.error(f"API error while getting items: {str(api_error)}")
                raise

            except Exception as e:
                self.logger.error(f"Unexpected error while getting items: {str(e)}")
                raise

    def _filter_items_after_id(self, items: List[Dict], last_processed_id: int) -> List[Dict]:
        """Filter items after the last processed ID."""
        if last_processed_id == 0:
            return items
            
        for index, item in enumerate(items):
            if int(item.get('id') or item.get('ID')) == last_processed_id:
                return items[index + 1:]
        return items
      
    async def _process_batch_items(self, items: List[Dict]) -> Optional[List[Dict]]:
        """Process individual item with retries and error handling."""
        
        # Định nghĩa hàm xử lý chính để có thể retry
        @self.retry
        async def _process_with_retry() -> Optional[List[Dict]]:
            return await self.process_items(items)

        try:
            # Circuit breaker bao bọc toàn bộ quá trình retry
            async with self.circuit_breaker:
                try:
                    return await _process_with_retry()
                except BusinessError as e:
                    self.logger.error(
                        "Business logic error",
                        extra={'error': e.to_dict()}
                    )
                    return {'error': e.to_dict()}
                except AuthError as e:
                    self.logger.error(
                        "Authentication failed",
                        extra={'error': e.to_dict()}
                    )
                    raise

        except RetryError as e:
            # Xử lý khi hết số lần retry
            if isinstance(e.last_exception, AuthError):
                raise e.last_exception
            elif isinstance(e.last_exception, (APITimeoutError, BitrixAPIError, ExternalAPIError, APIError)):
                self.logger.error(
                    f"API error after all retries",
                    extra={'error': e.last_exception.to_dict()}
                )
                raise e.last_exception
            else:
                self.logger.error(f"Unhandled error after all retries: {str(e.last_exception)}")
                raise e.last_exception
                
        except CircuitBreakerError as e:
            # Xử lý khi circuit breaker open
            self.logger.error("Circuit breaker is open", extra={'error': str(e)})
            raise
    
    async def _save_processing_state(self, data: dict) -> None:
        """Save current processing state."""

        self.progress_tracker.save_file_info(data)
        
        current_time = datetime.now()
        if (current_time - self._last_save_time).seconds >= self._save_interval:
            self._last_save_time = current_time

    async def _check_resume_state(self) -> Optional[Dict]:
        """Check for valid resume state."""
        state = await self.progress_tracker.get_state()
        if not state:
            return None
            
        if self.enable_resume_timeout:
            last_update = state.get('last_update')
            if last_update:
                last_update_time = datetime.fromisoformat(last_update)
                if (datetime.now() - last_update_time).total_seconds() > self.resume_timeout_hours * 3600:
                    return None
                    
        return state

    async def _display_processing_statistics(self) -> None:
        """Display detailed processing statistics."""
        stats = await self.progress_tracker.get_statistics()

        info = json.dumps(stats, indent=2, ensure_ascii=False)
        
        self.logger.debug(f"Detailed processing statistics: {info}")

        if self.notify:
            await self.notification.send(stats)

    async def _save_mapping(self, item_id: str, target_id: str) -> None:
        try:
            mapping = EntityMapping(
                source_id=item_id,
                target_id=str(target_id),
                processor_type=self.processor_type,
            )
            await self.mapping_repository.save(mapping)
        except Exception as e:
            self.logger.error(f"Error saving mapping: {str(e)}")
            with open(f"{self.processor_type}-failed-mappings.txt", 'a') as f:
                f.write(f"{item_id}:{target_id}\n")

    async def cleanup_old_data(self) -> None:
        """Clean up old processing data."""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.cleanup_days)
            
            # Cleanup old progress files
            for filename in os.listdir(self.progress_dir):
                filepath = os.path.join(self.progress_dir, filename)
                if os.path.getctime(filepath) < cutoff_date.timestamp():
                    os.remove(filepath)
                    self.logger.info(f"Removed old progress file: {filename}")

            # Cleanup old progress tracker data
            await self.progress_tracker.cleanup_old_data(cutoff_date)
            
            self.logger.info(f"Cleanup completed for data older than {cutoff_date}")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    async def handle_failed_item(self, item: Dict, error) -> None:
        """Handle failed item processing."""
        try:
            item_id = str(item.get('id') or item.get('ID'))
            
            await self.progress_tracker.mark_item_failed(
                item_id,
                str(error),
                item
            )

        except Exception as e:
            self.logger.error(f"Error handling failed item: {str(e)} - {item}")

    async def pause_processing(self) -> None:
        """Pause processing."""
        self._should_pause = True
        self._processing_paused = True
        self._pause_event.clear()
        self.logger.info(f"Processing paused for {self.__class__.__name__}")

    async def resume_processing(self) -> None:
        """Resume processing."""
        self._should_pause = False
        self._processing_paused = False
        self._pause_event.set()
        self.logger.info(f"Processing resumed for {self.__class__.__name__}")

    async def cleanup(self):
        """Cleanup resources properly"""
        try:
            for resource in ['db', 'source_api', 'target_api', 'downloader', 'notification']:
                if hasattr(self, resource):
                    try:
                        resource_obj = getattr(self, resource)
                        if hasattr(resource_obj, 'close') and callable(resource_obj.close):
                            await resource_obj.close()
                    except Exception as e:
                        self.logger.warning(f"Error closing {resource}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    async def get_mapping_id(
        self, 
        processor_type: str, 
        record_id: Union[str, int, List[str], List[int], List[Union[str, int]]]
    ) -> Union[str, List[str], None]:
        """
        Get mapping ID(s) from database
        Args:
            processor_type: Type of processor
            record_id: Can be single ID (str/int) or list of IDs
        Returns:
            - For single ID: returns target_id (str) or None
            - For list of IDs: returns list of target_ids or empty list
        """
        try:
            # Handle list input
            if isinstance(record_id, (list, tuple)):
                if not record_id:
                    return []
                    
                str_ids = [str(id) for id in record_id]
                
                query = """
                    SELECT target_id 
                    FROM mappings
                    WHERE processor_type = $1 AND source_id = ANY($2::varchar[])
                """
                rows = await self.db.fetch(query, processor_type, str_ids)
                return [str(row['target_id']) for row in rows]
                
            # Handle single input
            else:
                str_id = str(record_id)
                query = """
                    SELECT target_id 
                    FROM mappings
                    WHERE processor_type = $1 AND source_id = $2::varchar
                    LIMIT 1
                """
                return await self.db.fetchval(query, processor_type, str_id)
                
        except Exception as e:
            self.logger.error(f"Error getting mapping for {processor_type} with ID(s): {record_id}: {str(e)}")
            return [] if isinstance(record_id, (list, tuple)) else None

    def _is_default_key(self, key: str) -> str:
        if key.startswith('{') and key.endswith('}'):
            return key[1:-1]
        return None

    def _chunk_dict(self, data: dict, chunk_size: int = 50) -> list[dict]:
        """
        Split a large dictionary into smaller ones, each containing chunk_size properties
        
        Example:
        data = {
            'key1': value1,
            'key2': value2,
            ...  # 100 keys
        }
        Returns: [
            {key1->key50}, # dict with first 50 keys
            {key51->key100} # dict with next 50 keys
        ]
        """
        items = list(data.items())  # convert dict to list of (key, value) pairs
        return [
            dict(items[i:i + chunk_size]) 
            for i in range(0, len(items), chunk_size)
        ]
        
    def get_mapping_data(self, record: Dict, field_mapping: Dict) -> Optional[Dict]:
        transformed = {}
        
        defaults = self.mapping_fields['default'] or {}
        
        for old_field, new_field in field_mapping.items():
            _default_field = self._is_default_key(old_field)
            if _default_field and _default_field in defaults:
                transformed[new_field] = defaults[_default_field]
            elif old_field in record:
                transformed[new_field] = record[old_field]

        transformed['ORIGIN_ID'] = record.get('ID')
        transformed['ASSIGNED_BY_ID'] =  self.get_mapping_users(transformed['ASSIGNED_BY_ID'])
        
        return transformed

    def get_mapping_users(self, keys: Union[str, List[str]]) -> Union[str, List[str]]:
        """
        Get values from dictionary based on input keys
        
        Args:
            keys: Single key (str) or list of keys to search for
                
        Returns:
            Single value (str) if input is single key
            List of values if input is list of keys
            For list input:
                - If users[key] is None, replace with default['assignedById'] if not None
                - If both users[key] and default['assignedById'] are None, keep original key
        """
        default = self.mapping_fields['default'] or {}
        users = self.mapping_fields['user'] or {}
        default_assigned = default.get('ASSIGNED_BY_ID', 1)
        
        def get_value(key: str) -> str:
            return users.get(key, default_assigned or key)
        
        # Nếu input là single value (str)
        if isinstance(keys, str):
            return get_value(keys)
            
        # Nếu input là list 
        result = []
        for key in keys:
            value = users.get(key)
            if value is None and default_assigned is not None:
                result.append(default_assigned)
            else:
                result.append(value or key)
        return result
  
    async def handle_file_fields(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Get and process import files from records containing urlMachine or downloadUrl
        
        Args:
            records: List of dictionaries containing data with possible urlMachine or downloadUrl fields
                
        Returns:
            List of processed records with downloaded file data
        """
        if not records:
            self.logger.warning("Empty records received")
            return records

        def process_url(url_dict: dict) -> Optional[str]:
            if not url_dict:
                return None
                    
            if "urlMachine" in url_dict:
                url = url_dict["urlMachine"]
                if not url:
                    self.logger.warning("Empty urlMachine value found")
                    return None
                return url
                    
            elif "downloadUrl" in url_dict:
                token = self.source_api.auth_info.get("token")
                parsed_url = urlparse(self.source_api.webhook_url)
                domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                
                if not (token and domain):
                    self.logger.error("Missing token or domain in auth.json")
                    return None
                        
                download_url = url_dict["downloadUrl"]
                if not download_url:
                    self.logger.warning("Empty downloadUrl value found")
                    return None
                        
                try:
                    if "auth=" in download_url:
                        download_url = re.sub(r'auth=[^&]*', f'auth={token}', download_url)
                    full_url = f"{domain}{download_url}"
                    return full_url
                except Exception as e:
                    self.logger.error(f"Error processing downloadUrl: {str(e)}")
                    return None
                        
            return None

        def has_url_field(d: dict) -> bool:
            return isinstance(d, dict) and ("urlMachine" in d or "downloadUrl" in d)

        result = {}
        for idx, record in enumerate(records):
            record_urls = {}
            
            for field, value in record.items():
                if isinstance(value, dict) and has_url_field(value):
                    # Case: field là dict chứa urlMachine hoặc downloadUrl
                    url = process_url(value)
                    if url:
                        record_urls[field] = url
                        
                elif isinstance(value, list):
                    # Case: field là list các dict chứa urlMachine hoặc downloadUrl
                    urls = []
                    for item in value:
                        if isinstance(item, dict) and has_url_field(item):
                            url = process_url(item)
                            if url:
                                urls.append(url)
                    if urls:
                        record_urls[field] = urls
            
            if record_urls:
                result[str(idx)] = record_urls

        try:
            if result:
                if self.debug_mode:
                    self.progress_tracker.save_to_file(result, f"{self.processor_type}-file-urls")

                # Download files
                files_data = await self.downloader.download_url_batch(result)

                if self.debug_mode:
                    self.progress_tracker.save_to_file(files_data, f"{self.processor_type}-files_data")

                if files_data:
                    # Update original records with downloaded data
                    for idx_str, item in files_data.items():
                        try:
                            idx = int(idx_str)
                            if 0 <= idx < len(records):
                                records[idx].update(item)
                            else:
                                self.logger.error(f"Invalid index {idx} for records update")
                        except ValueError:
                            self.logger.error(f"Invalid index format: {idx_str}")
                else:
                    self.logger.warning("No files were downloaded")
            else:
                self.logger.info("No urlMachine or downloadUrl fields found in records")

            return records

        except Exception as e:
            self.logger.error(f"Error processing import files: {str(e)}", exc_info=True)
            return records         
        
    async def process_items(self, items: List[Dict]) -> Any:
        """Template method for processing batch items. Should be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement process_items method")

    async def get_all(self, current_page) -> Dict[str, List[Dict]]:
        """
        Template method for getting all items. Should be overridden by subclasses.
        Return dict: 
            {
                'result': result or [],
                'total': total,
                'next': current_page * self.batch_size
            }    
        """
        raise NotImplementedError("Subclasses must implement get_all method")

    @property 
    def is_paused(self) -> bool:
        """Check if processing is paused."""
        return self._processing_paused

    @property
    def total_items(self) -> int:
        """Get total number of items to process."""
        return self._total_items

    @property
    def total_batches(self) -> int:
        """Get total number of batches."""
        return (self.total_items + self.batch_size - 1) // self.batch_size