# src/bitrix_migration.py
import os
from datetime import datetime
from pathlib import Path
import unicodedata
import asyncio
from decorators.circuit_breaker import CircuitBreaker
from decorators.process_lock import ProcessLock
from decorators.retry import Retry, RetryConfig
from repositories.mapping import MappingRepository
from api.bitrix_api import BitrixAPI
from utils.progress_tracker import ProgressTracker
from utils.backup_manager import BackupManager
from utils.downloader import Downloader
from utils.notification import Notification
from utils.auth_manager import AuthManager
from processors.bitrix import (
    CompanyProcessor,
    ContactProcessor,
    DealProcessor,
    LeadProcessor,
    ProductProcessor,
    CommentProcessor,
    TaskProcessor,
    DealProductsProcessor,
)
from utils.exceptions import (
    APIError,
    BitrixAPIError,
    ExternalAPIError, 
    APITimeoutError,
    AuthError,
    BusinessError,
)
from utils.excel import Excel

class BitrixMigration:
    def __init__(self, db: 'Database', config: 'Config', logger: 'CustomLogger', background_mode: bool = False) -> None: # type: ignore
        """Initialize the migration tool"""
        self.db = db
        self.config = config
        self.logger = logger
        self.background_mode = background_mode
        
        self.processors = {}
        self.bg_processors = {}
       
        self.bg_processor_mapping = {
            'deal_product': DealProductsProcessor,
            'comment': CommentProcessor,
            'task': TaskProcessor,
        }
        self.processor_mapping = {
            'company': CompanyProcessor,
            'contact': ContactProcessor,
            'lead': LeadProcessor,
            'deal': DealProcessor, 
            # 'product': ProductProcessor,
        }
            
        if self.config.DEBUG_MODE:
            self.logger.warning(f"Initializing Migration Tool in DEBUG MODE")

        AuthManager.create_configs(self.config.AUTH_DIR)
        
        # Initialize APIs
        self.source_api = BitrixAPI(
            logger=logger,
            auth_dir=self.config.AUTH_DIR,
            is_source=True,
            rate_limit=self.config.API_RATE_LIMIT
        )
        self.target_api = BitrixAPI(
            logger=logger,
            auth_dir=self.config.AUTH_DIR,
            is_source=False,
            rate_limit=self.config.API_RATE_LIMIT
        )
        
        # Initialize utilities
        self.circuit_breaker = CircuitBreaker(
            name=f"{self.__class__.__name__}_CB",
            logger=self.logger,
            failure_threshold=self.config.CIRCUIT_BREAKER_THRESHOLD,
            reset_timeout=self.config.CIRCUIT_BREAKER_RECOVERY_TIME
        )

        self.retry = Retry(
            retry_config=RetryConfig(
                max_delay=self.config.RETRY_DELAY,
                max_retries=self.config.MAX_RETRIES
            ),
            retry_on_exceptions=(
                APITimeoutError,
                BitrixAPIError, 
                ExternalAPIError,
                APIError,
                Exception
            ),
            exclude_exceptions=(AuthError,),
            logger=self.logger
        )
        
        self.backup_manager = BackupManager(self.config, self.logger)
        
        self.downloader = Downloader(self.config, self.logger)
        
        self.progress_tracker = ProgressTracker(self.db, self.config, self.logger)
        
        self.mapping_repository = MappingRepository(self.db, self.config)
        
        self.notification = Notification(self.config, self.logger)

        self.process_lock = ProcessLock(self.config, self.logger)

        self.excel = Excel(self.logger, self.config.PROGRESS_PATH)
        self.mapping_fields = self.excel.get_mappings_sync(self.config.from_file.get('MAPPING_FIELDS_URL'))
       
        # Create necessary directories
        self._create_directories()
        
        # Initialize processors
        self._init_processors()

    def _create_directories(self):
        """Create necessary directories"""
        directories = [
            self.config.DATA_DIR,
            self.config.BACKUP_DIR,
            self.config.PROGRESS_PATH,
            self.config.AUTH_DIR,
            self.config.DOWNLOAD_DIR,
            'logs'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)

    def _init_processors(self):
      """Initialize data processors"""
      try:
          # Create processor initialization parameters
          processor_params = {
              'db': self.db,
              'config': self.config,
              'logger': self.logger,
              'downloader': self.downloader,
              'notification': self.notification,
              'process_lock': self.process_lock,
              'source_api': self.source_api,
              'target_api': self.target_api,
              'retry': self.retry,
              'circuit_breaker': self.circuit_breaker,
              'progress_tracker': self.progress_tracker,
              'backup_manager': self.backup_manager,
              'mapping_repository': self.mapping_repository,
              'mapping_fields': self.mapping_fields,
              'excel': self.excel,
          }
          
          # Verify all required params exist
          for param_name, param_value in processor_params.items():
              if param_value is None:
                  raise BusinessError(f"Missing required parameter: {param_name}")
          
          for entity_type, processor_class in self.processor_mapping.items():
              try:
                  processor = processor_class(**processor_params)
                  self.processors[entity_type] = processor
                  self.logger.debug(f"Successfully initialized {entity_type} processor")
                  
              except Exception as e:
                  error_msg = f"Failed to initialize {entity_type} processor: {str(e)}"
                  self.logger.error(error_msg)
                  raise
          
          for entity_type, processor_class in self.bg_processor_mapping.items():
              try:
                  bg_processor = processor_class(**processor_params)
                  self.bg_processors[entity_type] = bg_processor
                  self.logger.debug(f"Successfully initialized {entity_type} processor")
                  
              except Exception as e:
                  error_msg = f"Failed to initialize {entity_type} processor: {str(e)}"
                  self.logger.error(error_msg)
                  raise

      except Exception as e:
          error_msg = f"Processor initialization failed: {str(e)}"
          self.logger.error(error_msg)
          raise BusinessError(error_msg)
    
    def _normalize_text(self, text):
        if text is None:
            return ''
        try:
            normalized = unicodedata.normalize('NFC', str(text))
            return normalized.lower().strip()
        except Exception:
            return ''
        
    async def _get_mapping_info(self, sourceTypeId: str, targetTypeId: str) -> dict:
        """
        Create mapping file between two Bitrix portals based on matching labels
        Files are stored in .mappings folder
        Skip if mapping file already exists
        
        Args:
            entityTypeId: Type of entity to map fields for
                "1": Lead
                "2": Deal
                "3": Contact
                "4": Company
                "7": Quote
                "8": Address
        
        Returns:
            None if entityTypeId is invalid or file already exists
        """

        # Define valid endpoints and names
        endpoints = {
            "1": ('crm.lead.fields', 'lead'),
            "2": ('crm.deal.fields', 'deal'),
            "3": ('crm.contact.fields', 'contact'), 
            "4": ('crm.company.fields', 'company'),
            "7": ('crm.quote.fields', 'quote'),
            "8": ('crm.address.fields', 'address'),
            "-1": ('crm.item.fields', 'spa'),
            "product-fields": ('crm.product.fields', 'product-fields'),
            "products": ('crm.product.list', 'products'),
        }

        # For source entityTypeId
        sourceParams = {}
        sourceKey = str(sourceTypeId)
        if sourceKey not in endpoints:
            _sourceTypeId = "-1"
            sourceParams['entityTypeId'] = sourceTypeId
        else:
            _sourceTypeId = sourceKey
            
        sourceEndpoint, sourceEntityName = endpoints[_sourceTypeId]

        if _sourceTypeId == "-1":
            sourceEntityName += sourceKey

        # For target entityTypeId
        targetParams = {}
        targetKey = str(targetTypeId)
        if targetKey not in endpoints:
            _targetTypeId = "-1"
            targetParams['entityTypeId'] = targetTypeId
        else:
            _targetTypeId = targetKey
            
        targetEndpoint, targetEntityName = endpoints[_targetTypeId]

        if _targetTypeId == "-1":
            targetEntityName += targetKey    
        
        filename = os.path.join(self.config.DOWNLOAD_DIR, f"mapping-fields.xlsx")
        
        # Get fields from both portals
        source_fields = await self.source_api.call(method=sourceEndpoint, params=sourceParams, fullData=True)
        target_fields = await self.target_api.call(method=targetEndpoint, params=targetParams, fullData=True)
        
        if source_fields is None or target_fields is None:
            self.logger.debug(f"Create mapping fields file is failed: {filename} - {sourceEndpoint}")
            return None

        mapping_data = []

        if _targetTypeId == "-1":
            target_fields = target_fields.get('fields', {})
            
        if _sourceTypeId == "-1":
            source_fields = source_fields.get('fields', {})
        
        if _sourceTypeId == "products" and _targetTypeId == "products":
            target_fields = {self._normalize_text(item['NAME']): item['ID'] for item in target_fields}

            for item1 in source_fields:
                mapping = {
                    'old': item1['ID'],
                    'new': '',
                    'label': item1['NAME'],
                }
                name1 = self._normalize_text(item1['NAME'])
                if name1 in target_fields:
                    mapping['new'] = target_fields[name1]
                    
                mapping_data.append(mapping)
        else:
            def get_field_label(field_data: dict) -> str:
                """Get field label with priority: formLabel > title"""
                label = field_data.get('formLabel') or field_data.get('title', '')
                return self._normalize_text(label)

            # Create field_name and label mappings for target portal
            target_field_names = set(target_fields.keys())
            target_label_map = {
                get_field_label(field_data): field_name 
                for field_name, field_data in target_fields.items()
            }
            
            # Create mapping data
            for field_name, field_data in source_fields.items():
                source_label = get_field_label(field_data)
                # First priority: Check if field_name exists in target
                if field_name in target_field_names:
                    target_field_name = field_name
                else:
                    # Second priority: Check by label
                    target_field_name = target_label_map.get(source_label, '')
                
                target_note = get_field_label(target_fields.get(target_field_name, {}))
                
                mapping_data.append({
                    'old': field_name,
                    'new': target_field_name,
                    'new_label': target_note,
                    'old_label': source_label
                })

        sheetName = sourceEntityName if sourceEntityName == targetEntityName else f"{sourceEntityName}_{targetEntityName}"
        
        return {
          "name": sheetName,
          "data": mapping_data
        }

    async def create_mapping_file(self, mappings: list[str, any]) -> dict:
        
        file_path = f"{self.config.DOWNLOAD_DIR}/mapping-fields.xlsx"

        if os.path.exists(file_path):
            os.remove(file_path)
            self.logger.info(f"Removed existing file: {file_path}")

        mapping_results = []

        for item in mappings:
            try:
                if isinstance(item, list):
                    source_entity, target_entity = item
                else:
                    source_entity = target_entity = item

                mapping_data = await self._get_mapping_info(
                    sourceTypeId=source_entity,
                    targetTypeId=target_entity,
                )

                mapping_results.append(mapping_data)
                
            except Exception as e:
                mapping_results.append({
                    "mapping": [source_entity, target_entity],
                    "status": "error",
                    "error": str(e)
                })
                self.logger.error(
                    f"Error processing mapping source:{source_entity} "
                    f"target:{target_entity}: {str(e)}"
                )
        
        excel = Excel(logger=self.logger, output_path=self.config.DOWNLOAD_DIR)
        excel.export_to_spreadsheet('mapping-fields.xlsx', mapping_results)
        
    async def run(self):
        """Execute the migration process"""
        try:

            if self.config.DEBUG_MODE:
                self.logger.warning("Starting migration in DEBUG MODE")
            else:
                self.logger.info("Starting migration process...")
            
            start_time = datetime.now()
            
            # Validate connections
            if not await self.check_connection():
                raise ConnectionError("Failed to validate API connections")
            
            processor_order = list(self.processor_mapping.keys())
            
            for entity_type in processor_order:
                self.logger.info(f"Starting {entity_type} migration...")
                processor = self.processors[entity_type]
                
                await processor.process()    

                if entity_type == 'company':
                    await processor.update_company_logo()

                if processor.entityTypeId:
                    await processor.target_api.update_field_files(processor.entityTypeId)

                self.logger.info(f"Completed {entity_type} migration")
            
            bg_processor_order = list(self.bg_processor_mapping.keys())
            
            for entity_type in bg_processor_order:
                self.logger.info(f"Starting {entity_type} migration...")
                bg_processor = self.bg_processors[entity_type]
                
                await bg_processor.background()    

                self.logger.info(f"Completed {entity_type} migration")
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            if self.config.DEBUG_MODE:
                self.logger.warning(f"DEBUG MODE migration completed in {duration}")
            else:
                self.logger.info(f"Migration completed successfully in {duration}")
            
        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            raise
            
        finally:
            await self.cleanup()
            
    async def cleanup(self):
        """Cleanup resources"""
        try:
            await self.source_api.close()
            await self.target_api.close()
            await self.excel.close()
            await self.downloader.close()
            await self.notification.close()
            self.logger.info("Bitrix migration cleanup completed")
        except Exception as e:
            self.logger.error(f"Bitrix migration cleanup failed: {str(e)}")

    async def check_connection(self) -> bool:
        """Validate connection to both systems"""
        try:
            await self.source_api.check_connection()
            await self.target_api.check_connection()
            return True
        except BitrixAPIError as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False

    async def refresh(self, confirm: bool = False) -> None:
        """
        Refresh all data: clear database, files, excel reports and reset processing state.
        
        Args:
            confirm (bool): Safety confirmation flag
        """
        if not confirm:
            raise ValueError("Set confirm=True to proceed with refresh")
        
        try:
            # Clean database
            tasks = [
                await self.db.drop_database(),
                # self.circuit_breaker.reset() if hasattr(self, 'circuit_breaker') else None
            ]
            await asyncio.gather(*[t for t in tasks if t is not None])

            progress_dir = self.config.PROGRESS_PATH
            await self.empty_folder(progress_dir)
            
            backups_dir = self.config.BACKUP_DIR
            await self.empty_folder(backups_dir)

            backups_dir = self.config.DOWNLOAD_DIR
            await self.empty_folder(backups_dir)

        except Exception as e:
            self.logger.error(f"Refresh failed: {str(e)}")
            raise

    async def empty_folder(self, remove_dir) -> None:
        if remove_dir and os.path.exists(remove_dir):
            for filename in os.listdir(remove_dir):
              file_path = os.path.join(remove_dir, filename)
              try:
                  if os.path.isfile(file_path):
                      os.unlink(file_path)
                  elif os.path.isdir(file_path):
                      import shutil
                      shutil.rmtree(file_path)
              except Exception as e:
                  self.logger.warning(f"Error while removing {filename}: {str(e)}")
            
            # Recreate empty directory
            os.makedirs(remove_dir, exist_ok=True)
