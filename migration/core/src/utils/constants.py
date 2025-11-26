#constants.py
from enum import Enum

class ProcessorType(Enum):
    COMPANY = 'CompanyProcessor'
    CONTACT = 'ContactProcessor'
    DEAL = 'DealProcessor'
    LEAD = 'LeadProcessor'

class ProcessingStatus(Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class ItemStatus(Enum):
    PROCESSED = "processed"
    FAILED = "failed"
    