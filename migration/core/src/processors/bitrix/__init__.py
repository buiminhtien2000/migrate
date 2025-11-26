from .company_processor import CompanyProcessor
from .contact_processor import ContactProcessor
from .deal_processor import DealProcessor
from .lead_processor import LeadProcessor
from .product_processor import ProductProcessor
from .task_processor import TaskProcessor
from .comment_processor import CommentProcessor
from .activity_processor import ActivityProcessor
from .deal_products_processor import DealProductsProcessor

__all__ = [
    'CompanyProcessor',
    'ContactProcessor',
    'DealProcessor',
    'LeadProcessor',
    'ProductProcessor',
    'ActivityProcessor'
    'CommentProcessor'
    'TaskProcessor',
    'DealProductsProcessor',
]