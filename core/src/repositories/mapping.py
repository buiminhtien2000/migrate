from typing import Optional
from models.mapping import EntityMapping

class MappingRepository:
    def __init__(self, db: 'Database', config: 'Config'): # type: ignore
        self.db = db
        self.config = config

    async def save(self, mapping: EntityMapping) -> EntityMapping:
        query = """
            INSERT INTO mappings (processor_type, source_id, target_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (processor_type, source_id) 
            DO UPDATE SET 
                target_id = EXCLUDED.target_id
            RETURNING id, processor_type, source_id, target_id, created_at
        """
        
        record = await self.db.fetchrow(
            query,
            mapping.processor_type,
            mapping.source_id,
            mapping.target_id
        )
        return record

    async def get_by_source(self, processor_type: str, source_id: str) -> Optional[EntityMapping]:
        query = """
            SELECT id, processor_type, source_id, target_id, created_at
            FROM mappings
            WHERE processor_type = $1 AND source_id = $2
        """
        
        record = await self.db.fetchrow(query, processor_type, source_id)
        return EntityMapping.parse_obj(dict(record)) if record else None  # Convert record to dict

    async def delete_all(self) -> None:
        """Delete all mappings from the database"""
        query = "DELETE FROM mappings"
        try:
            await self.db.execute(query)
        except Exception as e:
            raise  