CREATE TABLE IF NOT EXISTS mappings (
    id SERIAL PRIMARY KEY,
    processor_type VARCHAR(50) NOT NULL,
    source_id VARCHAR(100) NOT NULL,
    target_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uix_entity_source UNIQUE (processor_type, source_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_processor_type ON mappings(processor_type);
CREATE INDEX IF NOT EXISTS idx_source_id ON mappings(source_id);