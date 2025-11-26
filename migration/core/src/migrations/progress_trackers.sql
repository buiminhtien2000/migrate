-- Modify progress_trackers table with just current_page
CREATE TABLE IF NOT EXISTS progress_trackers (
    processor_type VARCHAR(50) PRIMARY KEY,
    total_items INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    current_page INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Rest of schema remains the same
CREATE TABLE IF NOT EXISTS processed_items (
    processor_type VARCHAR(50) NOT NULL,
    item_id VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    error TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (processor_type, item_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_processed_items_processor_type ON processed_items(processor_type);
CREATE INDEX IF NOT EXISTS idx_processed_items_status ON processed_items(status);
CREATE INDEX IF NOT EXISTS idx_progress_trackers_status ON progress_trackers(status);
CREATE INDEX IF NOT EXISTS idx_processed_items_processed_at ON processed_items(processed_at);
CREATE INDEX IF NOT EXISTS idx_progress_trackers_current_page ON progress_trackers(current_page);
CREATE INDEX IF NOT EXISTS idx_processed_items_metadata ON processed_items USING gin (metadata);
CREATE INDEX IF NOT EXISTS idx_progress_trackers_metadata ON progress_trackers USING gin (metadata);