-- Track all datasets and their metadata
CREATE TABLE IF NOT EXISTS public.datasets (
    id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(255) UNIQUE NOT NULL,
    original_filename VARCHAR(500),
    file_type VARCHAR(50), -- csv, json, xlsx, pdf, etc.
    file_size_bytes BIGINT,
    table_name VARCHAR(255), -- dynamically created table name
    row_count INTEGER,
    column_count INTEGER,
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_processed TIMESTAMP,
    processing_status VARCHAR(50) DEFAULT 'uploaded', -- uploaded, processing, completed, failed
    schema_hash VARCHAR(64), -- hash of column structure for change detection
    created_by VARCHAR(100),
    file_path VARCHAR(1000) -- path where original file is stored
);

-- Create indexes for datasets table
CREATE INDEX IF NOT EXISTS idx_dataset_name ON public.datasets (dataset_name);
CREATE INDEX IF NOT EXISTS idx_processing_status ON public.datasets (processing_status);
CREATE INDEX IF NOT EXISTS idx_upload_timestamp ON public.datasets (upload_timestamp DESC);

-- Track dynamic table schemas
CREATE TABLE IF NOT EXISTS public.table_schemas (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    column_position INTEGER,
    data_type VARCHAR(100), -- detected PostgreSQL data type
    original_type VARCHAR(100), -- original type from source
    is_nullable BOOLEAN DEFAULT TRUE,
    max_length INTEGER,
    precision_scale VARCHAR(20), -- for numeric types
    sample_values TEXT[], -- sample values for reference
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(dataset_id, column_name)
);

-- Create indexes for table_schemas
CREATE INDEX IF NOT EXISTS idx_dataset_column ON public.table_schemas (dataset_id, column_name);



-- AI-generated suggestions (your original table, enhanced)
CREATE TABLE IF NOT EXISTS public.ai_suggestions (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
    dataset VARCHAR(255) NOT NULL, -- kept for backward compatibility
    column_name VARCHAR(255) NOT NULL,
    suggestion TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, accepted, rejected, applied
    ai_module VARCHAR(100), -- Profiling, Anomaly Detection, etc.
    issue_type VARCHAR(100), -- Missing Values, Duplicates, Format Error, etc.
    confidence_score DECIMAL(5,2), -- 0-100
    severity VARCHAR(20), -- low, medium, high, critical
    affected_rows INTEGER,
    sample_problematic_values TEXT[],
    suggested_sql TEXT, -- SQL to fix the issue
    user_feedback TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMP -- when the fix was actually applied
);

-- Create indexes for ai_suggestions
CREATE INDEX IF NOT EXISTS idx_dataset_status ON public.ai_suggestions (dataset, status);
CREATE INDEX IF NOT EXISTS idx_dataset_id_status ON public.ai_suggestions (dataset_id, status);
CREATE INDEX IF NOT EXISTS idx_ai_module ON public.ai_suggestions (ai_module);
CREATE INDEX IF NOT EXISTS idx_confidence_score ON public.ai_suggestions (confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_severity ON public.ai_suggestions (severity);

-- Data profiling results for each column
CREATE TABLE IF NOT EXISTS public.data_profiles (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100),
    null_count INTEGER,
    null_percentage DECIMAL(5,2),
    unique_count INTEGER,
    unique_percentage DECIMAL(5,2),
    total_count INTEGER,
    min_value TEXT,
    max_value TEXT,
    avg_value DECIMAL(15,5),
    median_value DECIMAL(15,5),
    std_deviation DECIMAL(15,5),
    most_common_values JSONB, -- [{value, count, percentage}]
    data_quality_score DECIMAL(5,2), -- calculated quality score 0-100
    profile_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    profile_date DATE DEFAULT CURRENT_DATE,
    
    UNIQUE(dataset_id, column_name, profile_date)
);

-- Create indexes for data_profiles
CREATE INDEX IF NOT EXISTS idx_dataset_profile ON public.data_profiles (dataset_id, column_name);

-- Anomaly detection results
CREATE TABLE IF NOT EXISTS public.anomalies (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(255),
    row_identifiers TEXT[], -- primary keys or row numbers of anomalous records
    anomaly_type VARCHAR(100), -- outlier, pattern_break, statistical_anomaly, business_rule
    anomaly_description TEXT,
    confidence_score DECIMAL(5,2),
    anomaly_values JSONB, -- the actual anomalous values
    detection_method VARCHAR(100), -- IQR, Z-score, Isolation Forest, etc.
    threshold_used DECIMAL(10,5),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for anomalies
CREATE INDEX IF NOT EXISTS idx_dataset_anomaly ON public.anomalies (dataset_id, anomaly_type);
CREATE INDEX IF NOT EXISTS idx_detection_method ON public.anomalies (detection_method);



-- Track Airflow DAG runs and processing jobs
CREATE TABLE IF NOT EXISTS public.processing_jobs (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
    job_type VARCHAR(100) NOT NULL, -- data_ingestion, profiling, anomaly_detection, quality_check, rule_application
    status VARCHAR(50) DEFAULT 'pending', -- pending, running, completed, failed, skipped
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    error_message TEXT,
    results_summary JSONB, -- summary of what was found/processed
    airflow_dag_id VARCHAR(255),
    airflow_task_id VARCHAR(255),
    airflow_run_id VARCHAR(255),
    retry_count INTEGER DEFAULT 0
);

-- Create indexes for processing_jobs
CREATE INDEX IF NOT EXISTS idx_dataset_job_type ON public.processing_jobs (dataset_id, job_type);
CREATE INDEX IF NOT EXISTS idx_job_status ON public.processing_jobs (status);
CREATE INDEX IF NOT EXISTS idx_airflow_run ON public.processing_jobs (airflow_dag_id, airflow_run_id);

-- Data quality rules (can be applied automatically)
CREATE TABLE IF NOT EXISTS public.data_quality_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(100) NOT NULL, -- validation, transformation, cleansing
    dataset_pattern VARCHAR(255), -- regex pattern for dataset names this applies to
    column_pattern VARCHAR(255), -- regex pattern for column names
    condition_sql TEXT, -- SQL condition to check
    fix_sql TEXT, -- SQL to apply the fix
    ai_module VARCHAR(100), -- which AI module can use this rule
    is_active BOOLEAN DEFAULT true,
    auto_apply BOOLEAN DEFAULT false, -- whether to auto-apply when confidence > threshold
    confidence_threshold DECIMAL(5,2) DEFAULT 80.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for data_quality_rules
CREATE INDEX IF NOT EXISTS idx_rule_type ON public.data_quality_rules (rule_type);
CREATE INDEX IF NOT EXISTS idx_dataset_pattern ON public.data_quality_rules (dataset_pattern);
CREATE INDEX IF NOT EXISTS idx_auto_apply ON public.data_quality_rules (auto_apply, is_active);



-- File upload queue - triggers Airflow DAGs
CREATE TABLE IF NOT EXISTS public.upload_queue (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(500) NOT NULL,
    file_path VARCHAR(1000) NOT NULL,
    file_type VARCHAR(50),
    file_size_bytes BIGINT,
    uploaded_by VARCHAR(100),
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_status VARCHAR(50) DEFAULT 'queued', -- queued, processing, completed, failed
    airflow_dag_triggered BOOLEAN DEFAULT FALSE,
    airflow_run_id VARCHAR(255),
    error_message TEXT,
    processed_at TIMESTAMP
);

-- Create indexes for upload_queue
CREATE INDEX IF NOT EXISTS idx_processing_status_uq ON public.upload_queue (processing_status);
CREATE INDEX IF NOT EXISTS idx_upload_timestamp_uq ON public.upload_queue (upload_timestamp DESC);



-- Dashboard view for datasets
CREATE OR REPLACE VIEW public.dataset_dashboard AS
SELECT 
    d.id,
    d.dataset_name,
    d.original_filename,
    d.file_type,
    d.row_count,
    d.column_count,
    d.upload_timestamp,
    d.processing_status,
    -- AI suggestions summary
    COUNT(s.id) as total_suggestions,
    COUNT(CASE WHEN s.status = 'pending' THEN 1 END) as pending_suggestions,
    COUNT(CASE WHEN s.status = 'accepted' THEN 1 END) as accepted_suggestions,
    COUNT(CASE WHEN s.severity = 'critical' THEN 1 END) as critical_issues,
    ROUND(AVG(s.confidence_score), 2) as avg_confidence,
    -- Processing jobs summary  
    COUNT(CASE WHEN pj.status = 'completed' THEN 1 END) as completed_jobs,
    COUNT(CASE WHEN pj.status = 'failed' THEN 1 END) as failed_jobs,
    MAX(pj.completed_at) as last_processing_time
FROM public.datasets d
LEFT JOIN public.ai_suggestions s ON d.id = s.dataset_id
LEFT JOIN public.processing_jobs pj ON d.id = pj.dataset_id
GROUP BY d.id, d.dataset_name, d.original_filename, d.file_type, 
         d.row_count, d.column_count, d.upload_timestamp, d.processing_status
ORDER BY d.upload_timestamp DESC;



-- Function to generate table name from dataset name
CREATE OR REPLACE FUNCTION generate_table_name(dataset_name VARCHAR)
RETURNS VARCHAR AS $$
BEGIN
    -- Create safe table name: lowercase, replace special chars with underscores
    RETURN 'data_' || regexp_replace(lower(dataset_name), '[^a-z0-9]', '_', 'g');
END;
$$ LANGUAGE plpgsql;

-- Function to trigger Airflow DAG (placeholder - actual implementation would use API)
CREATE OR REPLACE FUNCTION trigger_airflow_processing(dataset_id INTEGER)
RETURNS BOOLEAN AS $$
DECLARE
    dataset_record RECORD;
BEGIN
    SELECT * INTO dataset_record FROM public.datasets WHERE id = dataset_id;
    
    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;
    
    -- Insert into upload queue to trigger processing
    INSERT INTO public.upload_queue (
        filename, file_path, file_type, file_size_bytes, processing_status
    ) VALUES (
        dataset_record.original_filename,
        dataset_record.file_path, 
        dataset_record.file_type,
        dataset_record.file_size_bytes,
        'queued'
    );
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;



-- Auto-update timestamps
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ai_suggestions_update_timestamp
    BEFORE UPDATE ON public.ai_suggestions
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER data_quality_rules_update_timestamp  
    BEFORE UPDATE ON public.data_quality_rules
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();



-- Grant table and sequence privileges
GRANT ALL ON ALL TABLES IN SCHEMA public TO datauser;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO datauser;

-- Grant function execution privileges  
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO datauser;