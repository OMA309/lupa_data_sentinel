# # """
# # Lupa - File Processing DAG
# # Monitors staging directory, processes files, and moves to archive
# # """

# # from datetime import datetime, timedelta
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from airflow.operators.bash import BashOperator
# # from airflow.providers.postgres.hooks.postgres import PostgresHook
# # import pandas as pd
# # import os
# # import shutil
# # import logging

# # # DAG Configuration
# # default_args = {
# #     'owner': 'lupa',
# #     'depends_on_past': False,
# #     'start_date': datetime(2024, 1, 1),
# #     'email_on_failure': False,
# #     'email_on_retry': False,
# #     'retries': 1,
# #     'retry_delay': timedelta(minutes=5),
# # }

# # dag = DAG(
# #     'data_sentinel_pipeline',
# #     default_args=default_args,
# #     description='Lupa File Processing Pipeline',
# #     schedule_interval=None,  # Triggered by Streamlit app
# #     catchup=False,
# #     max_active_runs=1,
# #     tags=['lupa', 'data-processing', 'ai']
# # )

# # # Configuration
# # STAGING_DIR = '/opt/airflow/staging'
# # ARCHIVE_DIR = '/opt/airflow/archive'
# # POSTGRES_CONN_ID = 'data_sentinel_db'

# # class LupaProcessor:
# #     """File processor for Lupa system"""
    
# #     def __init__(self):
# #         self.pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
# #         self.logger = logging.getLogger(__name__)

# #     def process_file(self, **context):
# #         """Process uploaded file"""
# #         # Get file info from DAG run configuration
# #         dag_config = context['dag_run'].conf or {}
# #         filename = dag_config.get('filename')
# #         file_path = dag_config.get('file_path')
# #         dataset_name = dag_config.get('dataset_name')
        
# #         if not filename or not file_path:
# #             raise ValueError("Missing filename or file_path in DAG configuration")
        
# #         self.logger.info(f"Processing file: {filename}")
        
# #         # Read the CSV file
# #         df = pd.read_csv(file_path)
        
# #         # Create dataset record
# #         dataset_sql = """
# #         INSERT INTO public.datasets (
# #             dataset_name, original_filename, file_type, file_size_bytes, 
# #             table_name, row_count, column_count, processing_status, file_path
# #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'processing', %s)
# #         ON CONFLICT (dataset_name) 
# #         DO UPDATE SET 
# #             original_filename = EXCLUDED.original_filename,
# #             file_size_bytes = EXCLUDED.file_size_bytes,
# #             row_count = EXCLUDED.row_count,
# #             column_count = EXCLUDED.column_count,
# #             processing_status = 'processing',
# #             last_processed = CURRENT_TIMESTAMP
# #         RETURNING id;
# #         """
        
# #         file_size = os.path.getsize(file_path)
# #         table_name = f"data_{dataset_name.lower().replace('-', '_').replace(' ', '_')}"
        
# #         result = self.pg_hook.get_first(dataset_sql, parameters=[
# #             dataset_name, filename, 'csv', file_size, 
# #             table_name, len(df), len(df.columns), file_path
# #         ])
        
# #         dataset_id = result[0]
        
# #         # Store for next tasks
# #         context['task_instance'].xcom_push(key='dataset_id', value=dataset_id)
# #         context['task_instance'].xcom_push(key='table_name', value=table_name)
# #         context['task_instance'].xcom_push(key='dataframe_info', value={
# #             'columns': df.columns.tolist(),
# #             'dtypes': df.dtypes.astype(str).to_dict()
# #         })
        
# #         return f"Processed dataset {dataset_name} with ID {dataset_id}"

# #     def create_dynamic_table(self, **context):
# #         """Create dynamic table based on data structure"""
# #         dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
# #         table_name = context['task_instance'].xcom_pull(key='table_name')
        
# #         # Get file info
# #         dag_config = context['dag_run'].conf or {}
# #         file_path = dag_config.get('file_path')
        
# #         # Analyze data structure
# #         df_sample = pd.read_csv(file_path, nrows=100)
        
# #         # Generate CREATE TABLE SQL
# #         columns_sql = []
# #         for col in df_sample.columns:
# #             # Simple type detection
# #             if df_sample[col].dtype == 'int64':
# #                 col_type = 'INTEGER'
# #             elif df_sample[col].dtype == 'float64':
# #                 col_type = 'DECIMAL(15,5)'
# #             elif df_sample[col].dtype == 'bool':
# #                 col_type = 'BOOLEAN'
# #             else:
# #                 max_len = df_sample[col].astype(str).str.len().max()
# #                 if pd.isna(max_len) or max_len <= 255:
# #                     col_type = 'VARCHAR(255)'
# #                 else:
# #                     col_type = 'TEXT'
            
# #             columns_sql.append(f'    "{col}" {col_type}')
        
# #         create_table_sql = f"""
# #         CREATE TABLE IF NOT EXISTS public."{table_name}" (
# #             id SERIAL PRIMARY KEY,
# # {','.join(columns_sql)}
# #         );
# #         """
        
# #         # Create table
# #         self.pg_hook.run(create_table_sql)
# #         self.logger.info(f"Created table: {table_name}")
        
# #         # Store schema info
# #         for i, col in enumerate(df_sample.columns):
# #             schema_sql = """
# #             INSERT INTO public.table_schemas 
# #             (dataset_id, column_name, column_position, data_type)
# #             VALUES (%s, %s, %s, %s)
# #             ON CONFLICT (dataset_id, column_name) DO UPDATE SET
# #                 data_type = EXCLUDED.data_type;
# #             """
            
# #             col_type = 'TEXT'  # Simplified for now
# #             self.pg_hook.run(schema_sql, parameters=[dataset_id, col, i+1, col_type])
        
# #         return f"Table {table_name} created successfully"

# #     def load_data_to_table(self, **context):
# #         """Load data into the dynamic table"""
# #         dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
# #         table_name = context['task_instance'].xcom_pull(key='table_name')
        
# #         # Get file path
# #         dag_config = context['dag_run'].conf or {}
# #         file_path = dag_config.get('file_path')
        
# #         # Load data
# #         df = pd.read_csv(file_path)
        
# #         # Clear existing data
# #         self.pg_hook.run(f'TRUNCATE TABLE public."{table_name}";')
        
# #         # Insert data in batches
# #         batch_size = 1000
# #         total_rows = len(df)
        
# #         for i in range(0, total_rows, batch_size):
# #             batch = df.iloc[i:i+batch_size]
            
# #             # Prepare insert values
# #             values = []
# #             for _, row in batch.iterrows():
# #                 row_values = []
# #                 for val in row:
# #                     if pd.isna(val):
# #                         row_values.append(None)
# #                     else:
# #                         row_values.append(str(val))  # Convert all to string for simplicity
# #                 values.append(tuple(row_values))
            
# #             # Insert batch
# #             columns = '", "'.join(df.columns)
# #             placeholders = ', '.join(['%s'] * len(df.columns))
# #             insert_sql = f'INSERT INTO public."{table_name}" ("{columns}") VALUES ({placeholders})'
            
# #             self.pg_hook.run(insert_sql, parameters=values)
# #             self.logger.info(f"Inserted batch {i//batch_size + 1}")
        
# #         # Update dataset status
# #         update_sql = """
# #         UPDATE public.datasets 
# #         SET processing_status = 'completed', last_processed = CURRENT_TIMESTAMP
# #         WHERE id = %s;
# #         """
# #         self.pg_hook.run(update_sql, parameters=[dataset_id])
        
# #         return f"Loaded {total_rows} rows into {table_name}"

# #     def generate_ai_suggestions(self, **context):
# #         """Generate basic AI suggestions"""
# #         dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
# #         table_name = context['task_instance'].xcom_pull(key='table_name')
        
# #         # Get dataset info
# #         dataset_sql = "SELECT dataset_name FROM public.datasets WHERE id = %s;"
# #         dataset_name = self.pg_hook.get_first(dataset_sql, parameters=[dataset_id])[0]
        
# #         # Get table columns
# #         columns_sql = f"""
# #         SELECT column_name FROM information_schema.columns 
# #         WHERE table_name = '{table_name}' AND table_schema = 'public'
# #         AND column_name != 'id'
# #         ORDER BY ordinal_position;
# #         """
# #         columns = self.pg_hook.get_records(columns_sql)
        
# #         suggestions = []
        
# #         for (col_name,) in columns:
# #             # Check for null values
# #             null_check_sql = f"""
# #             SELECT 
# #                 COUNT(*) as total_count,
# #                 COUNT("{col_name}") as non_null_count
# #             FROM public."{table_name}";
# #             """
            
# #             result = self.pg_hook.get_first(null_check_sql)
# #             total_count, non_null_count = result
# #             null_count = total_count - non_null_count
            
# #             if null_count > 0:
# #                 null_percentage = (null_count / total_count) * 100
                
# #                 suggestion_sql = """
# #                 INSERT INTO public.ai_suggestions 
# #                 (dataset_id, dataset, column_name, suggestion, ai_module, issue_type, 
# #                  confidence_score, severity, affected_rows, status)
# #                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending');
# #                 """
                
# #                 severity = 'high' if null_percentage > 20 else 'medium'
# #                 confidence = min(95, 60 + null_percentage)
                
# #                 suggestion_text = f"Handle missing values in {col_name} column ({null_percentage:.1f}% missing)"
                
# #                 self.pg_hook.run(suggestion_sql, parameters=[
# #                     dataset_id, dataset_name, col_name, suggestion_text,
# #                     'Missing Values', 'Missing Data', confidence, severity, null_count
# #                 ])
                
# #                 suggestions.append(col_name)
        
# #         return f"Generated {len(suggestions)} AI suggestions"

# #     def move_to_archive(self, **context):
# #         """Move processed file to archive"""
# #         dag_config = context['dag_run'].conf or {}
# #         file_path = dag_config.get('file_path')
# #         filename = dag_config.get('filename')
        
# #         if os.path.exists(file_path):
# #             # Create archive path
# #             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# #             archive_filename = f"{timestamp}_{filename}"
# #             archive_path = os.path.join(ARCHIVE_DIR, archive_filename)
            
# #             # Move file
# #             shutil.move(file_path, archive_path)
# #             self.logger.info(f"Moved {file_path} to {archive_path}")
            
# #             return f"File archived to {archive_path}"
# #         else:
# #             self.logger.warning(f"File not found: {file_path}")
# #             return "File not found for archiving"

# # # Create processor instance
# # processor = LupaProcessor()

# # # Task 1: Process File
# # process_file_task = PythonOperator(
# #     task_id='process_file',
# #     python_callable=processor.process_file,
# #     dag=dag,
# # )

# # # Task 2: Create Dynamic Table
# # create_table_task = PythonOperator(
# #     task_id='create_dynamic_table',
# #     python_callable=processor.create_dynamic_table,
# #     dag=dag,
# # )

# # # Task 3: Load Data
# # load_data_task = PythonOperator(
# #     task_id='load_data_to_table',
# #     python_callable=processor.load_data_to_table,
# #     dag=dag,
# # )

# # # Task 4: Generate AI Suggestions
# # generate_suggestions_task = PythonOperator(
# #     task_id='generate_ai_suggestions',
# #     python_callable=processor.generate_ai_suggestions,
# #     dag=dag,
# # )

# # # Task 5: Move to Archive
# # archive_task = PythonOperator(
# #     task_id='move_to_archive',
# #     python_callable=processor.move_to_archive,
# #     dag=dag,
# # )

# # # Define task dependencies
# # process_file_task >> create_table_task >> load_data_task >> generate_suggestions_task >> archive_task



# """
# Lupa - File Processing DAG
# Monitors staging directory, processes files, and moves to archive
# """

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import pandas as pd
# import os
# import shutil
# import logging

# # DAG Configuration
# default_args = {
#     'owner': 'lupa',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 9, 19),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'data_sentinel_pipeline',
#     default_args=default_args,
#     description='Lupa File Processing Pipeline',
#     schedule_interval=None,  # Triggered by Streamlit app
#     catchup=False,
#     max_active_runs=1,
#     tags=['lupa', 'data-processing', 'ai']
# )

# # Configuration
# STAGING_DIR = '/opt/airflow/staging'
# ARCHIVE_DIR = '/opt/airflow/archive'
# POSTGRES_CONN_ID = 'data_sentinel_db'

# # Schema SQL - You can also read this from a file
# SCHEMA_SQL = """
# -- Track all datasets and their metadata
# CREATE TABLE IF NOT EXISTS public.datasets (
#     id SERIAL PRIMARY KEY,
#     dataset_name VARCHAR(255) UNIQUE NOT NULL,
#     original_filename VARCHAR(500),
#     file_type VARCHAR(50), -- csv, json, xlsx, pdf, etc.
#     file_size_bytes BIGINT,
#     table_name VARCHAR(255), -- dynamically created table name
#     row_count INTEGER,
#     column_count INTEGER,
#     upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     last_processed TIMESTAMP,
#     processing_status VARCHAR(50) DEFAULT 'uploaded', -- uploaded, processing, completed, failed
#     schema_hash VARCHAR(64), -- hash of column structure for change detection
#     created_by VARCHAR(100),
#     file_path VARCHAR(1000) -- path where original file is stored
# );

# -- Create indexes if they don't exist
# DO $$ 
# BEGIN
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dataset_name') THEN
#         CREATE INDEX idx_dataset_name ON public.datasets (dataset_name);
#     END IF;
    
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_processing_status') THEN
#         CREATE INDEX idx_processing_status ON public.datasets (processing_status);
#     END IF;
    
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_upload_timestamp') THEN
#         CREATE INDEX idx_upload_timestamp ON public.datasets (upload_timestamp DESC);
#     END IF;
# END $$;

# -- Track dynamic table schemas
# CREATE TABLE IF NOT EXISTS public.table_schemas (
#     id SERIAL PRIMARY KEY,
#     dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
#     column_name VARCHAR(255) NOT NULL,
#     column_position INTEGER,
#     data_type VARCHAR(100), -- detected PostgreSQL data type
#     original_type VARCHAR(100), -- original type from source
#     is_nullable BOOLEAN DEFAULT TRUE,
#     max_length INTEGER,
#     precision_scale VARCHAR(20), -- for numeric types
#     sample_values TEXT[], -- sample values for reference
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
#     UNIQUE(dataset_id, column_name)
# );

# -- Create indexes for table_schemas
# DO $$ 
# BEGIN
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dataset_column') THEN
#         CREATE INDEX idx_dataset_column ON public.table_schemas (dataset_id, column_name);
#     END IF;
# END $$;

# -- AI-generated suggestions
# CREATE TABLE IF NOT EXISTS public.ai_suggestions (
#     id SERIAL PRIMARY KEY,
#     dataset_id INTEGER REFERENCES public.datasets(id) ON DELETE CASCADE,
#     dataset VARCHAR(255) NOT NULL, -- kept for backward compatibility
#     column_name VARCHAR(255) NOT NULL,
#     suggestion TEXT NOT NULL,
#     status VARCHAR(50) DEFAULT 'pending', -- pending, accepted, rejected, applied
#     ai_module VARCHAR(100), -- Profiling, Anomaly Detection, etc.
#     issue_type VARCHAR(100), -- Missing Values, Duplicates, Format Error, etc.
#     confidence_score DECIMAL(5,2), -- 0-100
#     severity VARCHAR(20), -- low, medium, high, critical
#     affected_rows INTEGER,
#     sample_problematic_values TEXT[],
#     suggested_sql TEXT, -- SQL to fix the issue
#     user_feedback TEXT,
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     applied_at TIMESTAMP -- when the fix was actually applied
# );

# -- Create indexes for ai_suggestions
# DO $$ 
# BEGIN
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dataset_status') THEN
#         CREATE INDEX idx_dataset_status ON public.ai_suggestions (dataset, status);
#     END IF;
    
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dataset_id_status') THEN
#         CREATE INDEX idx_dataset_id_status ON public.ai_suggestions (dataset_id, status);
#     END IF;
    
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_ai_module') THEN
#         CREATE INDEX idx_ai_module ON public.ai_suggestions (ai_module);
#     END IF;
    
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_confidence_score') THEN
#         CREATE INDEX idx_confidence_score ON public.ai_suggestions (confidence_score DESC);
#     END IF;
    
#     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_severity') THEN
#         CREATE INDEX idx_severity ON public.ai_suggestions (severity);
#     END IF;
# END $$;

# -- Auto-update timestamps function
# CREATE OR REPLACE FUNCTION update_timestamp()
# RETURNS TRIGGER AS $$
# BEGIN
#     NEW.updated_at = CURRENT_TIMESTAMP;
#     RETURN NEW;
# END;
# $$ LANGUAGE plpgsql;

# -- Create trigger if it doesn't exist
# DO $$ 
# BEGIN
#     IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'ai_suggestions_update_timestamp') THEN
#         CREATE TRIGGER ai_suggestions_update_timestamp
#             BEFORE UPDATE ON public.ai_suggestions
#             FOR EACH ROW EXECUTE FUNCTION update_timestamp();
#     END IF;
# END $$;
# """

# class LupaProcessor:
#     """File processor for Lupa system"""
    
#     def __init__(self):
#         self.pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#         self.logger = logging.getLogger(__name__)

#     def process_file(self, **context):
#         """Process uploaded file"""
#         # Get file info from DAG run configuration
#         dag_config = context['dag_run'].conf or {}
#         filename = dag_config.get('filename')
#         file_path = dag_config.get('file_path')
#         dataset_name = dag_config.get('dataset_name')
        
#         if not filename or not file_path:
#             raise ValueError("Missing filename or file_path in DAG configuration")
        
#         self.logger.info(f"Processing file: {filename}")
        
#         # Read the CSV file
#         df = pd.read_csv(file_path)
        
#         # Create dataset record
#         dataset_sql = """
#         INSERT INTO public.datasets (
#             dataset_name, original_filename, file_type, file_size_bytes, 
#             table_name, row_count, column_count, processing_status, file_path
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'processing', %s)
#         ON CONFLICT (dataset_name) 
#         DO UPDATE SET 
#             original_filename = EXCLUDED.original_filename,
#             file_size_bytes = EXCLUDED.file_size_bytes,
#             row_count = EXCLUDED.row_count,
#             column_count = EXCLUDED.column_count,
#             processing_status = 'processing',
#             last_processed = CURRENT_TIMESTAMP
#         RETURNING id;
#         """
        
#         file_size = os.path.getsize(file_path)
#         table_name = f"data_{dataset_name.lower().replace('-', '_').replace(' ', '_')}"
        
#         result = self.pg_hook.get_first(dataset_sql, parameters=[
#             dataset_name, filename, 'csv', file_size, 
#             table_name, len(df), len(df.columns), file_path
#         ])
        
#         dataset_id = result[0]
        
#         # Store for next tasks
#         context['task_instance'].xcom_push(key='dataset_id', value=dataset_id)
#         context['task_instance'].xcom_push(key='table_name', value=table_name)
#         context['task_instance'].xcom_push(key='dataframe_info', value={
#             'columns': df.columns.tolist(),
#             'dtypes': df.dtypes.astype(str).to_dict()
#         })
        
#         return f"Processed dataset {dataset_name} with ID {dataset_id}"

#     def create_dynamic_table(self, **context):
#         """Create dynamic table based on data structure"""
#         dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
#         table_name = context['task_instance'].xcom_pull(key='table_name')
        
#         # Get file info
#         dag_config = context['dag_run'].conf or {}
#         file_path = dag_config.get('file_path')
        
#         # Analyze data structure
#         df_sample = pd.read_csv(file_path, nrows=100)
        
#         # Generate CREATE TABLE SQL
#         columns_sql = []
#         for col in df_sample.columns:
#             # Simple type detection
#             if df_sample[col].dtype == 'int64':
#                 col_type = 'INTEGER'
#             elif df_sample[col].dtype == 'float64':
#                 col_type = 'DECIMAL(15,5)'
#             elif df_sample[col].dtype == 'bool':
#                 col_type = 'BOOLEAN'
#             else:
#                 max_len = df_sample[col].astype(str).str.len().max()
#                 if pd.isna(max_len) or max_len <= 255:
#                     col_type = 'VARCHAR(255)'
#                 else:
#                     col_type = 'TEXT'
            
#             columns_sql.append(f'    "{col}" {col_type}')
        
#         create_table_sql = f"""
#         CREATE TABLE IF NOT EXISTS public."{table_name}" (
#             id SERIAL PRIMARY KEY,
# {','.join(columns_sql)}
#         );
#         """
        
#         # Create table
#         self.pg_hook.run(create_table_sql)
#         self.logger.info(f"Created table: {table_name}")
        
#         # Store schema info
#         for i, col in enumerate(df_sample.columns):
#             schema_sql = """
#             INSERT INTO public.table_schemas 
#             (dataset_id, column_name, column_position, data_type)
#             VALUES (%s, %s, %s, %s)
#             ON CONFLICT (dataset_id, column_name) DO UPDATE SET
#                 data_type = EXCLUDED.data_type;
#             """
            
#             col_type = 'TEXT'  # Simplified for now
#             self.pg_hook.run(schema_sql, parameters=[dataset_id, col, i+1, col_type])
        
#         return f"Table {table_name} created successfully"

#     def load_data_to_table(self, **context):
#         """Load data into the dynamic table"""
#         dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
#         table_name = context['task_instance'].xcom_pull(key='table_name')
        
#         # Get file path
#         dag_config = context['dag_run'].conf or {}
#         file_path = dag_config.get('file_path')
        
#         # Load data
#         df = pd.read_csv(file_path)
        
#         # Clear existing data
#         self.pg_hook.run(f'TRUNCATE TABLE public."{table_name}";')
        
#         # Insert data in batches
#         batch_size = 1000
#         total_rows = len(df)
        
#         for i in range(0, total_rows, batch_size):
#             batch = df.iloc[i:i+batch_size]
            
#             # Prepare insert values
#             values = []
#             for _, row in batch.iterrows():
#                 row_values = []
#                 for val in row:
#                     if pd.isna(val):
#                         row_values.append(None)
#                     else:
#                         row_values.append(str(val))  # Convert all to string for simplicity
#                 values.append(tuple(row_values))
            
#             # Insert batch
#             columns = '", "'.join(df.columns)
#             placeholders = ', '.join(['%s'] * len(df.columns))
#             insert_sql = f'INSERT INTO public."{table_name}" ("{columns}") VALUES ({placeholders})'
            
#             self.pg_hook.run(insert_sql, parameters=values)
#             self.logger.info(f"Inserted batch {i//batch_size + 1}")
        
#         # Update dataset status
#         update_sql = """
#         UPDATE public.datasets 
#         SET processing_status = 'completed', last_processed = CURRENT_TIMESTAMP
#         WHERE id = %s;
#         """
#         self.pg_hook.run(update_sql, parameters=[dataset_id])
        
#         return f"Loaded {total_rows} rows into {table_name}"

#     def generate_ai_suggestions(self, **context):
#         """Generate basic AI suggestions"""
#         dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
#         table_name = context['task_instance'].xcom_pull(key='table_name')
        
#         # Get dataset info
#         dataset_sql = "SELECT dataset_name FROM public.datasets WHERE id = %s;"
#         dataset_name = self.pg_hook.get_first(dataset_sql, parameters=[dataset_id])[0]
        
#         # Get table columns
#         columns_sql = f"""
#         SELECT column_name FROM information_schema.columns 
#         WHERE table_name = '{table_name}' AND table_schema = 'public'
#         AND column_name != 'id'
#         ORDER BY ordinal_position;
#         """
#         columns = self.pg_hook.get_records(columns_sql)
        
#         suggestions = []
        
#         for (col_name,) in columns:
#             # Check for null values
#             null_check_sql = f"""
#             SELECT 
#                 COUNT(*) as total_count,
#                 COUNT("{col_name}") as non_null_count
#             FROM public."{table_name}";
#             """
            
#             result = self.pg_hook.get_first(null_check_sql)
#             total_count, non_null_count = result
#             null_count = total_count - non_null_count
            
#             if null_count > 0:
#                 null_percentage = (null_count / total_count) * 100
                
#                 suggestion_sql = """
#                 INSERT INTO public.ai_suggestions 
#                 (dataset_id, dataset, column_name, suggestion, ai_module, issue_type, 
#                  confidence_score, severity, affected_rows, status)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending');
#                 """
                
#                 severity = 'high' if null_percentage > 20 else 'medium'
#                 confidence = min(95, 60 + null_percentage)
                
#                 suggestion_text = f"Handle missing values in {col_name} column ({null_percentage:.1f}% missing)"
                
#                 self.pg_hook.run(suggestion_sql, parameters=[
#                     dataset_id, dataset_name, col_name, suggestion_text,
#                     'Missing Values', 'Missing Data', confidence, severity, null_count
#                 ])
                
#                 suggestions.append(col_name)
        
#         return f"Generated {len(suggestions)} AI suggestions"

#     def move_to_archive(self, **context):
#         """Move processed file to archive"""
#         dag_config = context['dag_run'].conf or {}
#         file_path = dag_config.get('file_path')
#         filename = dag_config.get('filename')
        
#         if os.path.exists(file_path):
#             # Create archive path
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             archive_filename = f"{timestamp}_{filename}"
#             archive_path = os.path.join(ARCHIVE_DIR, archive_filename)
            
#             # Move file
#             shutil.move(file_path, archive_path)
#             self.logger.info(f"Moved {file_path} to {archive_path}")
            
#             return f"File archived to {archive_path}"
#         else:
#             self.logger.warning(f"File not found: {file_path}")
#             return "File not found for archiving"

# # Create processor instance
# processor = LupaProcessor()

# # Task 0: Initialize Database Schema (NEW TASK)
# init_schema_task = PostgresOperator(
#     task_id='init_schema',
#     postgres_conn_id=POSTGRES_CONN_ID,
#     sql=SCHEMA_SQL,
#     dag=dag,
#     doc_md="""
#     ## Initialize Database Schema
    
#     Creates all required tables for the Lupa data processing system:
#     - datasets: Track uploaded files and processing status
#     - table_schemas: Store dynamic table schema information
#     - ai_suggestions: Store AI-generated data quality suggestions
    
#     Uses CREATE TABLE IF NOT EXISTS to avoid conflicts on repeated runs.
#     """
# )

# # Task 1: Process File
# process_file_task = PythonOperator(
#     task_id='process_file',
#     python_callable=processor.process_file,
#     dag=dag,
# )

# # Task 2: Create Dynamic Table
# create_table_task = PythonOperator(
#     task_id='create_dynamic_table',
#     python_callable=processor.create_dynamic_table,
#     dag=dag,
# )

# # Task 3: Load Data
# load_data_task = PythonOperator(
#     task_id='load_data_to_table',
#     python_callable=processor.load_data_to_table,
#     dag=dag,
# )

# # Task 4: Generate AI Suggestions
# generate_suggestions_task = PythonOperator(
#     task_id='generate_ai_suggestions',
#     python_callable=processor.generate_ai_suggestions,
#     dag=dag,
# )

# # Task 5: Move to Archive
# archive_task = PythonOperator(
#     task_id='move_to_archive',
#     python_callable=processor.move_to_archive,
#     dag=dag,
# )

# # Define task dependencies (UPDATED TO INCLUDE SCHEMA INITIALIZATION)
# init_schema_task >> process_file_task >> create_table_task >> load_data_task >> generate_suggestions_task >> archive_task



"""
Lupa - File Processing DAG
Monitors staging directory, processes files, and moves to archive
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import shutil
import logging

# DAG Configuration
default_args = {
    'owner': 'lupa',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_sentinel_pipeline',
    default_args=default_args,
    description='Lupa File Processing Pipeline',
    schedule_interval=None,  # Triggered by Streamlit app
    catchup=False,
    max_active_runs=1,
    tags=['lupa', 'data-processing', 'ai']
)

# Configuration
STAGING_DIR = '/opt/airflow/staging'
ARCHIVE_DIR = '/opt/airflow/archive'
POSTGRES_CONN_ID = 'data_sentinel_db'

# Function to read schema from your existing file
def read_schema_file():
    """Read the schema SQL from your data_sentinel_schema.sql file"""
    # Your actual schema file path
    # schema_file_path = '~/BOTAFLI/Hackatthon/Nordiac/Project_root/sql/data_sentinel_schema.sql'
    schema_file_path = "/opt/project_root/sql/data_sentinel_schema.sql"
    "/opt/project_root/sql/data_sentinel_schema.sql"
    try:
        with open(schema_file_path, 'r') as f:
            schema_content = f.read()
        return schema_content
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found at {schema_file_path}")
    except Exception as e:
        raise Exception(f"Error reading schema file: {str(e)}")

class LupaProcessor:
    """File processor for Lupa system"""
    
    def __init__(self):
        self.pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        self.logger = logging.getLogger(__name__)

    def process_file(self, **context):
        """Process uploaded file"""
        # Get file info from DAG run configuration
        dag_config = context['dag_run'].conf or {}
        filename = dag_config.get('filename')
        file_path = dag_config.get('file_path')
        dataset_name = dag_config.get('dataset_name')
        
        if not filename or not file_path:
            raise ValueError("Missing filename or file_path in DAG configuration")
        
        self.logger.info(f"Processing file: {filename}")
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Create dataset record
        dataset_sql = """
        INSERT INTO public.datasets (
            dataset_name, original_filename, file_type, file_size_bytes, 
            table_name, row_count, column_count, processing_status, file_path
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'processing', %s)
        ON CONFLICT (dataset_name) 
        DO UPDATE SET 
            original_filename = EXCLUDED.original_filename,
            file_size_bytes = EXCLUDED.file_size_bytes,
            row_count = EXCLUDED.row_count,
            column_count = EXCLUDED.column_count,
            processing_status = 'processing',
            last_processed = CURRENT_TIMESTAMP
        RETURNING id;
        """
        
        file_size = os.path.getsize(file_path)
        table_name = f"data_{dataset_name.lower().replace('-', '_').replace(' ', '_')}"
        
        result = self.pg_hook.get_first(dataset_sql, parameters=[
            dataset_name, filename, 'csv', file_size, 
            table_name, len(df), len(df.columns), file_path
        ])
        
        dataset_id = result[0]
        
        # Store for next tasks
        context['task_instance'].xcom_push(key='dataset_id', value=dataset_id)
        context['task_instance'].xcom_push(key='table_name', value=table_name)
        context['task_instance'].xcom_push(key='dataframe_info', value={
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.astype(str).to_dict()
        })
        
        return f"Processed dataset {dataset_name} with ID {dataset_id}"

    def create_dynamic_table(self, **context):
        """Create dynamic table based on data structure"""
        dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
        table_name = context['task_instance'].xcom_pull(key='table_name')
        
        # Get file info
        dag_config = context['dag_run'].conf or {}
        file_path = dag_config.get('file_path')
        
        # Analyze data structure
        df_sample = pd.read_csv(file_path, nrows=100)
        
        # Generate CREATE TABLE SQL
        columns_sql = []
        for col in df_sample.columns:
            # Simple type detection
            if df_sample[col].dtype == 'int64':
                col_type = 'INTEGER'
            elif df_sample[col].dtype == 'float64':
                col_type = 'DECIMAL(15,5)'
            elif df_sample[col].dtype == 'bool':
                col_type = 'BOOLEAN'
            else:
                max_len = df_sample[col].astype(str).str.len().max()
                if pd.isna(max_len) or max_len <= 255:
                    col_type = 'VARCHAR(255)'
                else:
                    col_type = 'TEXT'
            
            columns_sql.append(f'    "{col}" {col_type}')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS public."{table_name}" (
            id SERIAL PRIMARY KEY,
{','.join(columns_sql)}
        );
        """
        
        # Create table
        self.pg_hook.run(create_table_sql)
        self.logger.info(f"Created table: {table_name}")
        
        # Store schema info
        for i, col in enumerate(df_sample.columns):
            schema_sql = """
            INSERT INTO public.table_schemas 
            (dataset_id, column_name, column_position, data_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (dataset_id, column_name) DO UPDATE SET
                data_type = EXCLUDED.data_type;
            """
            
            col_type = 'TEXT'  # Simplified for now
            self.pg_hook.run(schema_sql, parameters=[dataset_id, col, i+1, col_type])
        
        return f"Table {table_name} created successfully"

    def load_data_to_table(self, **context):
        """Load data into the dynamic table"""
        dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
        table_name = context['task_instance'].xcom_pull(key='table_name')
        
        # Get file path
        dag_config = context['dag_run'].conf or {}
        file_path = dag_config.get('file_path')
        
        # Load data
        df = pd.read_csv(file_path)
        
        # Clear existing data
        self.pg_hook.run(f'TRUNCATE TABLE public."{table_name}";')
        
        # Insert data in batches
        batch_size = 1000
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            
            # Prepare insert values
            values = []
            for _, row in batch.iterrows():
                row_values = []
                for val in row:
                    if pd.isna(val):
                        row_values.append(None)
                    else:
                        row_values.append(str(val))  # Convert all to string for simplicity
                values.append(tuple(row_values))
            
            # Insert batch
            columns = '", "'.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_sql = f'INSERT INTO public."{table_name}" ("{columns}") VALUES ({placeholders})'
            
            self.pg_hook.run(insert_sql, parameters=values)
            self.logger.info(f"Inserted batch {i//batch_size + 1}")
        
        # Update dataset status
        update_sql = """
        UPDATE public.datasets 
        SET processing_status = 'completed', last_processed = CURRENT_TIMESTAMP
        WHERE id = %s;
        """
        self.pg_hook.run(update_sql, parameters=[dataset_id])
        
        return f"Loaded {total_rows} rows into {table_name}"

    def generate_ai_suggestions(self, **context):
        """Generate basic AI suggestions"""
        dataset_id = context['task_instance'].xcom_pull(key='dataset_id')
        table_name = context['task_instance'].xcom_pull(key='table_name')
        
        # Get dataset info
        dataset_sql = "SELECT dataset_name FROM public.datasets WHERE id = %s;"
        dataset_name = self.pg_hook.get_first(dataset_sql, parameters=[dataset_id])[0]
        
        # Get table columns
        columns_sql = f"""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        AND column_name != 'id'
        ORDER BY ordinal_position;
        """
        columns = self.pg_hook.get_records(columns_sql)
        
        suggestions = []
        
        for (col_name,) in columns:
            # Check for null values
            null_check_sql = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT("{col_name}") as non_null_count
            FROM public."{table_name}";
            """
            
            result = self.pg_hook.get_first(null_check_sql)
            total_count, non_null_count = result
            null_count = total_count - non_null_count
            
            if null_count > 0:
                null_percentage = (null_count / total_count) * 100
                
                suggestion_sql = """
                INSERT INTO public.ai_suggestions 
                (dataset_id, dataset, column_name, suggestion, ai_module, issue_type, 
                 confidence_score, severity, affected_rows, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending');
                """
                
                severity = 'high' if null_percentage > 20 else 'medium'
                confidence = min(95, 60 + null_percentage)
                
                suggestion_text = f"Handle missing values in {col_name} column ({null_percentage:.1f}% missing)"
                
                self.pg_hook.run(suggestion_sql, parameters=[
                    dataset_id, dataset_name, col_name, suggestion_text,
                    'Missing Values', 'Missing Data', confidence, severity, null_count
                ])
                
                suggestions.append(col_name)
        
        return f"Generated {len(suggestions)} AI suggestions"

    def move_to_archive(self, **context):
        """Move processed file to archive"""
        dag_config = context['dag_run'].conf or {}
        file_path = dag_config.get('file_path')
        filename = dag_config.get('filename')
        
        if os.path.exists(file_path):
            # Create archive path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_filename = f"{timestamp}_{filename}"
            archive_path = os.path.join(ARCHIVE_DIR, archive_filename)
            
            # Move file
            shutil.move(file_path, archive_path)
            self.logger.info(f"Moved {file_path} to {archive_path}")
            
            return f"File archived to {archive_path}"
        else:
            self.logger.warning(f"File not found: {file_path}")
            return "File not found for archiving"

# Create processor instance
processor = LupaProcessor()

# Task 0: Initialize Database Schema using your existing SQL file
init_schema_task = PostgresOperator(
    task_id='init_schema',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=read_schema_file(),  # This reads your actual data_sentinel_schema.sql file
    dag=dag,
    doc_md="""
    ## Initialize Database Schema
    
    Runs your data_sentinel_schema.sql file to create all required tables:
    - Reads from /opt/airflow/sql/data_sentinel_schema.sql
    - Creates all tables, indexes, functions, triggers, and views
    - Uses your existing schema definitions exactly as written
    
    This ensures your DAG uses the same schema as your standalone SQL file.
    """
)

# Task 1: Process File
process_file_task = PythonOperator(
    task_id='process_file',
    python_callable=processor.process_file,
    dag=dag,
)

# Task 2: Create Dynamic Table
create_table_task = PythonOperator(
    task_id='create_dynamic_table',
    python_callable=processor.create_dynamic_table,
    dag=dag,
)

# Task 3: Load Data
load_data_task = PythonOperator(
    task_id='load_data_to_table',
    python_callable=processor.load_data_to_table,
    dag=dag,
)

# Task 4: Generate AI Suggestions
generate_suggestions_task = PythonOperator(
    task_id='generate_ai_suggestions',
    python_callable=processor.generate_ai_suggestions,
    dag=dag,
)

# Task 5: Move to Archive
archive_task = PythonOperator(
    task_id='move_to_archive',
    python_callable=processor.move_to_archive,
    dag=dag,
)

# Task dependencies - Schema from your file runs first
init_schema_task >> process_file_task >> create_table_task >> load_data_task >> generate_suggestions_task >> archive_task