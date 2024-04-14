#!/usr/bin/env python3
"""
GitHub Data Engineering Pipeline DAG
Main DAG orchestrating the complete data pipeline from landing to silver layer
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

# Import custom operators
from dags.operators.github_ingestion_operator import GitHubIngestionOperator
from dags.operators.bronze_processing_operator import BronzeProcessingOperator
from dags.operators.silver_processing_operator import SilverProcessingOperator

# Import utilities
from dags.utils.helpers import get_default_args, check_prerequisites, get_github_token, get_airflow_config

# Default arguments for the DAG
default_args = get_default_args()

# DAG configuration
dag_config = get_airflow_config()

# Create the DAG
dag = DAG(
    'github_data_pipeline',
    default_args=default_args,
    description=dag_config['description'],
    schedule_interval=timedelta(days=1),  # Run daily
    max_active_runs=dag_config['max_active_runs'],
    catchup=dag_config['catchup'],
    tags=dag_config['tags']
)

# Task 1: Prerequisites Check
def check_prerequisites_task():
    """Check if all prerequisites are met"""
    if not check_prerequisites():
        raise Exception("Prerequisites check failed")
    return "Prerequisites check passed"

prerequisites_check = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites_task,
    dag=dag
)

# Task 2: GitHub API Data Ingestion
github_ingestion = GitHubIngestionOperator(
    task_id='github_api_ingestion',
    github_token=get_github_token(),
    query="language:python stars:>1000",
    per_page=50,
    top_repos_count=10,
    output_dir="landing",
    dag=dag
)

# Task 3: Bronze Layer Processing
bronze_processing = BronzeProcessingOperator(
    task_id='bronze_layer_processing',
    landing_dir="landing",
    dag=dag
)

# Task 4: Silver Layer Processing
silver_processing = SilverProcessingOperator(
    task_id='silver_layer_processing',
    sql_dir="bronze_to_silver",
    dag=dag
)

# Task 5: Data Quality Check
def data_quality_check():
    """Perform basic data quality checks"""
    import os
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Check if bronze layer files exist
    bronze_files = ['bronze/repositories', 'bronze/commits', 'bronze/issues']
    for file_path in bronze_files:
        if not os.path.exists(file_path):
            raise Exception(f"Bronze layer file not found: {file_path}")
        logger.info(f" Bronze layer file exists: {file_path}")
    
    # Check if silver layer files exist
    silver_files = ['silver/dim_repositories', 'silver/dim_users', 'silver/dim_repo_permissions', 'silver/fact_commits', 'silver/fact_issues', 'silver/fact_pull_requests']
    for file_path in silver_files:
        if not os.path.exists(file_path):
            raise Exception(f"Silver layer file not found: {file_path}")
        logger.info(f" Silver layer file exists: {file_path}")
    
    logger.info(" All data quality checks passed")
    return "Data quality check completed successfully"

data_quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)

# Task 6: Pipeline Summary
def pipeline_summary():
    """Generate pipeline execution summary"""
    import os
    import logging
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Count files in each layer
    landing_count = len([f for f in os.listdir('landing') if f.endswith('.json')]) if os.path.exists('landing') else 0
    bronze_count = len([f for f in os.listdir('bronze') if os.path.isdir(os.path.join('bronze', f))]) if os.path.exists('bronze') else 0
    silver_count = len([f for f in os.listdir('silver') if os.path.isdir(os.path.join('silver', f))]) if os.path.exists('silver') else 0
    
    logger.info(f"Landing layer files: {landing_count}")
    logger.info(f"Bronze layer tables: {bronze_count}")
    logger.info(f"Silver layer tables: {silver_count}")
    logger.info("=" * 60)
    
    return f"Pipeline completed successfully - Landing: {landing_count}, Bronze: {bronze_count}, Silver: {silver_count}"

pipeline_summary_task = PythonOperator(
    task_id='pipeline_summary',
    python_callable=pipeline_summary,
    dag=dag
)

# Task 7: Cleanup (Optional)
def cleanup_temp_files():
    """Clean up temporary files if needed"""
    import os
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Clean up any temporary files
    temp_files = ['spark-warehouse', 'metastore_db']
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            if os.path.isdir(temp_file):
                import shutil
                shutil.rmtree(temp_file)
                logger.info(f"Cleaned up directory: {temp_file}")
            else:
                os.remove(temp_file)
                logger.info(f"Cleaned up file: {temp_file}")
    
    logger.info("Cleanup completed")
    return "Cleanup completed"

cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag,
    trigger_rule='all_done'  # Run even if previous tasks failed
)

# Define task dependencies
prerequisites_check >> github_ingestion >> bronze_processing >> silver_processing >> data_quality_check_task >> pipeline_summary_task >> cleanup_task
