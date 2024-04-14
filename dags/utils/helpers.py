#!/usr/bin/env python3
"""
Helper functions for Airflow DAGs
Common utilities used across different operators
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

logger = logging.getLogger(__name__)

def get_default_args() -> Dict[str, Any]:
    """Get default arguments for DAGs"""
    return {
        'owner': 'data-engineering-team',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

def check_prerequisites() -> bool:
    """Check if all prerequisites are met"""
    logger.info("Checking prerequisites...")
    
    # Check if required directories exist
    required_dirs = ["landing", "bronze", "silver", "bronze_to_silver"]
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            logger.error(f"Required directory not found: {dir_name}")
            return False
    
    # Check if required SQL files exist
    required_sql_files = [
        "bronze_to_silver/dim_repositories.sql",
        "bronze_to_silver/dim_users.sql",
        "bronze_to_silver/fact_commits.sql",
        "bronze_to_silver/fact_issues.sql"
    ]
    
    for file_name in required_sql_files:
        if not os.path.exists(file_name):
            logger.error(f"Required SQL file not found: {file_name}")
            return False
    
    logger.info(" All prerequisites met")
    return True

def get_github_token() -> str:
    """Get GitHub token from environment variables"""
    return os.getenv('GITHUB_TOKEN', '')

def get_airflow_config() -> Dict[str, Any]:
    """Get Airflow-specific configuration"""
    return {
        'max_active_runs': 1,
        'catchup': False,
        'tags': ['data-engineering', 'github', 'pipeline'],
        'description': 'Data Engineering Pipeline for GitHub Data Processing'
    }
