"""
Configuration file for the Data Engineering Project
Contains all configuration settings and constants
"""

import os
from typing import Dict, Any

class Config:
    """Configuration class for the data engineering project"""
    
    # API Configuration
    GITHUB_API_BASE_URL = "https://api.github.com"
    GITHUB_API_TOKEN = os.getenv("GITHUB_TOKEN", None)
    API_RATE_LIMIT_DELAY = 1  # seconds between requests
    
    # Data Processing Configuration
    LANDING_DIR = "landing"
    BRONZE_DIR = "bronze"
    SILVER_DIR = "silver"
    
    # Spark Configuration
    SPARK_CONFIG = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.warehouse.dir": "spark-warehouse",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true"
    }
    
    # Data Quality Configuration
    MAX_NULL_PERCENTAGE = 0.1  # 10% max null values
    MIN_ROWS_THRESHOLD = 1
    
    # File Processing Configuration
    JSON_MULTILINE = True
    PARQUET_COMPRESSION = "snappy"
    
    # Logging Configuration
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    
    # GitHub API Query Configuration
    GITHUB_QUERIES = {
        "python_repos": "language:python stars:>1000",
        "javascript_repos": "language:javascript stars:>1000",
        "java_repos": "language:java stars:>1000"
    }
    
    # Data Processing Limits
    MAX_REPOSITORIES = 50
    MAX_COMMITS_PER_REPO = 50
    MAX_ISSUES_PER_REPO = 50
    
    # Airflow Configuration
    AIRFLOW_DAG_DEFAULT_ARGS = {
        'owner': 'data-engineering-team',
        'depends_on_past': False,
        'start_date': '2024-01-01',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': 300,  # 5 minutes
    }
    
    AIRFLOW_DAG_CONFIG = {
        'max_active_runs': 1,
        'catchup': False,
        'tags': ['data-engineering', 'github', 'pipeline'],
        'description': 'Data Engineering Pipeline for GitHub Data Processing'
    }
    
    @classmethod
    def get_spark_config(cls) -> Dict[str, str]:
        """Get Spark configuration as dictionary"""
        return cls.SPARK_CONFIG.copy()
    
    @classmethod
    def get_api_headers(cls) -> Dict[str, str]:
        """Get API headers for GitHub requests"""
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DataEngineering-Pipeline"
        }
        if cls.GITHUB_API_TOKEN:
            headers["Authorization"] = f"token {cls.GITHUB_API_TOKEN}"
        return headers
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate configuration settings"""
        # Check if required directories exist
        required_dirs = [cls.LANDING_DIR, cls.BRONZE_DIR, cls.SILVER_DIR]
        for dir_name in required_dirs:
            if not os.path.exists(dir_name):
                print(f"Warning: Directory {dir_name} does not exist")
        
        # Check if GitHub token is provided
        if not cls.GITHUB_API_TOKEN:
            print("Warning: GITHUB_TOKEN not set. API rate limits may apply.")
        
        return True

# Environment-specific configurations
class DevelopmentConfig(Config):
    """Development environment configuration"""
    LOG_LEVEL = "DEBUG"
    MAX_REPOSITORIES = 10
    MAX_COMMITS_PER_REPO = 10
    MAX_ISSUES_PER_REPO = 10

class ProductionConfig(Config):
    """Production environment configuration"""
    LOG_LEVEL = "WARNING"
    MAX_REPOSITORIES = 100
    MAX_COMMITS_PER_REPO = 100
    MAX_ISSUES_PER_REPO = 100

# Configuration factory
def get_config(env: str = "development") -> Config:
    """Get configuration based on environment"""
    if env.lower() == "production":
        return ProductionConfig()
    else:
        return DevelopmentConfig()

# Default configuration
config = get_config()

