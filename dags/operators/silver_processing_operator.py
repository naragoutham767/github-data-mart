#!/usr/bin/env python3
"""
Silver Layer Processing Airflow Operator
Custom operator for processing bronze layer data into silver layer using Spark SQL
"""

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SilverLayerProcessor:
    """Processes bronze layer data into silver layer using Spark SQL"""
    
    def __init__(self, app_name: str = "SilverLayerProcessor"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.warehouse.dir", "spark-warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
    
    def execute_sql_file(self, sql_file_path: str) -> None:
        """Execute a SQL file"""
        if not os.path.exists(sql_file_path):
            logger.error(f"SQL file not found: {sql_file_path}")
            return
        
        logger.info(f"Executing SQL file: {sql_file_path}")
        
        try:
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
            
            # Split SQL content by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    logger.info(f"Executing: {statement[:100]}...")
                    self.spark.sql(statement)
            
            logger.info(f"Successfully executed {sql_file_path}")
            
        except Exception as e:
            logger.error(f"Error executing {sql_file_path}: {e}")
            raise
    
    def create_silver_schema(self) -> None:
        """Create silver schema if it doesn't exist"""
        logger.info("Creating silver schema...")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        logger.info("Silver schema created")
    
    def process_all_silver_tables(self, sql_dir: str = "bronze_to_silver") -> None:
        """Process all SQL files to create silver layer tables"""
        logger.info("Starting silver layer processing...")
        
        # Create silver schema
        self.create_silver_schema()
        
        # List of SQL files to execute in order
        sql_files = [
            "dim_repositories.sql",
            "dim_users.sql",
            "dim_repo_permissions.sql",
            "fact_commits.sql",
            "fact_issues.sql",
            "fact_pull_requests.sql"
        ]
        
        for sql_file in sql_files:
            sql_path = os.path.join(sql_dir, sql_file)
            if os.path.exists(sql_path):
                try:
                    self.execute_sql_file(sql_path)
                    logger.info(f"Successfully processed {sql_file}")
                except Exception as e:
                    logger.error(f"Failed to process {sql_file}: {e}")
                    raise AirflowException(f"Failed to process {sql_file}: {e}")
            else:
                logger.warning(f"SQL file not found: {sql_path}")
        
        # Show summary of created tables
        self.show_silver_tables()
        
        logger.info("Silver layer processing completed!")
    
    def show_silver_tables(self) -> None:
        """Show summary of silver layer tables"""
        logger.info("Silver layer tables summary:")
        
        try:
            # Show tables in silver schema
            tables = self.spark.sql("SHOW TABLES IN silver").collect()
            
            for table in tables:
                table_name = table['tableName']
                logger.info(f"Table: {table_name}")
                
                # Get row count
                try:
                    count = self.spark.sql(f"SELECT COUNT(*) as count FROM silver.{table_name}").collect()[0]['count']
                    logger.info(f"  - Row count: {count}")
                except Exception as e:
                    logger.warning(f"  - Could not get row count: {e}")
                
                # Show schema
                try:
                    schema = self.spark.sql(f"DESCRIBE silver.{table_name}").collect()
                    logger.info(f"  - Columns: {len(schema)}")
                except Exception as e:
                    logger.warning(f"  - Could not get schema: {e}")
                
        except Exception as e:
            logger.error(f"Error showing silver tables: {e}")
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()
        logger.info("Spark session closed")

class SilverProcessingOperator(BaseOperator):
    """Custom operator for silver layer data processing"""
    
    @apply_defaults
    def __init__(
        self,
        sql_dir: str = "bronze_to_silver",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql_dir = sql_dir
    
    def execute(self, context):
        """Execute the silver layer processing"""
        logger.info("Starting silver layer processing...")
        
        processor = SilverLayerProcessor()
        
        try:
            processor.process_all_silver_tables(self.sql_dir)
            logger.info("Silver layer processing completed successfully!")
            return "Silver layer processing completed successfully"
        except Exception as e:
            logger.error(f"Silver layer processing failed: {e}")
            raise AirflowException(f"Silver layer processing failed: {e}")
        finally:
            processor.close()
