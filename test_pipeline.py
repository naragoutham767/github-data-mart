#!/usr/bin/env python3
"""
Pipeline Test Script
Validates the data engineering pipeline components
"""

import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineTester:
    """Test the data engineering pipeline components"""
    
    def __init__(self):
        self.spark = None
        self.test_results = {}
    
    def initialize_spark(self):
        """Initialize Spark session for testing"""
        try:
            self.spark = SparkSession.builder \
                .appName("PipelineTester") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized for testing")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            return False
    
    def test_landing_zone(self) -> bool:
        """Test landing zone data"""
        logger.info("Testing landing zone...")
        
        landing_dir = "landing"
        if not os.path.exists(landing_dir):
            logger.error(f"Landing directory not found: {landing_dir}")
            return False
        
        json_files = [f for f in os.listdir(landing_dir) if f.endswith('.json')]
        if not json_files:
            logger.error("No JSON files found in landing directory")
            return False
        
        logger.info(f"Found {len(json_files)} JSON files in landing directory")
        
        # Test JSON file structure
        for json_file in json_files[:3]:  # Test first 3 files
            filepath = os.path.join(landing_dir, json_file)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                
                if 'ingestion_timestamp' in data and 'data' in data:
                    logger.info(f" {json_file} has correct structure")
                else:
                    logger.warning(f" {json_file} has unexpected structure")
            except Exception as e:
                logger.error(f" Error reading {json_file}: {e}")
                return False
        
        self.test_results['landing_zone'] = True
        return True
    
    def test_bronze_layer(self) -> bool:
        """Test bronze layer data"""
        logger.info("Testing bronze layer...")
        
        if not self.spark:
            logger.error("Spark session not initialized")
            return False
        
        bronze_tables = ['repositories', 'repo_details', 'commits', 'issues']
        
        for table in bronze_tables:
            table_path = f"bronze/{table}"
            if os.path.exists(table_path):
                try:
                    df = self.spark.read.parquet(table_path)
                    row_count = df.count()
                    logger.info(f" Bronze table {table}: {row_count} rows")
                except Exception as e:
                    logger.error(f" Error reading bronze table {table}: {e}")
                    return False
            else:
                logger.warning(f" Bronze table {table} not found")
        
        self.test_results['bronze_layer'] = True
        return True
    
    def test_silver_layer(self) -> bool:
        """Test silver layer data"""
        logger.info("Testing silver layer...")
        
        if not self.spark:
            logger.error("Spark session not initialized")
            return False
        
        try:
            # Check if silver schema exists
            tables = self.spark.sql("SHOW TABLES IN silver").collect()
            if not tables:
                logger.error("No tables found in silver schema")
                return False
            
            silver_tables = [table['tableName'] for table in tables]
            logger.info(f"Found silver tables: {silver_tables}")
            
            # Test each silver table
            for table in silver_tables:
                try:
                    df = self.spark.sql(f"SELECT COUNT(*) as count FROM silver.{table}")
                    count = df.collect()[0]['count']
                    logger.info(f" Silver table {table}: {count} rows")
                except Exception as e:
                    logger.error(f" Error reading silver table {table}: {e}")
                    return False
            
            self.test_results['silver_layer'] = True
            return True
            
        except Exception as e:
            logger.error(f"Error testing silver layer: {e}")
            return False
    
    def test_data_quality(self) -> bool:
        """Test data quality metrics"""
        logger.info("Testing data quality...")
        
        if not self.spark:
            logger.error("Spark session not initialized")
            return False
        
        try:
            # Test repository data quality
            repo_df = self.spark.sql("SELECT * FROM silver.dim_repositories")
            
            # Check for null values in critical fields
            null_counts = repo_df.select(
                sum(when(col("repo_id").isNull(), 1).otherwise(0)).alias("null_repo_id"),
                sum(when(col("repo_name").isNull(), 1).otherwise(0)).alias("null_repo_name"),
                sum(when(col("stars").isNull(), 1).otherwise(0)).alias("null_stars")
            ).collect()[0]
            
            logger.info(f"Data quality metrics:")
            logger.info(f"  - Null repo_id: {null_counts['null_repo_id']}")
            logger.info(f"  - Null repo_name: {null_counts['null_repo_name']}")
            logger.info(f"  - Null stars: {null_counts['null_stars']}")
            
            # Test user data quality
            user_df = self.spark.sql("SELECT * FROM silver.dim_users")
            user_count = user_df.count()
            logger.info(f"  - Total users: {user_count}")
            
            self.test_results['data_quality'] = True
            return True
            
        except Exception as e:
            logger.error(f"Error testing data quality: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all pipeline tests"""
        logger.info("🧪 Starting Pipeline Tests")
        logger.info("=" * 50)
        
        # Initialize Spark
        if not self.initialize_spark():
            return False
        
        # Run tests
        tests = [
            ("Landing Zone", self.test_landing_zone),
            ("Bronze Layer", self.test_bronze_layer),
            ("Silver Layer", self.test_silver_layer),
            ("Data Quality", self.test_data_quality)
        ]
        
        all_passed = True
        
        for test_name, test_func in tests:
            logger.info(f"\n Running {test_name} test...")
            try:
                if test_func():
                    logger.info(f" {test_name} test passed")
                else:
                    logger.error(f" {test_name} test failed")
                    all_passed = False
            except Exception as e:
                logger.error(f" {test_name} test failed with error: {e}")
                all_passed = False
        
        # Show test summary
        logger.info("\n" + "=" * 50)
        logger.info("TEST SUMMARY")
        logger.info("=" * 50)
        
        for test_name, passed in self.test_results.items():
            status = " PASSED" if passed else " FAILED"
            logger.info(f"{test_name}: {status}")
        
        if all_passed:
            logger.info(" All tests passed!")
        else:
            logger.error(" Some tests failed!")
        
        return all_passed
    
    def cleanup(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")

def main():
    """Main function to run pipeline tests"""
    tester = PipelineTester()
    
    try:
        success = tester.run_all_tests()
        if success:
            logger.info("Pipeline testing completed successfully!")
            exit(0)
        else:
            logger.error("Pipeline testing failed!")
            exit(1)
    except KeyboardInterrupt:
        logger.info("Testing interrupted by user")
        exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during testing: {e}")
        exit(1)
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()
