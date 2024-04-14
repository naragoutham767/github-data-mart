#!/usr/bin/env python3
"""
Bronze Layer Processing Airflow Operator
Custom operator for processing JSON data into bronze layer Parquet tables
"""

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BronzeLayerProcessor:
    """Processes raw JSON data into bronze layer Parquet tables"""
    
    def __init__(self, app_name: str = "BronzeLayerProcessor"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
    
    def read_json_files(self, landing_dir: str) -> dict:
        """Read all JSON files from landing directory"""
        json_files = {}
        
        if not os.path.exists(landing_dir):
            logger.error(f"Landing directory {landing_dir} does not exist")
            return json_files
        
        for filename in os.listdir(landing_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(landing_dir, filename)
                logger.info(f"Reading JSON file: {filename}")
                
                try:
                    # Read JSON with schema inference
                    df = self.spark.read.option("multiline", "true").json(filepath)
                    json_files[filename] = df
                    logger.info(f"Successfully read {filename} with {df.count()} rows")
                except Exception as e:
                    logger.error(f"Error reading {filename}: {e}")
        
        return json_files
    
    def process_repositories(self, df) -> None:
        """Process repositories data into bronze layer"""
        logger.info("Processing repositories data...")
        
        # Extract repositories from the nested structure
        repositories_df = df.select(
            col("data.repositories").alias("repositories")
        ).select(explode(col("repositories")).alias("repo"))
        
        # Flatten the repository structure
        bronze_repositories = repositories_df.select(
            col("repo.id").alias("repo_id"),
            col("repo.name").alias("repo_name"),
            col("repo.full_name").alias("full_name"),
            col("repo.description").alias("description"),
            col("repo.html_url").alias("html_url"),
            col("repo.clone_url").alias("clone_url"),
            col("repo.language").alias("language"),
            col("repo.stargazers_count").alias("stars"),
            col("repo.watchers_count").alias("watchers"),
            col("repo.forks_count").alias("forks"),
            col("repo.open_issues_count").alias("open_issues"),
            col("repo.size").alias("size"),
            col("repo.created_at").alias("created_at"),
            col("repo.updated_at").alias("updated_at"),
            col("repo.pushed_at").alias("pushed_at"),
            col("repo.default_branch").alias("default_branch"),
            col("repo.owner.id").alias("owner_id"),
            col("repo.owner.login").alias("owner_login"),
            col("repo.owner.type").alias("owner_type"),
            col("repo.owner.html_url").alias("owner_url"),
            col("repo.license.name").alias("license_name"),
            col("repo.license.key").alias("license_key"),
            col("repo.topics").alias("topics"),
            col("repo.archived").alias("archived"),
            col("repo.disabled").alias("disabled"),
            col("repo.private").alias("private"),
            col("repo.fork").alias("fork"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Write to bronze layer
        output_path = "bronze/repositories"
        bronze_repositories.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        logger.info(f"Bronze repositories table written to {output_path}")
    
    def process_repo_details(self, df) -> None:
        """Process repository details data into bronze layer"""
        logger.info("Processing repository details data...")
        
        # Extract data from nested structure
        repo_details = df.select(
            col("data.*")
        ).select(
            col("id").alias("repo_id"),
            col("name").alias("repo_name"),
            col("full_name").alias("full_name"),
            col("description").alias("description"),
            col("html_url").alias("html_url"),
            col("clone_url").alias("clone_url"),
            col("language").alias("language"),
            col("stargazers_count").alias("stars"),
            col("watchers_count").alias("watchers"),
            col("forks_count").alias("forks"),
            col("open_issues_count").alias("open_issues"),
            col("size").alias("size"),
            col("created_at").alias("created_at"),
            col("updated_at").alias("updated_at"),
            col("pushed_at").alias("pushed_at"),
            col("default_branch").alias("default_branch"),
            col("owner.id").alias("owner_id"),
            col("owner.login").alias("owner_login"),
            col("owner.type").alias("owner_type"),
            col("owner.html_url").alias("owner_url"),
            col("license.name").alias("license_name"),
            col("license.key").alias("license_key"),
            col("topics").alias("topics"),
            col("archived").alias("archived"),
            col("disabled").alias("disabled"),
            col("private").alias("private"),
            col("fork").alias("fork"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Write to bronze layer
        output_path = "bronze/repo_details"
        repo_details.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        logger.info(f"Bronze repo details table written to {output_path}")
    
    def process_commits(self, df) -> None:
        """Process commits data into bronze layer"""
        logger.info("Processing commits data...")
        
        # Extract commits from nested structure
        commits_df = df.select(
            col("data.commits").alias("commits")
        ).select(explode(col("commits")).alias("commit"))
        
        # Flatten the commit structure
        bronze_commits = commits_df.select(
            col("commit.sha").alias("commit_sha"),
            col("commit.node_id").alias("node_id"),
            col("commit.commit.message").alias("message"),
            col("commit.commit.author.name").alias("author_name"),
            col("commit.commit.author.email").alias("author_email"),
            col("commit.commit.author.date").alias("author_date"),
            col("commit.commit.committer.name").alias("committer_name"),
            col("commit.commit.committer.email").alias("committer_email"),
            col("commit.commit.committer.date").alias("committer_date"),
            col("commit.commit.tree.sha").alias("tree_sha"),
            col("commit.commit.url").alias("commit_url"),
            col("commit.commit.comment_count").alias("comment_count"),
            col("commit.commit.verification.verified").alias("verification_verified"),
            col("commit.commit.verification.reason").alias("verification_reason"),
            col("commit.html_url").alias("html_url"),
            col("commit.comments_url").alias("comments_url"),
            col("commit.author.id").alias("author_id"),
            col("commit.author.login").alias("author_login"),
            col("commit.author.type").alias("author_type"),
            col("commit.author.html_url").alias("author_html_url"),
            col("commit.committer.id").alias("committer_id"),
            col("commit.committer.login").alias("committer_login"),
            col("commit.committer.type").alias("committer_type"),
            col("commit.committer.html_url").alias("committer_html_url"),
            col("commit.parents").alias("parents"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Write to bronze layer
        output_path = "bronze/commits"
        bronze_commits.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        logger.info(f"Bronze commits table written to {output_path}")
    
    def process_issues(self, df) -> None:
        """Process issues data into bronze layer"""
        logger.info("Processing issues data...")
        
        # Extract issues from nested structure
        issues_df = df.select(
            col("data.issues").alias("issues")
        ).select(explode(col("issues")).alias("issue"))
        
        # Flatten the issue structure
        bronze_issues = issues_df.select(
            col("issue.id").alias("issue_id"),
            col("issue.node_id").alias("node_id"),
            col("issue.number").alias("issue_number"),
            col("issue.title").alias("title"),
            col("issue.body").alias("body"),
            col("issue.state").alias("state"),
            col("issue.locked").alias("locked"),
            col("issue.assignee.id").alias("assignee_id"),
            col("issue.assignee.login").alias("assignee_login"),
            col("issue.assignee.type").alias("assignee_type"),
            col("issue.assignee.html_url").alias("assignee_html_url"),
            col("issue.user.id").alias("user_id"),
            col("issue.user.login").alias("user_login"),
            col("issue.user.type").alias("user_type"),
            col("issue.user.html_url").alias("user_html_url"),
            col("issue.labels").alias("labels"),
            col("issue.milestone.id").alias("milestone_id"),
            col("issue.milestone.title").alias("milestone_title"),
            col("issue.milestone.description").alias("milestone_description"),
            col("issue.milestone.state").alias("milestone_state"),
            col("issue.comments").alias("comments_count"),
            col("issue.created_at").alias("created_at"),
            col("issue.updated_at").alias("updated_at"),
            col("issue.closed_at").alias("closed_at"),
            col("issue.author_association").alias("author_association"),
            col("issue.active_lock_reason").alias("active_lock_reason"),
            col("issue.pull_request").alias("pull_request"),
            col("issue.html_url").alias("html_url"),
            col("issue.url").alias("url"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Write to bronze layer
        output_path = "bronze/issues"
        bronze_issues.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
        
        logger.info(f"Bronze issues table written to {output_path}")
    
    def process_all_data(self, landing_dir: str = "landing") -> None:
        """Process all JSON files from landing directory"""
        logger.info("Starting bronze layer processing...")
        
        # Read all JSON files
        json_files = self.read_json_files(landing_dir)
        
        if not json_files:
            raise AirflowException("No JSON files found in landing directory")
        
        # Process each type of data
        for filename, df in json_files.items():
            logger.info(f"Processing file: {filename}")
            
            try:
                if "repositories" in filename and "details" not in filename:
                    self.process_repositories(df)
                elif "repo_details" in filename:
                    self.process_repo_details(df)
                elif "commits" in filename:
                    self.process_commits(df)
                elif "issues" in filename:
                    self.process_issues(df)
                else:
                    logger.warning(f"Unknown file type: {filename}")
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                raise AirflowException(f"Error processing {filename}: {e}")
        
        logger.info("Bronze layer processing completed!")
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()
        logger.info("Spark session closed")

class BronzeProcessingOperator(BaseOperator):
    """Custom operator for bronze layer data processing"""
    
    @apply_defaults
    def __init__(
        self,
        landing_dir: str = "landing",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.landing_dir = landing_dir
    
    def execute(self, context):
        """Execute the bronze layer processing"""
        logger.info("Starting bronze layer processing...")
        
        processor = BronzeLayerProcessor()
        
        try:
            processor.process_all_data(self.landing_dir)
            logger.info("Bronze layer processing completed successfully!")
            return "Bronze layer processing completed successfully"
        except Exception as e:
            logger.error(f"Bronze layer processing failed: {e}")
            raise AirflowException(f"Bronze layer processing failed: {e}")
        finally:
            processor.close()
