#!/usr/bin/env python3
"""
GitHub API Ingestion Airflow Operator
Custom operator for fetching data from GitHub API
"""

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import requests
import json
import os
from datetime import datetime
import time
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class GitHubAPIClient:
    """Client for fetching data from GitHub API"""
    
    def __init__(self, token: str = None):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DataEngineering-Pipeline"
        }
        if token:
            self.headers["Authorization"] = f"token {token}"
    
    def fetch_repositories(self, query: str = "language:python", per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch repositories from GitHub API"""
        url = f"{self.base_url}/search/repositories"
        params = {
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": per_page
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched {len(data.get('items', []))} repositories")
            return data.get('items', [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching repositories: {e}")
            return []
    
    def fetch_repository_details(self, owner: str, repo: str) -> Dict[str, Any]:
        """Fetch detailed information about a specific repository"""
        url = f"{self.base_url}/repos/{owner}/{repo}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching repository details for {owner}/{repo}: {e}")
            return {}
    
    def fetch_commits(self, owner: str, repo: str, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch commits for a repository"""
        url = f"{self.base_url}/repos/{owner}/{repo}/commits"
        params = {"per_page": per_page}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching commits for {owner}/{repo}: {e}")
            return []
    
    def fetch_issues(self, owner: str, repo: str, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch issues for a repository"""
        url = f"{self.base_url}/repos/{owner}/{repo}/issues"
        params = {"per_page": per_page, "state": "all"}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching issues for {owner}/{repo}: {e}")
            return []

class GitHubIngestionOperator(BaseOperator):
    """Custom operator for GitHub API data ingestion"""
    
    @apply_defaults
    def __init__(
        self,
        github_token: str = None,
        query: str = "language:python stars:>1000",
        per_page: int = 50,
        top_repos_count: int = 10,
        output_dir: str = "landing",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.github_token = github_token
        self.query = query
        self.per_page = per_page
        self.top_repos_count = top_repos_count
        self.output_dir = output_dir
    
    def save_json_data(self, data: Dict[str, Any], filename: str) -> str:
        """Save data as JSON file with timestamp"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Add metadata
        data_with_metadata = {
            "ingestion_timestamp": datetime.now().isoformat(),
            "data": data
        }
        
        filepath = os.path.join(self.output_dir, f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data_with_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved data to {filepath}")
        return filepath
    
    def execute(self, context):
        """Execute the GitHub API ingestion"""
        logger.info("Starting GitHub API data ingestion...")
        
        # Initialize API client
        client = GitHubAPIClient(token=self.github_token)
        
        # Fetch repositories
        logger.info(f"Fetching repositories with query: {self.query}")
        repositories = client.fetch_repositories(query=self.query, per_page=self.per_page)
        
        if not repositories:
            raise AirflowException("No repositories fetched from GitHub API")
        
        # Save repositories data
        self.save_json_data({"repositories": repositories}, "repositories")
        
        # Fetch additional data for top repositories
        top_repos = repositories[:self.top_repos_count]
        
        for i, repo in enumerate(top_repos):
            owner = repo['owner']['login']
            repo_name = repo['name']
            
            logger.info(f"Processing repository {i+1}/{self.top_repos_count}: {owner}/{repo_name}")
            
            # Fetch detailed repository information
            repo_details = client.fetch_repository_details(owner, repo_name)
            if repo_details:
                self.save_json_data(repo_details, f"repo_details_{owner}_{repo_name}")
            
            # Fetch commits
            commits = client.fetch_commits(owner, repo_name, per_page=50)
            if commits:
                self.save_json_data({"commits": commits}, f"commits_{owner}_{repo_name}")
            
            # Fetch issues
            issues = client.fetch_issues(owner, repo_name, per_page=50)
            if issues:
                self.save_json_data({"issues": issues}, f"issues_{owner}_{repo_name}")
            
            # Rate limiting - GitHub API has rate limits
            time.sleep(1)
        
        logger.info("GitHub API data ingestion completed successfully!")
        return f"Successfully ingested data for {len(repositories)} repositories"
