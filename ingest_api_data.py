#!/usr/bin/env python3
"""
API Data Ingestion Script
Fetches data from GitHub API and saves JSON files to landing/ directory
"""

import requests
import json
import os
from datetime import datetime
import time
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

def save_json_data(data: Dict[str, Any], filename: str, output_dir: str = "landing") -> str:
    """Save data as JSON file with timestamp"""
    os.makedirs(output_dir, exist_ok=True)
    
    # Add metadata
    data_with_metadata = {
        "ingestion_timestamp": datetime.now().isoformat(),
        "data": data
    }
    
    filepath = os.path.join(output_dir, f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data_with_metadata, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved data to {filepath}")
    return filepath

def main():
    """Main function to orchestrate data ingestion"""
    logger.info("Starting GitHub API data ingestion...")
    
    # Initialize API client
    client = GitHubAPIClient()
    
    # Fetch repositories
    logger.info("Fetching popular Python repositories...")
    repositories = client.fetch_repositories(query="language:python stars:>1000", per_page=50)
    
    if repositories:
        # Save repositories data
        save_json_data({"repositories": repositories}, "repositories")
        
        # Fetch additional data for top repositories
        top_repos = repositories[:10]  # Process top 10 repositories
        
        for i, repo in enumerate(top_repos):
            owner = repo['owner']['login']
            repo_name = repo['name']
            
            logger.info(f"Processing repository {i+1}/10: {owner}/{repo_name}")
            
            # Fetch detailed repository information
            repo_details = client.fetch_repository_details(owner, repo_name)
            if repo_details:
                save_json_data(repo_details, f"repo_details_{owner}_{repo_name}")
            
            # Fetch commits
            commits = client.fetch_commits(owner, repo_name, per_page=50)
            if commits:
                save_json_data({"commits": commits}, f"commits_{owner}_{repo_name}")
            
            # Fetch issues
            issues = client.fetch_issues(owner, repo_name, per_page=50)
            if issues:
                save_json_data({"issues": issues}, f"issues_{owner}_{repo_name}")
            
            # Rate limiting - GitHub API has rate limits
            time.sleep(1)
    
    logger.info("Data ingestion completed successfully!")

if __name__ == "__main__":
    main()

