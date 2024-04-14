-- Silver Layer: Fact Table - Commits
-- Creates a fact table for commit activities with proper relationships

CREATE OR REPLACE TEMPORARY VIEW bronze_commits AS
SELECT * FROM parquet.`bronze/commits`;

CREATE OR REPLACE TEMPORARY VIEW bronze_repositories AS
SELECT * FROM parquet.`bronze/repositories`;

-- Create fact table for commits
CREATE OR REPLACE TABLE silver.fact_commits AS
WITH commits_cleaned AS (
    SELECT 
        commit_sha,
        node_id,
        message,
        author_name,
        author_email,
        author_date,
        committer_name,
        committer_email,
        committer_date,
        tree_sha,
        commit_url,
        comment_count,
        verification_verified,
        verification_reason,
        html_url,
        comments_url,
        author_id,
        author_login,
        author_type,
        author_html_url,
        committer_id,
        committer_login,
        committer_type,
        committer_html_url,
        parents,
        bronze_ingestion_timestamp,
        -- Extract repository information from commit URL
        regexp_extract(html_url, 'github\\.com/([^/]+)/([^/]+)/', 1) as repo_owner,
        regexp_extract(html_url, 'github\\.com/[^/]+/([^/]+)/', 1) as repo_name,
        -- Add computed fields
        CASE 
            WHEN length(message) > 100 THEN 'Long'
            WHEN length(message) > 50 THEN 'Medium'
            ELSE 'Short'
        END as message_length_category,
        CASE 
            WHEN message LIKE '%fix%' OR message LIKE '%bug%' THEN 'Bug Fix'
            WHEN message LIKE '%feat%' OR message LIKE '%feature%' THEN 'Feature'
            WHEN message LIKE '%refactor%' THEN 'Refactor'
            WHEN message LIKE '%test%' THEN 'Test'
            WHEN message LIKE '%doc%' THEN 'Documentation'
            WHEN message LIKE '%style%' THEN 'Style'
            ELSE 'Other'
        END as commit_type,
        CASE 
            WHEN verification_verified = true THEN 'Verified'
            WHEN verification_verified = false THEN 'Unverified'
            ELSE 'Unknown'
        END as verification_status,
        CASE 
            WHEN author_date IS NOT NULL AND committer_date IS NOT NULL 
            THEN unix_timestamp(committer_date) - unix_timestamp(author_date)
            ELSE NULL
        END as commit_delay_seconds
    FROM bronze_commits
    WHERE commit_sha IS NOT NULL
),
commits_with_repo AS (
    SELECT 
        c.*,
        r.repo_id,
        r.repo_name,
        r.full_name,
        r.language,
        r.stars,
        r.forks
    FROM commits_cleaned c
    LEFT JOIN bronze_repositories r 
        ON c.repo_owner = r.owner_login 
        AND c.repo_name = r.repo_name
)
SELECT 
    commit_sha,
    node_id,
    message,
    author_name,
    author_email,
    author_date,
    committer_name,
    committer_email,
    committer_date,
    tree_sha,
    commit_url,
    comment_count,
    verification_verified,
    verification_reason,
    html_url,
    comments_url,
    author_id,
    author_login,
    author_type,
    author_html_url,
    committer_id,
    committer_login,
    committer_type,
    committer_html_url,
    parents,
    repo_id,
    repo_name,
    full_name as repo_full_name,
    language as repo_language,
    stars as repo_stars,
    forks as repo_forks,
    message_length_category,
    commit_type,
    verification_status,
    commit_delay_seconds,
    bronze_ingestion_timestamp,
    current_timestamp() as silver_ingestion_timestamp
FROM commits_with_repo;

