-- Silver Layer: Dimension Table - Repository Permissions
-- Creates a dimension table for repository permissions and access control

CREATE OR REPLACE TEMPORARY VIEW bronze_repositories AS
SELECT * FROM parquet.`bronze/repositories`;

CREATE OR REPLACE TEMPORARY VIEW bronze_repo_details AS
SELECT * FROM parquet.`bronze/repo_details`;

-- Create dimension table for repository permissions
CREATE OR REPLACE TABLE silver.dim_repo_permissions AS
WITH repo_permissions_base AS (
    SELECT 
        COALESCE(r.repo_id, rd.repo_id) as repo_id,
        COALESCE(r.repo_name, rd.repo_name) as repo_name,
        COALESCE(r.full_name, rd.full_name) as full_name,
        COALESCE(r.owner_id, rd.owner_id) as owner_id,
        COALESCE(r.owner_login, rd.owner_login) as owner_login,
        COALESCE(r.owner_type, rd.owner_type) as owner_type,
        COALESCE(r.private, rd.private) as is_private,
        COALESCE(r.archived, rd.archived) as is_archived,
        COALESCE(r.disabled, rd.disabled) as is_disabled,
        COALESCE(r.fork, rd.fork) as is_fork,
        COALESCE(r.language, rd.language) as primary_language,
        COALESCE(r.stars, rd.stars) as stargazers_count,
        COALESCE(r.watchers, rd.watchers) as watchers_count,
        COALESCE(r.forks, rd.forks) as forks_count,
        COALESCE(r.open_issues, rd.open_issues) as open_issues_count,
        COALESCE(r.created_at, rd.created_at) as created_at,
        COALESCE(r.updated_at, rd.updated_at) as updated_at,
        COALESCE(r.pushed_at, rd.pushed_at) as pushed_at,
        COALESCE(r.bronze_ingestion_timestamp, rd.bronze_ingestion_timestamp) as bronze_ingestion_timestamp
    FROM bronze_repositories r
    FULL OUTER JOIN bronze_repo_details rd ON r.repo_id = rd.repo_id
),
permissions_enhanced AS (
    SELECT 
        repo_id,
        repo_name,
        full_name,
        owner_id,
        owner_login,
        owner_type,
        is_private,
        is_archived,
        is_disabled,
        is_fork,
        primary_language,
        stargazers_count,
        watchers_count,
        forks_count,
        open_issues_count,
        created_at,
        updated_at,
        pushed_at,
        bronze_ingestion_timestamp,
        -- Calculate permission levels based on repository characteristics
        CASE 
            WHEN is_private = true THEN 'Private'
            ELSE 'Public'
        END as visibility_level,
        CASE 
            WHEN is_archived = true THEN 'Archived'
            WHEN is_disabled = true THEN 'Disabled'
            WHEN is_fork = true THEN 'Fork'
            ELSE 'Active'
        END as repository_status,
        CASE 
            WHEN stargazers_count >= 10000 THEN 'Very Popular'
            WHEN stargazers_count >= 1000 THEN 'Popular'
            WHEN stargazers_count >= 100 THEN 'Moderately Popular'
            WHEN stargazers_count >= 10 THEN 'Somewhat Popular'
            ELSE 'Less Popular'
        END as popularity_tier,
        CASE 
            WHEN forks_count >= 1000 THEN 'High Fork Activity'
            WHEN forks_count >= 100 THEN 'Moderate Fork Activity'
            WHEN forks_count >= 10 THEN 'Low Fork Activity'
            ELSE 'Minimal Fork Activity'
        END as fork_activity_level,
        CASE 
            WHEN open_issues_count >= 100 THEN 'High Issue Volume'
            WHEN open_issues_count >= 50 THEN 'Moderate Issue Volume'
            WHEN open_issues_count >= 10 THEN 'Low Issue Volume'
            ELSE 'Minimal Issue Volume'
        END as issue_volume_level,
        -- Calculate activity metrics
        CASE 
            WHEN updated_at IS NOT NULL AND created_at IS NOT NULL 
            THEN unix_timestamp(updated_at) - unix_timestamp(created_at)
            ELSE NULL 
        END as repository_age_seconds,
        CASE 
            WHEN pushed_at IS NOT NULL AND updated_at IS NOT NULL 
            THEN unix_timestamp(pushed_at) - unix_timestamp(updated_at)
            ELSE NULL 
        END as last_activity_seconds,
        -- Determine access patterns
        CASE 
            WHEN is_private = true THEN 'Restricted Access'
            WHEN is_archived = true THEN 'Read-Only Access'
            WHEN is_disabled = true THEN 'No Access'
            ELSE 'Open Access'
        END as access_level,
        -- Repository maturity assessment
        CASE 
            WHEN is_archived = true THEN 'Archived'
            WHEN is_disabled = true THEN 'Disabled'
            WHEN stargazers_count >= 1000 AND forks_count >= 100 THEN 'Mature'
            WHEN stargazers_count >= 100 AND forks_count >= 10 THEN 'Developing'
            WHEN stargazers_count >= 10 THEN 'Growing'
            ELSE 'Early Stage'
        END as maturity_level
    FROM repo_permissions_base
)
SELECT 
    repo_id,
    repo_name,
    full_name,
    owner_id,
    owner_login,
    owner_type,
    is_private,
    is_archived,
    is_disabled,
    is_fork,
    primary_language,
    stargazers_count,
    watchers_count,
    forks_count,
    open_issues_count,
    created_at,
    updated_at,
    pushed_at,
    visibility_level,
    repository_status,
    popularity_tier,
    fork_activity_level,
    issue_volume_level,
    repository_age_seconds,
    last_activity_seconds,
    access_level,
    maturity_level,
    bronze_ingestion_timestamp,
    current_timestamp() as silver_ingestion_timestamp
FROM permissions_enhanced;
