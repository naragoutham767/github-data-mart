-- Silver Layer: Dimension Table - Users
-- Creates a clean dimension table for users (owners, authors, committers)

CREATE OR REPLACE TEMPORARY VIEW bronze_repositories AS
SELECT * FROM parquet.`bronze/repositories`;

CREATE OR REPLACE TEMPORARY VIEW bronze_commits AS
SELECT * FROM parquet.`bronze/commits`;

CREATE OR REPLACE TEMPORARY VIEW bronze_issues AS
SELECT * FROM parquet.`bronze/issues`;

-- Create dimension table for users
CREATE OR REPLACE TABLE silver.dim_users AS
WITH all_users AS (
    -- Repository owners
    SELECT DISTINCT
        owner_id as user_id,
        owner_login as user_login,
        owner_type as user_type,
        owner_url as user_html_url,
        'owner' as user_role,
        NULL as user_email,
        NULL as user_name
    FROM bronze_repositories
    WHERE owner_id IS NOT NULL
    
    UNION ALL
    
    -- Commit authors
    SELECT DISTINCT
        author_id as user_id,
        author_login as user_login,
        author_type as user_type,
        author_html_url as user_html_url,
        'author' as user_role,
        author_email,
        author_name
    FROM bronze_commits
    WHERE author_id IS NOT NULL
    
    UNION ALL
    
    -- Commit committers
    SELECT DISTINCT
        committer_id as user_id,
        committer_login as user_login,
        committer_type as user_type,
        committer_html_url as user_html_url,
        'committer' as user_role,
        committer_email,
        committer_name
    FROM bronze_commits
    WHERE committer_id IS NOT NULL
    
    UNION ALL
    
    -- Issue users
    SELECT DISTINCT
        user_id,
        user_login,
        user_type,
        user_html_url,
        'issue_user' as user_role,
        NULL as user_email,
        NULL as user_name
    FROM bronze_issues
    WHERE user_id IS NOT NULL
    
    UNION ALL
    
    -- Issue assignees
    SELECT DISTINCT
        assignee_id as user_id,
        assignee_login as user_login,
        assignee_type as user_type,
        assignee_html_url as user_html_url,
        'assignee' as user_role,
        NULL as user_email,
        NULL as user_name
    FROM bronze_issues
    WHERE assignee_id IS NOT NULL
),
user_aggregated AS (
    SELECT 
        user_id,
        user_login,
        user_type,
        user_html_url,
        user_email,
        user_name,
        collect_list(DISTINCT user_role) as user_roles,
        count(DISTINCT user_role) as role_count
    FROM all_users
    WHERE user_id IS NOT NULL
    GROUP BY user_id, user_login, user_type, user_html_url, user_email, user_name
),
user_cleaned AS (
    SELECT 
        user_id,
        user_login,
        CASE 
            WHEN user_type IS NULL THEN 'User'
            ELSE user_type
        END as user_type,
        user_html_url,
        CASE 
            WHEN user_email IS NULL OR TRIM(user_email) = '' THEN 'No email available'
            ELSE user_email
        END as user_email,
        CASE 
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 'No name available'
            ELSE TRIM(user_name)
        END as user_name,
        user_roles,
        role_count,
        -- Add computed fields
        CASE 
            WHEN array_contains(user_roles, 'owner') THEN 'Repository Owner'
            WHEN array_contains(user_roles, 'author') AND array_contains(user_roles, 'committer') THEN 'Active Contributor'
            WHEN array_contains(user_roles, 'author') THEN 'Code Author'
            WHEN array_contains(user_roles, 'committer') THEN 'Code Committer'
            WHEN array_contains(user_roles, 'issue_user') THEN 'Issue Participant'
            WHEN array_contains(user_roles, 'assignee') THEN 'Issue Assignee'
            ELSE 'Other'
        END as primary_role,
        CASE 
            WHEN role_count >= 3 THEN 'Multi-role User'
            WHEN role_count = 2 THEN 'Dual-role User'
            ELSE 'Single-role User'
        END as user_activity_level
    FROM user_aggregated
)
SELECT 
    user_id,
    user_login,
    user_type,
    user_html_url,
    user_email,
    user_name,
    user_roles,
    role_count,
    primary_role,
    user_activity_level,
    current_timestamp() as silver_ingestion_timestamp
FROM user_cleaned;

