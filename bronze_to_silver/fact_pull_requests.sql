-- Silver Layer: Fact Table - Pull Requests
-- Creates a fact table for pull request activities with proper relationships

CREATE OR REPLACE TEMPORARY VIEW bronze_issues AS
SELECT * FROM parquet.`bronze/issues`;

CREATE OR REPLACE TEMPORARY VIEW bronze_repositories AS
SELECT * FROM parquet.`bronze/repositories`;

-- Create fact table for pull requests
CREATE OR REPLACE TABLE silver.fact_pull_requests AS
WITH pull_requests_cleaned AS (
    SELECT 
        issue_id as pr_id,
        node_id,
        issue_number as pr_number,
        title,
        body,
        state,
        locked,
        assignee_id,
        assignee_login,
        assignee_type,
        assignee_html_url,
        user_id as author_id,
        user_login as author_login,
        user_type as author_type,
        user_html_url as author_html_url,
        labels,
        milestone_id,
        milestone_title,
        milestone_description,
        milestone_state,
        comments_count,
        created_at,
        updated_at,
        closed_at,
        author_association,
        active_lock_reason,
        pull_request,
        html_url,
        url,
        bronze_ingestion_timestamp
    FROM bronze_issues
    WHERE pull_request IS NOT NULL  -- Only pull requests, not issues
),
pr_with_repo AS (
    SELECT 
        pr.*,
        r.repo_id,
        r.repo_name,
        r.full_name,
        r.owner_id,
        r.owner_login
    FROM pull_requests_cleaned pr
    LEFT JOIN bronze_repositories r ON pr.author_id = r.owner_id
),
pr_enhanced AS (
    SELECT 
        pr_id,
        node_id,
        pr_number,
        title,
        COALESCE(body, '') as body,
        state,
        locked,
        assignee_id,
        assignee_login,
        assignee_type,
        assignee_html_url,
        author_id,
        author_login,
        author_type,
        author_html_url,
        labels,
        milestone_id,
        milestone_title,
        milestone_description,
        milestone_state,
        comments_count,
        created_at,
        updated_at,
        closed_at,
        author_association,
        active_lock_reason,
        pull_request,
        html_url,
        url,
        repo_id,
        repo_name,
        full_name,
        owner_id,
        owner_login,
        bronze_ingestion_timestamp,
        -- Calculate derived fields
        CASE 
            WHEN closed_at IS NOT NULL AND created_at IS NOT NULL 
            THEN unix_timestamp(closed_at) - unix_timestamp(created_at)
            ELSE NULL 
        END as resolution_time_seconds,
        CASE 
            WHEN state = 'closed' AND closed_at IS NOT NULL 
            THEN 1 
            ELSE 0 
        END as is_resolved,
        CASE 
            WHEN assignee_id IS NOT NULL 
            THEN 1 
            ELSE 0 
        END as is_assigned,
        CASE 
            WHEN milestone_id IS NOT NULL 
            THEN 1 
            ELSE 0 
        END as has_milestone,
        CASE 
            WHEN labels IS NOT NULL AND size(labels) > 0 
            THEN 1 
            ELSE 0 
        END as has_labels
    FROM pr_with_repo
)
SELECT 
    pr_id,
    node_id,
    pr_number,
    title,
    body,
    state,
    locked,
    assignee_id,
    assignee_login,
    assignee_type,
    assignee_html_url,
    author_id,
    author_login,
    author_type,
    author_html_url,
    labels,
    milestone_id,
    milestone_title,
    milestone_description,
    milestone_state,
    comments_count,
    created_at,
    updated_at,
    closed_at,
    author_association,
    active_lock_reason,
    pull_request,
    html_url,
    url,
    repo_id,
    repo_name,
    full_name,
    owner_id,
    owner_login,
    resolution_time_seconds,
    is_resolved,
    is_assigned,
    has_milestone,
    has_labels,
    bronze_ingestion_timestamp,
    current_timestamp() as silver_ingestion_timestamp
FROM pr_enhanced;
