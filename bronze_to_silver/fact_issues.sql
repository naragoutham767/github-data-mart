-- Silver Layer: Fact Table - Issues
-- Creates a fact table for issue activities with proper relationships

CREATE OR REPLACE TEMPORARY VIEW bronze_issues AS
SELECT * FROM parquet.`bronze/issues`;

CREATE OR REPLACE TEMPORARY VIEW bronze_repositories AS
SELECT * FROM parquet.`bronze/repositories`;

-- Create fact table for issues
CREATE OR REPLACE TABLE silver.fact_issues AS
WITH issues_cleaned AS (
    SELECT 
        issue_id,
        node_id,
        issue_number,
        title,
        body,
        state,
        locked,
        assignee_id,
        assignee_login,
        assignee_type,
        assignee_html_url,
        user_id,
        user_login,
        user_type,
        user_html_url,
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
        bronze_ingestion_timestamp,
        -- Extract repository information from issue URL
        regexp_extract(html_url, 'github\\.com/([^/]+)/([^/]+)/', 1) as repo_owner,
        regexp_extract(html_url, 'github\\.com/[^/]+/([^/]+)/', 1) as repo_name,
        -- Add computed fields
        CASE 
            WHEN length(title) > 100 THEN 'Long'
            WHEN length(title) > 50 THEN 'Medium'
            ELSE 'Short'
        END as title_length_category,
        CASE 
            WHEN length(body) > 1000 THEN 'Long'
            WHEN length(body) > 500 THEN 'Medium'
            WHEN length(body) > 0 THEN 'Short'
            ELSE 'Empty'
        END as body_length_category,
        CASE 
            WHEN state = 'open' THEN 'Open'
            WHEN state = 'closed' THEN 'Closed'
            ELSE 'Unknown'
        END as issue_status,
        CASE 
            WHEN assignee_id IS NOT NULL THEN 'Assigned'
            ELSE 'Unassigned'
        END as assignment_status,
        CASE 
            WHEN milestone_id IS NOT NULL THEN 'Has Milestone'
            ELSE 'No Milestone'
        END as milestone_status,
        CASE 
            WHEN pull_request IS NOT NULL THEN 'Pull Request'
            ELSE 'Issue'
        END as issue_type,
        CASE 
            WHEN created_at IS NOT NULL AND closed_at IS NOT NULL 
            THEN unix_timestamp(closed_at) - unix_timestamp(created_at)
            ELSE NULL
        END as resolution_time_seconds,
        CASE 
            WHEN comments_count > 20 THEN 'High Activity'
            WHEN comments_count > 10 THEN 'Medium Activity'
            WHEN comments_count > 0 THEN 'Low Activity'
            ELSE 'No Activity'
        END as activity_level
    FROM bronze_issues
    WHERE issue_id IS NOT NULL
),
issues_with_repo AS (
    SELECT 
        i.*,
        r.repo_id,
        r.repo_name,
        r.full_name,
        r.language,
        r.stars,
        r.forks,
        r.open_issues
    FROM issues_cleaned i
    LEFT JOIN bronze_repositories r 
        ON i.repo_owner = r.owner_login 
        AND i.repo_name = r.repo_name
)
SELECT 
    issue_id,
    node_id,
    issue_number,
    title,
    body,
    state,
    locked,
    assignee_id,
    assignee_login,
    assignee_type,
    assignee_html_url,
    user_id,
    user_login,
    user_type,
    user_html_url,
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
    full_name as repo_full_name,
    language as repo_language,
    stars as repo_stars,
    forks as repo_forks,
    open_issues as repo_open_issues,
    title_length_category,
    body_length_category,
    issue_status,
    assignment_status,
    milestone_status,
    issue_type,
    resolution_time_seconds,
    activity_level,
    bronze_ingestion_timestamp,
    current_timestamp() as silver_ingestion_timestamp
FROM issues_with_repo;

