-- Silver Layer: Dimension Table - Repositories
-- Creates a clean, deduplicated dimension table for repositories

CREATE OR REPLACE TEMPORARY VIEW bronze_repositories AS
SELECT * FROM parquet.`bronze/repositories`;

CREATE OR REPLACE TEMPORARY VIEW bronze_repo_details AS
SELECT * FROM parquet.`bronze/repo_details`;

-- Create dimension table for repositories with data quality improvements
CREATE OR REPLACE TABLE silver.dim_repositories AS
WITH repo_combined AS (
    -- Combine data from both repositories and repo_details tables
    SELECT 
        COALESCE(r.repo_id, rd.repo_id) as repo_id,
        COALESCE(r.repo_name, rd.repo_name) as repo_name,
        COALESCE(r.full_name, rd.full_name) as full_name,
        COALESCE(r.description, rd.description) as description,
        COALESCE(r.html_url, rd.html_url) as html_url,
        COALESCE(r.clone_url, rd.clone_url) as clone_url,
        COALESCE(r.language, rd.language) as language,
        COALESCE(r.stars, rd.stars) as stars,
        COALESCE(r.watchers, rd.watchers) as watchers,
        COALESCE(r.forks, rd.forks) as forks,
        COALESCE(r.open_issues, rd.open_issues) as open_issues,
        COALESCE(r.size, rd.size) as size,
        COALESCE(r.created_at, rd.created_at) as created_at,
        COALESCE(r.updated_at, rd.updated_at) as updated_at,
        COALESCE(r.pushed_at, rd.pushed_at) as pushed_at,
        COALESCE(r.default_branch, rd.default_branch) as default_branch,
        COALESCE(r.owner_id, rd.owner_id) as owner_id,
        COALESCE(r.owner_login, rd.owner_login) as owner_login,
        COALESCE(r.owner_type, rd.owner_type) as owner_type,
        COALESCE(r.owner_url, rd.owner_url) as owner_url,
        COALESCE(r.license_name, rd.license_name) as license_name,
        COALESCE(r.license_key, rd.license_key) as license_key,
        COALESCE(r.topics, rd.topics) as topics,
        COALESCE(r.archived, rd.archived) as archived,
        COALESCE(r.disabled, rd.disabled) as disabled,
        COALESCE(r.private, rd.private) as private,
        COALESCE(r.fork, rd.fork) as fork,
        GREATEST(
            COALESCE(r.bronze_ingestion_timestamp, '1900-01-01'),
            COALESCE(rd.bronze_ingestion_timestamp, '1900-01-01')
        ) as bronze_ingestion_timestamp
    FROM bronze_repositories r
    FULL OUTER JOIN bronze_repo_details rd 
        ON r.repo_id = rd.repo_id
),
repo_cleaned AS (
    SELECT 
        repo_id,
        repo_name,
        full_name,
        CASE 
            WHEN description IS NULL OR TRIM(description) = '' THEN 'No description available'
            ELSE TRIM(description)
        END as description,
        html_url,
        clone_url,
        CASE 
            WHEN language IS NULL THEN 'Unknown'
            ELSE language
        END as language,
        COALESCE(stars, 0) as stars,
        COALESCE(watchers, 0) as watchers,
        COALESCE(forks, 0) as forks,
        COALESCE(open_issues, 0) as open_issues,
        COALESCE(size, 0) as size,
        CASE 
            WHEN created_at IS NULL THEN '1900-01-01T00:00:00Z'
            ELSE created_at
        END as created_at,
        CASE 
            WHEN updated_at IS NULL THEN '1900-01-01T00:00:00Z'
            ELSE updated_at
        END as updated_at,
        CASE 
            WHEN pushed_at IS NULL THEN '1900-01-01T00:00:00Z'
            ELSE pushed_at
        END as pushed_at,
        COALESCE(default_branch, 'main') as default_branch,
        owner_id,
        owner_login,
        COALESCE(owner_type, 'User') as owner_type,
        owner_url,
        COALESCE(license_name, 'No License') as license_name,
        COALESCE(license_key, 'none') as license_key,
        topics,
        COALESCE(archived, false) as archived,
        COALESCE(disabled, false) as disabled,
        COALESCE(private, false) as private,
        COALESCE(fork, false) as fork,
        bronze_ingestion_timestamp,
        -- Add computed fields
        CASE 
            WHEN stars > 10000 THEN 'Very Popular'
            WHEN stars > 1000 THEN 'Popular'
            WHEN stars > 100 THEN 'Moderately Popular'
            ELSE 'Less Popular'
        END as popularity_category,
        CASE 
            WHEN language = 'Python' THEN 'Python'
            WHEN language = 'JavaScript' THEN 'JavaScript'
            WHEN language = 'Java' THEN 'Java'
            WHEN language = 'C++' THEN 'C++'
            WHEN language = 'C#' THEN 'C#'
            ELSE 'Other'
        END as language_category
    FROM repo_combined
    WHERE repo_id IS NOT NULL
)
SELECT 
    repo_id,
    repo_name,
    full_name,
    description,
    html_url,
    clone_url,
    language,
    language_category,
    stars,
    watchers,
    forks,
    open_issues,
    size,
    created_at,
    updated_at,
    pushed_at,
    default_branch,
    owner_id,
    owner_login,
    owner_type,
    owner_url,
    license_name,
    license_key,
    topics,
    archived,
    disabled,
    private,
    fork,
    popularity_category,
    bronze_ingestion_timestamp,
    current_timestamp() as silver_ingestion_timestamp
FROM repo_cleaned
WHERE repo_id IS NOT NULL;

