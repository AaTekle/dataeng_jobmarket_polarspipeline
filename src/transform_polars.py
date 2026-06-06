import re
import duckdb
import polars as pl
from bs4 import BeautifulSoup
from config import DB_PATH, SKILLS

# Removing HTML tags and normalize whitespace.
def clean_html(text: str | None) -> str:
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    cleaned = soup.get_text(separator=" ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return cleaned

'''
Scan job description text for known skills and return a deduplicated
list of matches. i.e., skill-demand analysis.
'''
def extract_skills(text: str | None) -> list[str]:
    if not text:
        return []

    text_lower = text.lower()
    found = []

    for skill in SKILLS:
        pattern = r"\b" + re.escape(skill.lower()) + r"\b"
        if re.search(pattern, text_lower):
            found.append(skill)

    return sorted(set(found))

# Loading raw job postings from DuckDB into a Polars DataFrame. (landing zone of the pipeline)
def read_raw_jobs() -> pl.DataFrame:
    with duckdb.connect(DB_PATH) as con:
        arrow_table = con.execute("SELECT * FROM raw_jobs").arrow()

    return pl.from_arrow(arrow_table)
'''
- Transform raw job data into a clean staging model by standardizing
- text fields, handling missing values, parsing dates, and removing HTML.
- The resulting table becomes the primary source for downstream marts.
'''
def build_staging_jobs(raw_jobs: pl.DataFrame) -> pl.DataFrame:
    return (
        raw_jobs
        .with_columns(
            [
                # Standardizing job titles to improve grouping and aggregation accuracy.
                pl.col("title")
                .str.strip_chars()
                .str.to_lowercase()
                .alias("job_title"),
                '''
                Clean company names to prevent duplicate entities caused by inconsistent whitespace formatting.
                '''
                pl.col("company_name")
                .str.strip_chars()
                .alias("company_name_clean"),
                # Normalizing location values and provide a default value for jobs. where the location was not supplied.
                pl.col("candidate_required_location")
                .fill_null("Not specified")
                .str.replace_all(r"\s+", " ")
                .alias("location"),
                # Converting job descriptions into plain text, making the textual information ready for text analysis and skill extraction.
                pl.col("description")
                .map_elements(clean_html, return_dtype=pl.String)
                .alias("clean_description"),
                # Parsing publication dates into a proper datetime type, so we can run the time-series analysis.
                pl.col("publication_date")
                .str.to_datetime(strict=False)
                .alias("publication_date_clean"),
            ]
        )
        .select(
            [
                "job_id",
                "url",
                "job_title",
                pl.col("company_name_clean").alias("company_name"),
                "category",
                "job_type",
                "location",
                "salary",
                pl.col("publication_date_clean").alias("publication_date"),
                "clean_description",
                "source",
                "ingested_at",
            ]
        )
    )

# Generating a many-to-many relationship between jobs and skills by extracting skill mentions from each job description
def build_job_skill_bridge(stg_jobs: pl.DataFrame) -> pl.DataFrame:
    jobs_with_skills = stg_jobs.with_columns(
        pl.col("clean_description")
        .map_elements(extract_skills, return_dtype=pl.List(pl.String))
        .alias("skills")
    )

    return (
        jobs_with_skills
        .select(["job_id", "skills"])
        # Expanding each list of skills into individual rows so every job-skill relationship can be analyzed independently
        .explode("skills")
        .rename({"skills": "skill"})
        .filter(pl.col("skill").is_not_null())
    )


def build_marts(
    stg_jobs: pl.DataFrame,
    job_skill_bridge: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    # aggregating job postings by skill, identifying the most frequently requested technologies across the dataset
    mart_top_skills = (
        job_skill_bridge
        .group_by("skill")
        .agg(pl.len().alias("job_count"))
        .sort("job_count", descending=True)
    )

    # Summarizing job demand by category to help analyze hiring distribution across functional areas.
    mart_jobs_by_category = (
        stg_jobs
        .group_by("category")
        .agg(pl.len().alias("job_count"))
        .sort("job_count", descending=True)
    )
    # measuring which companies are posting the largest number of jobs.
    mart_jobs_by_company = (
        stg_jobs
        .group_by("company_name")
        .agg(pl.len().alias("job_count"))
        .sort("job_count", descending=True)
    )
    # daily posting trend to analyze hiring activity over time
    mart_jobs_by_day = (
        stg_jobs
        .with_columns(
            pl.col("publication_date").dt.date().alias("posting_date")
        )
        .filter(pl.col("posting_date").is_not_null())
        .group_by("posting_date")
        .agg(pl.len().alias("job_count"))
        .sort("posting_date")
    )

    return {
        "mart_top_skills": mart_top_skills,
        "mart_jobs_by_category": mart_jobs_by_category,
        "mart_jobs_by_company": mart_jobs_by_company,
        "mart_jobs_by_day": mart_jobs_by_day,
    }

# Validating data quality assumptions before loading results into analytics tables
def run_quality_checks(
    stg_jobs: pl.DataFrame,
    job_skill_bridge: pl.DataFrame,
) -> None:
    if stg_jobs.is_empty():
        raise ValueError("stg_jobs is empty.")
    # Validating data quality assumptions before loading results into analytics tables
    if stg_jobs["job_id"].null_count() > 0:
        raise ValueError("stg_jobs contains null job_id values.")

    if stg_jobs["job_id"].n_unique() != stg_jobs.height:
        raise ValueError("stg_jobs contains duplicate job_id values.")

    if stg_jobs["job_title"].null_count() > 0:
        raise ValueError("stg_jobs contains null job_title values.")
    # Warn rather than fail because an empty bridge table may indicate a skills dictionary issue rather than a pipeline failure
    if job_skill_bridge.is_empty():
        print("Warning: job_skill_bridge is empty. Skill dictionary may be too narrow.")

# Persisting transformed data back to DuckDB, replacing any existing version of the target table
def write_table(table_name: str, df: pl.DataFrame) -> None:
    with duckdb.connect(DB_PATH) as con:
        con.register("df_to_write", df.to_arrow())
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM df_to_write
            """
        )
'''
Execute the end-to-end ETL workflow:
1. Read raw data
2. Build staging tables
3. Generate analytics marts
4. Run quality checks
5. Load outputs into DuckDB
'''
def main() -> None:
    raw_jobs = read_raw_jobs()

    if raw_jobs.is_empty():
        raise ValueError("raw_jobs is empty. Run ingestion first.")

    stg_jobs = build_staging_jobs(raw_jobs)
    job_skill_bridge = build_job_skill_bridge(stg_jobs)
    marts = build_marts(stg_jobs, job_skill_bridge)

    run_quality_checks(stg_jobs, job_skill_bridge)

    write_table("stg_jobs", stg_jobs)
    write_table("job_skill_bridge", job_skill_bridge)

    for table_name, df in marts.items():
        write_table(table_name, df)

    print("Polars transformation complete.")


if __name__ == "__main__":
    main()
