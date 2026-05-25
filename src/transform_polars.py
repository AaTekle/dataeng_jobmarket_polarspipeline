import re
import duckdb
import polars as pl
from bs4 import BeautifulSoup
from config import DB_PATH, SKILLS


def clean_html(text: str | None) -> str:
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    cleaned = soup.get_text(separator=" ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return cleaned


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


def read_raw_jobs() -> pl.DataFrame:
    with duckdb.connect(DB_PATH) as con:
        arrow_table = con.execute("SELECT * FROM raw_jobs").arrow()

    return pl.from_arrow(arrow_table)


def build_staging_jobs(raw_jobs: pl.DataFrame) -> pl.DataFrame:
    return (
        raw_jobs
        .with_columns(
            [
                pl.col("title")
                .str.strip_chars()
                .str.to_lowercase()
                .alias("job_title"),

                pl.col("company_name")
                .str.strip_chars()
                .alias("company_name_clean"),

                pl.col("candidate_required_location")
                .fill_null("Not specified")
                .str.replace_all(r"\s+", " ")
                .alias("location"),

                pl.col("description")
                .map_elements(clean_html, return_dtype=pl.String)
                .alias("clean_description"),

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


def build_job_skill_bridge(stg_jobs: pl.DataFrame) -> pl.DataFrame:
    jobs_with_skills = stg_jobs.with_columns(
        pl.col("clean_description")
        .map_elements(extract_skills, return_dtype=pl.List(pl.String))
        .alias("skills")
    )

    return (
        jobs_with_skills
        .select(["job_id", "skills"])
        .explode("skills")
        .rename({"skills": "skill"})
        .filter(pl.col("skill").is_not_null())
    )


def build_marts(
    stg_jobs: pl.DataFrame,
    job_skill_bridge: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    mart_top_skills = (
        job_skill_bridge
        .group_by("skill")
        .agg(pl.len().alias("job_count"))
        .sort("job_count", descending=True)
    )

    mart_jobs_by_category = (
        stg_jobs
        .group_by("category")
        .agg(pl.len().alias("job_count"))
        .sort("job_count", descending=True)
    )

    mart_jobs_by_company = (
        stg_jobs
        .group_by("company_name")
        .agg(pl.len().alias("job_count"))
        .sort("job_count", descending=True)
    )

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


def run_quality_checks(
    stg_jobs: pl.DataFrame,
    job_skill_bridge: pl.DataFrame,
) -> None:
    if stg_jobs.is_empty():
        raise ValueError("stg_jobs is empty.")

    if stg_jobs["job_id"].null_count() > 0:
        raise ValueError("stg_jobs contains null job_id values.")

    if stg_jobs["job_id"].n_unique() != stg_jobs.height:
        raise ValueError("stg_jobs contains duplicate job_id values.")

    if stg_jobs["job_title"].null_count() > 0:
        raise ValueError("stg_jobs contains null job_title values.")

    if job_skill_bridge.is_empty():
        print("Warning: job_skill_bridge is empty. Skill dictionary may be too narrow.")


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