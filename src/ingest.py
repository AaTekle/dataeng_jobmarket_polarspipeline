import json
from datetime import datetime, timezone

import duckdb
import polars as pl
import requests

from config import DATA_DIR, DB_PATH, REMOTIVE_API_URL


def fetch_jobs() -> list[dict]:
    response = requests.get(REMOTIVE_API_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()
    jobs = payload.get("jobs", [])

    if not jobs:
        raise ValueError("No jobs returned from the API.")

    return jobs


def normalize_jobs(jobs: list[dict]) -> pl.DataFrame:
    ingested_at = datetime.now(timezone.utc).isoformat()

    records = []

    for job in jobs:
        records.append(
            {
                "job_id": str(job.get("id")),
                "url": job.get("url"),
                "title": job.get("title"),
                "company_name": job.get("company_name"),
                "category": job.get("category"),
                "job_type": job.get("job_type"),
                "candidate_required_location": job.get("candidate_required_location"),
                "salary": job.get("salary"),
                "publication_date": job.get("publication_date"),
                "description": job.get("description"),
                "source": "remotive",
                "ingested_at": ingested_at,
                "raw_json": json.dumps(job),
            }
        )

    return pl.DataFrame(records)


def load_raw_jobs(df: pl.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(DB_PATH) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_jobs (
                job_id VARCHAR PRIMARY KEY,
                url VARCHAR,
                title VARCHAR,
                company_name VARCHAR,
                category VARCHAR,
                job_type VARCHAR,
                candidate_required_location VARCHAR,
                salary VARCHAR,
                publication_date VARCHAR,
                description VARCHAR,
                source VARCHAR,
                ingested_at VARCHAR,
                raw_json VARCHAR
            )
            """
        )

        con.register("incoming_jobs", df.to_arrow())

        con.execute(
            """
            INSERT OR REPLACE INTO raw_jobs
            SELECT *
            FROM incoming_jobs
            """
        )


def main() -> None:
    jobs = fetch_jobs()
    df = normalize_jobs(jobs)
    load_raw_jobs(df)

    print(f"Loaded {df.height} raw jobs into {DB_PATH}")


if __name__ == "__main__":
    main()