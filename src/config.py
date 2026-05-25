from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "jobs.duckdb"

REMOTIVE_API_URL = "https://remotive.com/api/remote-jobs"

SKILLS = [
    "python",
    "sql",
    "excel",
    "tableau",
    "power bi",
    "aws",
    "azure",
    "gcp",
    "spark",
    "pyspark",
    "airflow",
    "dbt",
    "docker",
    "kubernetes",
    "pandas",
    "polars",
    "numpy",
    "scikit-learn",
    "machine learning",
    "data engineering",
    "etl",
    "elt",
    "snowflake",
    "bigquery",
    "redshift",
    "postgresql",
    "duckdb",
    "linux",
    "git",
]