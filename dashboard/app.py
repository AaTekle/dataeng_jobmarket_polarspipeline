import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "jobs.duckdb"

st.set_page_config(
    page_title="Remote Job Market Dashboard",
    layout="wide",
)


@st.cache_data
def load_table(table_name: str) -> pd.DataFrame:
    with duckdb.connect(DB_PATH) as con:
        return con.execute(f"SELECT * FROM {table_name}").df()


def main() -> None:
    st.title("Remote Job Market Data Pipeline")
    st.caption("Built with Python, Polars, DuckDB, and Streamlit")

    if not DB_PATH.exists():
        st.error("Database not found. Run: python src/run_pipeline.py")
        sys.exit(1)

    jobs = load_table("stg_jobs")
    top_skills = load_table("mart_top_skills")
    jobs_by_category = load_table("mart_jobs_by_category")
    jobs_by_company = load_table("mart_jobs_by_company")
    jobs_by_day = load_table("mart_jobs_by_day")

    total_jobs = len(jobs)
    total_companies = jobs["company_name"].nunique()
    total_skills = top_skills["skill"].nunique()

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Jobs", f"{total_jobs:,}")
    col2.metric("Companies", f"{total_companies:,}")
    col3.metric("Tracked Skills", f"{total_skills:,}")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Top Technical Skills")
        st.bar_chart(top_skills.head(20), x="skill", y="job_count")

    with right:
        st.subheader("Jobs by Category")
        st.bar_chart(jobs_by_category.head(15), x="category", y="job_count")

    st.subheader("Job Postings Over Time")
    st.line_chart(jobs_by_day, x="posting_date", y="job_count")

    st.subheader("Top Hiring Companies")
    st.dataframe(jobs_by_company.head(25), use_container_width=True)

    st.subheader("Search Jobs")
    search_term = st.text_input("Search job title, company, or description")

    display_cols = [
        "job_title",
        "company_name",
        "category",
        "location",
        "publication_date",
        "url",
    ]

    if search_term:
        search_lower = search_term.lower()

        filtered = jobs[
            jobs["job_title"].str.lower().str.contains(search_lower, na=False)
            | jobs["company_name"].str.lower().str.contains(search_lower, na=False)
            | jobs["clean_description"].str.lower().str.contains(search_lower, na=False)
        ]

        st.dataframe(filtered[display_cols], use_container_width=True)
    else:
        st.dataframe(jobs[display_cols].head(50), use_container_width=True)


if __name__ == "__main__":
    main()