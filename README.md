# Data Engineering Remote Job Market Data Pipeline

## Overview

Data engineering pipeline (using polars) that collects remote job postings, stores the raw data, cleans it, transforms it, and displays the results in a streamlit dashboard.


The goal is to understand what skills, companies, and job categories appear most often in remote job postings.

The project uses:

- Python to run the pipeline
- Requests to get job data from an API
- DuckDB to store the data locally
- Polars to clean and transform the data
- BeautifulSoup to clean HTML from job descriptions
- Streamlit to build the dashboard

---

## Project Goal

This project answers questions like:

- What technical skills appear most often in remote job postings?
- Which companies are hiring the most?
- Which job categories have the most postings?
- How many job postings are being published over time?
- What job titles and descriptions mention tools like Python, SQL, AWS, Docker, or dbt?

---
## Applied Data Transformations
**Reads raw data:**
- Pulls the raw_jobs table from DuckDB.
- Loads it into Polars so the data can be cleaned and transformed.

**Cleans job descriptions:**
- Some job descriptions contain HTML.
- BeautifulSoup removes the HTML tags.
- The script also removes extra spaces and line breaks.

**Cleans job fields:**
- Job titles are trimmed and converted to lowercase.
- Company names are trimmed.
- Missing locations are replaced with Not specified.
- Extra spaces in locations are cleaned.
- Publication dates are converted from text into real date/time values.

**Creates a cleaned job table:**
- The script creates stg_jobs.
- This is the cleaned version of the raw job postings.
- It keeps only the useful columns needed for analysis and the dashboard.

**Extracts skills from job descriptions:**
- The script scans each cleaned job description.
- It checks for skills from the SKILLS list, such as Python, SQL, AWS, Docker, dbt, and others.
- It creates a list of skills found in each job posting.

**Creates a job-to-skill table:**
- The script creates `job_skill_bridge`.
- This table connects each job posting to the skills found in its description.
- One job can appear multiple times if it mentions multiple skills.

**Creates summary tables:**
- `mart_top_skills`: counts how often each skill appears.
- `mart_jobs_by_category`: counts jobs by category.
- `mart_jobs_by_company`: counts jobs by company.
- `mart_jobs_by_day`: counts job postings by publication date.

**Runs quality checks:**
- Makes sure the cleaned jobs table is not empty.
- Checks that job IDs are not missing.
- Checks that job IDs are unique.
- Checks that job titles are not missing.
- Warns if no skills were found.

**Saves final tables:**
- Writes the cleaned table, skill table, and summary tables back into DuckDB.
- These final tables are what the Streamlit dashboard uses.

---
## How to run the project locally

1. Install dependencies

```code
pip install -r requirements.txt
```

2. Run the pipeline
```code
python src/run_pipeline.py
```

3. Run the dashboard
```code
streamlit run dashboard/app.py
```
---

## Data Source

The data comes from the Remotive public jobs API:

```text
https://remotive.com/api/remote-jobs
```
