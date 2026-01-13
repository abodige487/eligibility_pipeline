# Healthcare Eligibility Pipeline (PySpark)

A small, configuration-driven data engineering pipeline built using PySpark.
The pipeline ingests eligibility files from multiple healthcare partners, standardizes
their schemas, and produces a single unified output dataset.

The design ensures that onboarding a new partner requires only configuration changes
and no changes to core processing logic.


## Project Structure

eligibility_pipeline/
├─ pipeline.py          - Main pipeline entry point
├─ transforms.py        - Reusable transformations and validations
├─ config.py            - Partner-specific configuration
├─ data/                - Input partner eligibility files
├─ output/              - Final unified output
└─ README.md


## Problem Statement

Healthcare partners provide eligibility files with:
- Different delimiters (e.g. |, ,, ^)
- Different column names
- Different date and phone formats

The goal of this project is to:
- Ingest eligibility data from multiple partners
- Standardize partner-specific schemas into a single canonical schema
- Output one unified dataset for downstream consumption

---

## Configuration-Driven Design

All partner-specific details are defined in config.py, including:
- Input file name
- File delimiter
- Column mappings (partner column → standard column)
- Partner code

Adding a new partner requires:
- Placing the partner file in the data/ directory
- Adding a new configuration entry in config.py

No changes are required in:
- pipeline.py
- transforms.py

This satisfies the assessment requirement for configuration-driven ingestion.

---

## Standard Output Schema

The pipeline always produces the following schema:

- external_id
- first_name
- last_name
- dob
- email
- phone
- partner_code

This schema is enforced regardless of input format.

---

## Transformations and Validations

The pipeline applies the following transformations:
- First and last names are converted to Title Case
- Email addresses are lowercased
- Dates of birth are standardized to YYYY-MM-DD
- Phone numbers are normalized to XXX-XXX-XXXX when valid
- Partner code is added for traceability

Bonus features implemented:
- Records missing external_id are dropped
- Malformed or incorrect date formats are handled gracefully
- Invalid data does not cause the pipeline to fail

---

## Prerequisites

- Python 3.9 or higher
- Apache Spark installed locally
- PySpark available in the Python environment
- Git (optional)

---

## Create and Activate a Virtual Environment

python -m venv venv
venv/Scripts/activate

---

## Install Dependencies

pip install pyspark

---

## Run the Pipeline

From the project directory:

spark-submit pipeline.py

---

## Output

After successful execution, the unified eligibility file will be created at:

output/eligibility_final.csv

---

## Summary

This project demonstrates:
- Configuration-driven data ingestion
- Clean separation of concerns
- Robust handling of real-world data inconsistencies
- A scalable and maintainable PySpark pipeline

