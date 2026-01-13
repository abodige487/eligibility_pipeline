# transforms.py

from pyspark.sql.functions import (
    col,
    lower,
    initcap,
    regexp_replace,
    lit,
    try_to_date,
    coalesce,
    when
)
from pyspark.sql.types import StringType


def apply_column_mapping(df, column_mapping):
    """
    Rename partner-specific columns to canonical field names
    using configuration-driven mappings.
    """
    for src, tgt in column_mapping.items():
        df = df.withColumnRenamed(src, tgt)
    return df


def standardize_fields(df, partner_code):
    """
    Apply standard transformations required by the pipeline.

    BONUS:
    - Gracefully handle incorrect or mixed date formats
      using try_to_date (invalid values become NULL).
    """

    # Standardize first and last names to Title Case
    df = (
        df
        .withColumn("first_name", initcap(col("first_name")))
        .withColumn("last_name", initcap(col("last_name")))
    )

    # BONUS: Robust DOB parsing (supports multiple formats without failing job)
    df = df.withColumn(
        "dob",
        coalesce(
            try_to_date(col("dob"), "MM/dd/yyyy"),
            try_to_date(col("dob"), "yyyy-MM-dd")
        )
    )

    # Normalize email to lowercase
    df = df.withColumn("email", lower(col("email")))

    # Normalize phone number to XXX-XXX-XXXX when valid
    df = df.withColumn(
        "phone_digits",
        regexp_replace(col("phone"), r"\D", "")
    )

    df = df.withColumn(
        "phone",
        when(
            col("phone_digits").rlike(r"^\d{10}$"),
            regexp_replace(
                col("phone_digits"),
                r"(\d{3})(\d{3})(\d{4})",
                r"$1-$2-$3"
            )
        ).otherwise(None)
    ).drop("phone_digits")

    # Add partner identifier for downstream traceability
    df = df.withColumn("partner_code", lit(partner_code))

    return df


def validate_records(df):
    """
    BONUS:
    - Ensure external_id is present
    - Drop records where external_id is missing
    """
    return df.filter(col("external_id").isNotNull())


def enforce_final_schema(df, final_columns):
    """
    Enforce canonical output schema:
    - Add missing columns as NULL
    - Drop extra columns
    - Preserve deterministic column order
    """
    for c in final_columns:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))
    return df.select(final_columns)
