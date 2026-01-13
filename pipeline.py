import os
from functools import reduce
from pyspark.sql import SparkSession
from config import PARTNER_CONFIG, FINAL_COLUMNS
from transforms import (
    apply_column_mapping,
    standardize_fields,
    validate_records,
    enforce_final_schema
)


def create_spark_session():
    """Create Spark session for the eligibility pipeline."""
    return (
        SparkSession.builder
        .appName("HealthcareEligibilityPipeline")
        .getOrCreate()
    )


def read_partner_file(spark, file_path, partner_name):
    """
    Read partner file using delimiter defined in configuration.
    """
    cfg = PARTNER_CONFIG[partner_name]["file"]
    delimiter = cfg["delimiter"]

    # Spark CSV supports only single-character delimiters
    if len(delimiter) != 1:
        raise ValueError("Only single-character delimiters are supported")

    return (
        spark.read
        .option("header", str(cfg.get("header", True)).lower())
        .option("delimiter", delimiter)
        .csv(file_path)
    )


def process_partner(spark, data_dir, partner_name):
    """
    Process a single partner file using config-driven rules.
    """
    cfg = PARTNER_CONFIG[partner_name]

    df = read_partner_file(
        spark,
        os.path.join(data_dir, cfg["file_name"]),
        partner_name
    )

    # Apply partner-specific column mapping
    df = apply_column_mapping(df, cfg["column_mapping"])

    # Standardize fields and add partner code
    df = standardize_fields(df, cfg["partner_code"])

    # Validate required fields
    df = validate_records(df)

    # Enforce canonical output schema
    df = enforce_final_schema(df, FINAL_COLUMNS)

    return df


if __name__ == "__main__":
    # Resolve project-relative paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    OUTPUT_DIR = os.path.join(BASE_DIR, "output")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Process all partners defined in configuration
    partner_dfs = []
    for partner_name in PARTNER_CONFIG.keys():
        df = process_partner(spark, DATA_DIR, partner_name)
        partner_dfs.append(df)

    if not partner_dfs:
        raise RuntimeError("No partner data processed")

    # Union all partner datasets dynamically
    final_df = reduce(lambda d1, d2: d1.unionByName(d2), partner_dfs)

    final_df.show(truncate=False)
    print("Final row count:", final_df.count())

    output_path = os.path.join(OUTPUT_DIR, "eligibility_final.csv")

    final_df.toPandas().to_csv(output_path, index=False)

    print(f"Output written to {output_path}")

    spark.stop()
