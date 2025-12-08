import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max


def main(input_path: str, output_path: str) -> None:
    """
    COVID-19 analytics with PySpark.

    Works with CSV data containing at least:
    - date
    - location
    - total_cases
    - new_cases
    - people_vaccinated (optional but used in sample)

    Produces:
    - top_cases: max total_cases per location
    - avg_cases: avg new_cases per date
    - vaccination: max people_vaccinated per location (if column exists)
    """

    spark = (
        SparkSession.builder
        .appName("CovidAnalysis")
        .getOrCreate()
    )

    # Read CSV (works with hdfs:// and file:// paths)
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # --- 1) Top countries by total cases ---
    top_cases = (
        df.groupBy("location")
          .agg(spark_max("total_cases").alias("max_cases"))
          .orderBy(col("max_cases").desc())
    )

    # --- 2) Global average new cases per day ---
    avg_cases = (
        df.groupBy("date")
          .agg(avg("new_cases").alias("avg_new_cases"))
          .orderBy("date")
    )

    # --- 3) Top countries by vaccination (if available) ---
    vaccination = None
    if "people_vaccinated" in df.columns:
        vaccination = (
            df.groupBy("location")
              .agg(spark_max("people_vaccinated").alias("max_vaccinated"))
              .orderBy(col("max_vaccinated").desc())
        )

    # --- Write outputs to output_path subdirectories ---
    top_cases.write.mode("overwrite").csv(f"{output_path}/top_cases")
    avg_cases.write.mode("overwrite").csv(f"{output_path}/avg_cases")

    if vaccination is not None:
        vaccination.write.mode("overwrite").csv(f"{output_path}/vaccination")

    # --- Print sample results (for screenshots / logs) ---
    print("\n=== Top Countries by Total Cases (sample) ===")
    top_cases.show(10, truncate=False)

    print("\n=== Average New Cases per Day (sample) ===")
    avg_cases.show(10, truncate=False)

    if vaccination is not None:
        print("\n=== Top Countries by People Vaccinated (sample) ===")
        vaccination.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="COVID-19 Analytics with PySpark")
    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV file path (hdfs:///... or file:///...)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory (hdfs:///... or file:///...)"
    )
    args = parser.parse_args()

    main(args.input, args.output)
