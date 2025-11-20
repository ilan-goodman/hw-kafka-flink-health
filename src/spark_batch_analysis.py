import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="Path to alerts JSON files")
    parser.add_argument("--output_path", required=True, help="Path to write aggregated stats")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("HeartRateAlertBatchAnalysis")
        .getOrCreate()
    )

    # TODO: Implement:
    #  - read JSON alerts from args.input_path
    #  - group by patient_id and alert_type, count alerts
    #  - maybe compute time-based stats
    #  - write results to args.output_path in parquet or csv

    spark.stop()


if __name__ == "__main__":
    main()