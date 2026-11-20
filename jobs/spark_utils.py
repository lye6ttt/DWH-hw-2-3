import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, functions as F


META_COLUMNS = [
    "_dag_id", "_run_id", "_task_id", "_data_interval_start",
    "_data_interval_end", "_loaded_at",
]


def create_spark(name):
    endpoint = os.environ["S3_ENDPOINT"]
    jars = "/opt/airflow/s3-jars/"

    spark = (
        SparkSession.builder
        .appName(name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars",
            jars + "hadoop-aws-3.3.4.jar," +
            jars + "aws-java-sdk-bundle-1.12.262.jar",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def s3_path(*parts):
    bucket = os.environ.get("S3_BUCKET", "hw23")
    return "s3a://" + bucket + "/" + "/".join(parts)


def raw_path(dataset, table):
    if table == "events":
        return s3_path("raw", dataset, "events", "*.csv")
    return s3_path("raw", dataset, "campaigns.csv")


def result_path(dataset, table, date):
    field = "snapshot_date" if table == "user_scd2" else "calculation_date"
    return s3_path("results", dataset, table, f"{field}={date}")


def run_info(context):
    start = context["data_interval_start"].in_timezone("UTC")
    end = context["data_interval_end"].in_timezone("UTC")
    if end - start != timedelta(days=1):
        raise ValueError("Ожидался интервал ровно в одни сутки")

    return {
        "date": start.date().isoformat(),
        "start": start,
        "end": end,
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "task_id": context["task"].task_id,
        "loaded_at": datetime.now(timezone.utc),
    }


def add_metadata(df, info):
    return (
        df.withColumn("_dag_id", F.lit(info["dag_id"]))
        .withColumn("_run_id", F.lit(info["run_id"]))
        .withColumn("_task_id", F.lit(info["task_id"]))
        .withColumn("_data_interval_start", F.lit(info["start"]).cast("timestamp"))
        .withColumn("_data_interval_end", F.lit(info["end"]).cast("timestamp"))
        .withColumn("_loaded_at", F.lit(info["loaded_at"]).cast("timestamp"))
    )
