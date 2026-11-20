from pyspark.sql import Window, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_utils import add_metadata, create_spark, raw_path, result_path, run_info


EVENT_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", LongType()),
    StructField("user_fio", StringType()),
    StructField("user_email", StringType()),
    StructField("user_login", StringType()),
])

PROFILE = ["user_fio", "user_email", "user_login"]


def read_events(spark, dataset):
    return (
        spark.read.option("header", True)
        .schema(EVENT_SCHEMA)
        .csv(raw_path(dataset, "events"))
        .withColumn("event_time", F.to_timestamp("event_time"))
    )


def build_scd2(events, end):
    events = events.filter(F.col("event_time") < F.lit(end))
    by_time = Window.partitionBy("user_id").orderBy("event_time")

    marked = (
        events.withColumn("_state", F.struct(*PROFILE))
        .withColumn("_previous", F.lag("_state").over(by_time))
    )
    changed = marked.filter(
        F.col("_previous").isNull()
        | ~F.col("_state").eqNullSafe(F.col("_previous"))
    )

    versions = changed.withColumn("valid_from", F.col("event_time"))
    by_version = Window.partitionBy("user_id").orderBy("valid_from")

    return (
        versions.withColumn("valid_to", F.lead("valid_from").over(by_version))
        .withColumn("is_current", F.col("valid_to").isNull())
        .select("user_id", *PROFILE, "valid_from", "valid_to", "is_current")
    )


def run(context):
    info = run_info(context)
    dataset = "demo"
    spark = create_spark("hw23-build-user-scd2")
    try:
        events = read_events(spark, dataset)
        source = events.agg(
            F.count("*").alias("rows"),
            F.min("event_time").alias("first_event"),
            F.max("event_time").alias("last_event"),
        ).first()
        print(
            f"Source {dataset}: rows={source.rows}, "
            f"range=[{source.first_event}, {source.last_event}]",
            flush=True,
        )
        result = build_scd2(events, info["end"])
        result = result.withColumn("snapshot_date", F.lit(info["date"]).cast("date"))
        result = add_metadata(result, info)

        path = result_path(dataset, "user_scd2", info["date"])
        result.write.mode("overwrite").parquet(path)
        print(f"SCD2: {info['date']} | rows={spark.read.parquet(path).count()} | {path}", flush=True)
    finally:
        spark.stop()
