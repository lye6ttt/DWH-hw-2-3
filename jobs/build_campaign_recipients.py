from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_utils import add_metadata, create_spark, raw_path, result_path, run_info


CAMPAIGN_SCHEMA = StructType([
    StructField("campaign_id", LongType()),
    StructField("email_domain", StringType()),
    StructField("send_at", StringType()),
    StructField("campaign_name", StringType()),
])

BUSINESS_COLUMNS = [
    "campaign_id", "campaign_name", "send_at", "email_domain",
    "user_id", "user_fio", "user_email", "user_login",
    "user_valid_from", "user_valid_to", "calculation_date",
]


def read_campaigns(spark, dataset):
    return (
        spark.read.option("header", True)
        .schema(CAMPAIGN_SCHEMA)
        .csv(raw_path(dataset, "campaigns"))
        .withColumn("send_at", F.to_timestamp("send_at"))
    )


def build_recipients(campaigns, scd2, start, end):
    campaigns = campaigns.filter(
        (F.col("send_at") >= F.lit(start))
        & (F.col("send_at") < F.lit(end))
    )

    c = F.broadcast(campaigns.alias("c"))
    s = scd2.alias("s")

    condition = (
        (F.lower(F.col("c.email_domain")) ==
         F.lower(F.split(F.col("s.user_email"), "@").getItem(1)))
        & (F.col("s.valid_from") <= F.col("c.send_at"))
        & (F.col("s.valid_to").isNull() | (F.col("c.send_at") < F.col("s.valid_to")))
    )

    return c.join(s, condition, "inner").select(
        F.col("c.campaign_id"),
        F.col("c.campaign_name"),
        F.col("c.send_at"),
        F.col("c.email_domain"),
        F.col("s.user_id"),
        F.col("s.user_fio"),
        F.col("s.user_email"),
        F.col("s.user_login"),
        F.col("s.valid_from").alias("user_valid_from"),
        F.col("s.valid_to").alias("user_valid_to"),
    )


def run(context):
    info = run_info(context)
    dataset = "demo"
    spark = create_spark("hw23-build-campaign-recipients")
    try:
        scd2 = spark.read.parquet(result_path(dataset, "user_scd2", info["date"]))
        campaigns = read_campaigns(spark, dataset)
        result = build_recipients(campaigns, scd2, info["start"], info["end"])

        print("План исторического JOIN:", flush=True)
        result.explain(mode="formatted")

        result = result.withColumn("calculation_date", F.lit(info["date"]).cast("date"))
        result = add_metadata(result, info)
        path = result_path(dataset, "campaign_recipients", info["date"])
        result.write.mode("overwrite").parquet(path)
        print(f"Recipients: {info['date']} | rows={spark.read.parquet(path).count()} | {path}", flush=True)
    finally:
        spark.stop()
