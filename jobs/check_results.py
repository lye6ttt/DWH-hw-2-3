import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import Window, functions as F
from pyspark.sql.types import BooleanType, DateType, LongType, StringType, TimestampType

from build_campaign_recipients import BUSINESS_COLUMNS, build_recipients, read_campaigns
from build_user_scd2 import PROFILE
from spark_utils import META_COLUMNS, create_spark, result_path, s3_path


DATES = ("2026-01-23", "2026-01-24", "2026-01-25")
SCD_COLUMNS = ["user_id", *PROFILE, "valid_from", "valid_to", "is_current", "snapshot_date"]
SCHEMAS = {
    "user_scd2": {
        "user_id": LongType, "user_fio": StringType, "user_email": StringType,
        "user_login": StringType, "valid_from": TimestampType,
        "valid_to": TimestampType, "is_current": BooleanType, "snapshot_date": DateType,
    },
    "campaign_recipients": {
        "campaign_id": LongType, "campaign_name": StringType, "send_at": TimestampType,
        "email_domain": StringType, "user_id": LongType, "user_fio": StringType,
        "user_email": StringType, "user_login": StringType,
        "user_valid_from": TimestampType, "user_valid_to": TimestampType,
        "calculation_date": DateType,
    },
}


def check_empty(df, message):
    rows = df.limit(3).collect()
    if rows:
        raise AssertionError(f"{message}: {rows}")


def check_schema(df, table):
    expected = dict(SCHEMAS[table])
    expected.update({name: StringType for name in META_COLUMNS[:3]})
    expected.update({name: TimestampType for name in META_COLUMNS[3:]})
    if set(df.columns) != set(expected):
        raise AssertionError(f"Неверные поля {table}: {df.columns}")
    for field in df.schema:
        if not isinstance(field.dataType, expected[field.name]):
            raise AssertionError(f"Неверный тип {table}.{field.name}: {field.dataType}")


def check_metadata(df, table, day):
    check_schema(df, table)
    nullable = "valid_to" if table == "user_scd2" else "user_valid_to"
    null_fields = [field for field in df.columns if field != nullable]
    check_empty(
        df.filter(" OR ".join(f"`{field}` IS NULL" for field in null_fields)),
        f"NULL в обязательных полях {table}, {day}",
    )
    begin = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    end = begin + timedelta(days=1)
    date_field = "snapshot_date" if table == "user_scd2" else "calculation_date"
    task = "build_user_scd2" if table == "user_scd2" else "build_campaign_recipients"

    check_empty(df.filter(F.col(date_field) != F.lit(day).cast("date")), "Дата партиции")
    condition = F.col("_dag_id") != "hw_2_3_scd2_campaigns"
    condition = condition | (F.col("_task_id") != task)
    condition = condition | (F.col("_data_interval_start") != F.lit(begin))
    condition = condition | (F.col("_data_interval_end") != F.lit(end))
    for field in META_COLUMNS:
        condition = condition | F.col(field).isNull()
    check_empty(df.filter(condition), f"Метаполя {table}, {day}")

    groups = df.groupBy(*META_COLUMNS[:5]).agg(
        F.count("*").alias("rows"),
        F.min("_loaded_at").alias("min_loaded_at"),
        F.max("_loaded_at").alias("max_loaded_at"),
    )
    groups.show(truncate=False)
    if groups.count() > 1 or df.select("_loaded_at").distinct().count() > 1:
        raise AssertionError(f"В партиции {table}, {day} разные метаполя одного запуска")

    run_ids = [row[0] for row in df.select("_run_id").distinct().collect()]
    if run_ids and not run_ids[0].startswith("scheduled__"):
        raise AssertionError("Для основной сдачи нужны scheduled DAG runs")
    return run_ids[0] if run_ids else None


def check_scd2(df, day):
    check_empty(
        df.groupBy("user_id", "valid_from").count().filter("count > 1"),
        f"Дубли SCD2 {day}",
    )
    by_version = Window.partitionBy("user_id").orderBy("valid_from")
    checked = (
        df.withColumn("next_from", F.lead("valid_from").over(by_version))
        .withColumn("previous_state", F.lag(F.struct(*PROFILE)).over(by_version))
    )
    check_empty(
        checked.filter(~F.col("valid_to").eqNullSafe(F.col("next_from"))),
        f"Разрывы/пересечения SCD2 {day}",
    )
    check_empty(
        checked.filter(
            F.col("previous_state").isNotNull()
            & F.struct(*PROFILE).eqNullSafe(F.col("previous_state"))
        ),
        f"Лишняя версия при неизменном состоянии {day}",
    )
    check_empty(
        checked.filter(F.col("is_current") != F.col("valid_to").isNull()),
        f"Флаг is_current {day}",
    )
    end = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=1)
    check_empty(
        checked.filter(F.col("valid_from") >= F.lit(end)),
        f"Будущие события SCD2 {day}",
    )
    users = df.select("user_id").distinct().count()
    currents = df.filter("is_current").count()
    if users != currents:
        raise AssertionError(f"SCD2 {day}: {users} users, {currents} current")
    print(f"SCD2 {day}: rows={df.count()}, users={users}, current={currents}", flush=True)


def compare_business(actual, expected, columns, label):
    a = actual.select(*columns)
    b = expected.select(*columns)
    check_empty(a.exceptAll(b), f"{label}: лишние/изменённые строки")
    check_empty(b.exceptAll(a), f"{label}: пропущенные строки")


def check_recipients(df, scd2, campaigns, day):
    check_empty(
        df.groupBy("campaign_id", "user_id").count().filter("count > 1"),
        f"Дубли получателей {day}",
    )
    domain = F.lower(F.split(F.col("user_email"), "@").getItem(1))
    bad = (
        (domain != F.lower(F.col("email_domain")))
        | (F.col("send_at") < F.col("user_valid_from"))
        | (F.col("user_valid_to").isNotNull()
           & (F.col("send_at") >= F.col("user_valid_to")))
    )
    check_empty(df.filter(bad), f"Неверный исторический JOIN {day}")
    begin = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    expected = build_recipients(campaigns, scd2, begin, begin + timedelta(days=1))
    expected = expected.withColumn("calculation_date", F.lit(day).cast("date"))
    compare_business(df, expected, BUSINESS_COLUMNS, f"Полнота JOIN {day}")
    print(f"Recipients {day}: rows={df.count()}", flush=True)
    df.groupBy("campaign_id").agg(
        F.count("*").alias("rows"), F.countDistinct("user_id").alias("users")
    ).orderBy("campaign_id").show(30, truncate=False)


def backup_path(dataset, table, day):
    part = "snapshot_date" if table == "user_scd2" else "calculation_date"
    return s3_path("backups", dataset, "before_repeat", table, f"{part}={day}")


def check_backup(spark, dataset):
    for day in DATES:
        for table in ("user_scd2", "campaign_recipients"):
            before = spark.read.parquet(backup_path(dataset, table, day))
            after = spark.read.parquet(result_path(dataset, table, day))
            columns = SCHEMAS[table].keys()
            if day != DATES[0]:
                columns = [*columns, *META_COLUMNS]
            compare_business(after, before, list(columns), f"Повтор: {table}, {day}")
            print(f"Repeat OK: {table}, {day}, compare fields={len(columns)}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare-backup", action="store_true")
    args = parser.parse_args()
    spark = create_spark("hw23-check-results")
    dataset = "demo"
    try:
        campaigns = read_campaigns(spark, dataset)
        for day in DATES:
            print(f"\n========== {day} ==========", flush=True)
            scd2 = spark.read.parquet(result_path(dataset, "user_scd2", day))
            recipients = spark.read.parquet(result_path(dataset, "campaign_recipients", day))
            scd_run_id = check_metadata(scd2, "user_scd2", day)
            recipients_run_id = check_metadata(recipients, "campaign_recipients", day)
            if scd_run_id and recipients_run_id and scd_run_id != recipients_run_id:
                raise AssertionError(f"{day}: разные _run_id между задачами")
            check_scd2(scd2, day)
            check_recipients(recipients, scd2, campaigns, day)

        if args.compare_backup:
            print("\n========== Повторная обработка ==========")
            check_backup(spark, dataset)
        print("\nOK: все проверки прошли", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
