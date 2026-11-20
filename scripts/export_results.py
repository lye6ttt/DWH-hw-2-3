import os
from pathlib import Path

from upload_data import s3_client


DATES = ("2026-01-23", "2026-01-24", "2026-01-25")


def main():
    client = s3_client()
    bucket = os.environ.get("S3_BUCKET", "hw23")
    target = Path("/opt/airflow/exports")
    for table, field in (("user_scd2", "snapshot_date"),
                         ("campaign_recipients", "calculation_date")):
        for day in DATES:
            prefix = f"results/demo/{table}/{field}={day}/"
            found = 0
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    if not key.endswith(".parquet"):
                        continue
                    file = target / key
                    file.parent.mkdir(parents=True, exist_ok=True)
                    client.download_file(bucket, key, str(file))
                    found += 1
            if not found:
                raise RuntimeError(f"Не удалось скачать Parquet: {prefix}")
            print(f"Export: {prefix} ({found} files)", flush=True)
    print(f"OK: выгрузка готова в {target}", flush=True)


if __name__ == "__main__":
    main()
