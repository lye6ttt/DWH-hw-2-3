import os

from upload_data import s3_client


DATES = ("2026-01-23", "2026-01-24", "2026-01-25")


def list_keys(client, bucket, prefix):
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def main():
    client = s3_client()
    bucket = os.environ.get("S3_BUCKET", "hw23")
    original = "results/demo/"
    backup = "backups/demo/before_repeat/"
    if list_keys(client, bucket, backup):
        raise RuntimeError("Копия before_repeat уже существует. Перезаписывать её нельзя.")

    for day in DATES:
        for table, field in (("user_scd2", "snapshot_date"),
                             ("campaign_recipients", "calculation_date")):
            part = f"{table}/{field}={day}/"
            keys = list_keys(client, bucket, original + part)
            if not any(key.endswith(".parquet") for key in keys):
                raise RuntimeError(f"Нет Parquet в {original + part}")
            for key in keys:
                dest = backup + key[len(original):]
                client.copy({"Bucket": bucket, "Key": key}, bucket, dest)
            print(f"Backup: {part} ({len(keys)} objects)", flush=True)
    print("OK: все шесть партиций сохранены", flush=True)


if __name__ == "__main__":
    main()
