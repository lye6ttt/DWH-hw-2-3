from datetime import datetime, timedelta, timezone

from build_campaign_recipients import build_recipients, read_campaigns
from build_user_scd2 import build_scd2, read_events
from spark_utils import create_spark


EXPECTED = {
    "2026-01-23": {(101, 1001), (102, 1002), (103, 1001)},
    "2026-01-24": {(105, 1001), (105, 1002), (106, 1003)},
    "2026-01-25": {(108, 1002), (109, 1001), (110, 1004),
                   (115, 1001), (116, 1001)},
    "2026-01-27": set(),
}
EXPECTED_VERSIONS = {"2026-01-23": 3, "2026-01-24": 6, "2026-01-25": 10}


def main():
    spark = create_spark("hw23-check-fixtures")
    try:
        events = read_events(spark, "fixtures")
        campaigns = read_campaigns(spark, "fixtures")

        empty = build_scd2(events, datetime(2026, 1, 20, tzinfo=timezone.utc))
        assert empty.count() == 0, "До первого события SCD2 должен быть пуст"

        for day, expected in EXPECTED.items():
            start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            scd2 = build_scd2(events, start + timedelta(days=1))
            recipients = build_recipients(campaigns, scd2, start, start + timedelta(days=1))
            actual = {(row.campaign_id, row.user_id) for row in
                      recipients.select("campaign_id", "user_id").collect()}
            assert actual == expected, f"{day}: ожидалось {expected}, получили {actual}"
            if day in EXPECTED_VERSIONS:
                assert scd2.count() == EXPECTED_VERSIONS[day], f"{day}: версии SCD2"
            print(f"{day}: {scd2.count()} SCD2 versions, {len(actual)} recipients: {sorted(actual)}")

            if day == "2026-01-25":
                returned = recipients.filter("campaign_id = 116").first()
                assert returned.user_login == "alice", "Не обработан возврат A-B-A"
                uppercase = recipients.filter("campaign_id = 115").first()
                assert uppercase.email_domain == "MAIL.EXAMPLE", "Изменён регистр исходных данных"

        print("OK: границы времени, смены домена, A-B-A и пустые результаты", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
