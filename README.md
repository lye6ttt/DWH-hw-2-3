# ДЗ 2.3 - SCD2 и исторический JOIN в Airflow

## 1. Структура новых файлов

| Файл | Для чего нужен |
| --- | --- |
| `dags/hw_2_3.py` | DAG с двумя зависимыми задачами и расписанием |
| `jobs/build_user_scd2.py` | SCD2 по всем событиям до конца суток |
| `jobs/build_campaign_recipients.py` | Исторический JOIN с рассылками |
| `jobs/spark_utils.py` | Подключение Spark к RustFS, пути и метаполя |
| `jobs/check_fixtures.py` | Проверка граничных случаев на маленьком наборе |
| `jobs/check_results.py` | Проверки трех результатов и повторного расчета |
| `scripts/backup_results.py` | Копирование шести партиций до повторного запуска |
| `scripts/export_results.py` | Выгрузка Parquet-файлов для сдачи |
| `report.md` | Краткий отчет |


## 2. Окружение

Нужны Docker Engine и Docker Compose v2, около 6–8 ГБ ОЗУ для Docker

В терминале из папки `hw_2_3`:

```bash
cp .env.example .env
docker compose up -d --build
```

Проверить работу Spark и S3:

```bash
docker compose run --rm --no-deps tools python /opt/airflow/scripts/check_environment.py
```

Ожидается строка `OK: Spark ..., Hadoop ..., S3A write/read, input data`

## 3. Запуски Airflow

Проверка импорта DAG:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

После появления `hw_2_3_scd2_campaigns` включить его в интерфейсе Airflow или командой:

```bash
docker compose exec airflow-scheduler airflow dags unpause hw_2_3_scd2_campaigns
```

DAG сам запланирует три historical scheduled run (`catchup=True`, `max_active_runs=1`):

| Дата D (UTC) | Интервал [начало, конец) |
| --- | --- |
| `2026-01-23` | `2026-01-23 00:00:00` — `2026-01-24 00:00:00` |
| `2026-01-24` | `2026-01-24 00:00:00` — `2026-01-25 00:00:00` |
| `2026-01-25` | `2026-01-25 00:00:00` — `2026-01-26 00:00:00` |

По условию для Airflow 2.10.5 используется `CronDataIntervalTimetable("0 0 * * *", timezone="UTC")`, `start_date=2026-01-23`, `end_date=2026-01-25` (последнее значение — логическая дата начала интервала, не конец суток). Каждая задача имеет одну автоматическую повторную попытку. В DAG явно проверяется длительность интервала — 24 часа

Посмотреть историю:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d hw_2_3_scd2_campaigns
```

Каждый из трех запусков должен быть `success`, обе задачи — зелеными. В логах `build_campaign_recipients` выводится физический план JOIN (ищем `BroadcastHashJoin`)

## 4. Результаты в RustFS

Источники:

```text
s3a://hw23/raw/demo/events/*.csv
s3a://hw23/raw/demo/campaigns.csv
```

Выходные папки:

```text
s3a://hw23/results/demo/user_scd2/snapshot_date=2026-01-23/
s3a://hw23/results/demo/user_scd2/snapshot_date=2026-01-24/
s3a://hw23/results/demo/user_scd2/snapshot_date=2026-01-25/

s3a://hw23/results/demo/campaign_recipients/calculation_date=2026-01-23/
s3a://hw23/results/demo/campaign_recipients/calculation_date=2026-01-24/
s3a://hw23/results/demo/campaign_recipients/calculation_date=2026-01-25/
```

Каждая папка — самостоятельный читаемый Parquet, включая пустой результат. В обеих таблицах есть шесть метаполей `_dag_id`, `_run_id`, `_task_id`, `_data_interval_start`, `_data_interval_end`, `_loaded_at`

## 5. Проверки

Когда все три DAG run завершатся успешно, запустить:

```bash
# Контрольные события: границы времени, возвраты A→B→A, регистр и пустые дни
docker compose run --rm --no-deps tools python /opt/airflow/jobs/check_fixtures.py

# Количества, уникальность, интервалы, полнота JOIN и метаполя всех дат
docker compose run --rm --no-deps tools python /opt/airflow/jobs/check_results.py
```

Скрипт проверяет не только количества, но и ключи, текущие версии, отсутствие лишних версий, соответствие временных интервалов, все бизнес-поля JOIN, схемы и метаполя

## 6. Проверка повторного выполнения

1. Убедиться, что три scheduled run закончились успешно, и сохранить результаты командой:

```bash
docker compose run --rm --no-deps tools python /opt/airflow/scripts/backup_results.py
```

Создаются копии всех шести папок в `s3a://hw23/backups/demo/before_repeat/`. Повторно копию скрипт не перезаписывает — это нужно для честного сравнения.

2. В Airflow открыть первый scheduled run (`2026-01-23`), выбрать обе задачи и выполнить Clear. Не включать параметры очистки других дат (`Past`/`Future`); не создавать новый ручной DAG run.

3. После повторного успешного выполнения запустить:

```bash
docker compose run --rm --no-deps tools python /opt/airflow/jobs/check_results.py --compare-backup
```

Сравнение сделано через `exceptAll` в обе стороны: для первого дня проверяются все бизнес-поля и поле даты, а для двух следующих дней вообще все поля, в том числе метаполя. При повторе того же DAG run `_run_id` должен сохраниться, `_loaded_at` может поменяться

## 7. Сохранение партиций

Выкачать все шесть Parquet-партиций на компьютер:

```bash
mkdir -p exports
docker compose run --rm --no-deps --user "$(id -u):0"" \
  -v "$(pwd)/exports:/opt/airflow/exports" \
  tools python /opt/airflow/scripts/export_results.py
```