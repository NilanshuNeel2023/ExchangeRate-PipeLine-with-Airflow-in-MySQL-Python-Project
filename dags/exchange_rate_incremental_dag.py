from __future__ import annotations 

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

sys.path.append(str(Path(__file__).resolve().parent.parent/ "scripts"))

from exchange_rate_client import ExchangeRateClient

PIPELINE_NAME = 'exchange_rate_incremental'

MYSQL_CONN_ID = "mysql_exchange"

with DAG(
    dag_id = "exchange_rate_incremental_load",
    description = "load new fx rates from exchange API into SQl once a day",
    schedule = "0 10 * * *",
    start_date = pendulum.datetime(2026,1,1, tz= "Asia/Mumbai"),
    catchup = False,
    max_active_runs = 1,
    default_args = {
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    tags = ["Exchange_Rates", "incremental", "sql"],
) as dag:
    
    @task
    def fetch_rates():
        api_key = Variable.get("exchangerate_api_key")
        base_currency = Variable.get("exchangerate_base_currency", default_var = "USD")

        client = ExchangeRateClient(api_key=api_key, base_currency=base_currency)
        payload = client.fetch_latest()

        return payload
    
    @task
    def build_dataframe(payload):
        df = ExchangeRateClient.to_dataframe(payload)
        df = ExchangeRateClient.clean_dataframe(df)

        if df.empty:
            return[]

        df = df.copy()

        df["rate_date"] = df["rate_date"].astype(str)
        df["time_last_time_utc"] = df["time_last_time_utc"].astype(str)

        return df.to_dict(orient="records")

    @task.short_circuit
    def check_for_new_data(records):
        if not records:
            return False

        base_currency = records[0]["base_currency"]
        api_timestamp = int(records[0]["time_last_update_unix"])

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        result = hook.get_first(
            """ Select last_loaded_unix
            from Exchange_Rates.pipeline_watermark
            where PIPELINE_NAME = %s 
            AND base_currency = %s
            """,
            parameters = (PIPELINE_NAME, base_currency),
        )

        last_saved_timestamp = result[0] if result else None

        if last_saved_timestamp is None:
            return True

        return api_timestamp > last_saved_timestamp

    @task
    def load_rates(records):
        if not records:
            raise AirflowSkipException("nothing to load")

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        connection = hook.get_conn()

        try:
            with connection.cursor() as cursor:
                 cursor.executemany(
                    """
                    INSERT INTO FOREIGN_RATES.EXCHANGE_RATES(
                        base_currency, target_currency, rate, rate_date,
                        time_last_update_unix, time_last_update_utc
                    )
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                    rate = VALUES(rate)
                    """,
                    [
                        (
                            row["base_currency"],
                            row["target_currency"],
                            row["rate"],
                            row["rate_date"],
                            row["time_last_update_unix"],
                            row["time_last_update_utc"],
                        )
                        for row in records
                    ],
                )
            connection.commit()
        finally:
            connection.close()
        return len(records)

    @task
    def update_watermark(records):
        if not records:
            raise AirflowSkipException ("Nothing to Update")
        
        base_currency = records[0]["base_currency"]
        api_timestamp = int(records[0]["time_last_update_unix"])
        
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        hook.run(
            """ INSERT INTO FOREIGN_RATES.pipeline_watermark(
                pipeline_name, base_currency, last_loaded_unix, last_loaded_at, update_at)
                VALUES (%s, %s, %s, now(), NOW())
                ON DUPLICATE KEY UPDATE
                last_loaded_unix = VALUES(last_loaded_unix),
                last_loaded_at = VALUES(last_loaded_at),
                update_at = NOW()
                """,
                parameters = (PIPELINE_NAME, base_currency, api_timestamp),
                )

    payload = fetch_rates()
    parsed_records = build_dataframe(payload)

    has_new_data = check_for_new_data(
        parsed_records
    )

    loaded = load_rates(parsed_records)
    watermark = update_watermark(parsed_records)

    has_new_data >> loaded >> watermark