from __future__  import annotations

import pandas as pd
import requests

class ExchangeRateApiError(Exception):
    """We rise pthis if we get error"""

class ExchangeRateClient():
    BASE_URL = "https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}"

    def __init__(self, api_key, base_currency, timeout=15):
        self.api_key = api_key
        self.base_currency = base_currency.upper()
        self.timeout = timeout

    def fetch_latest(self):
        url = self.BASE_URL.format(api_key=self.api_key, base_currency=self.base_currency)

        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        payload = response.json()

        if payload.get('result') != "Success":
            error_type = payload.get("error-type","unknown_error")
            raise ExchangeRateApiError(f"API Return error{error_type}")

        return payload

    @staticmethod 
    def to_dataframe(payload):
        conversion_rates = payload["conversion_rates"]

        rows = list(conversion_rates.items())

        df = pd.DataFrame(rows, columns=["target_currency", "rate"])

        df['base_currency'] = payload['base_code']
        df['time_last_update_unix'] = payload['time_last_update_unix']
        df['time_last_update_utc'] = payload['time_last_update_utc']

        return df
    
    @staticmethod
    def clean_dataframe(df):
        df = df.copy()

        df['base_currency'] = df['base_currency'].str.upper().str.strip()
        df['target_currency'] =df['target_currency'].str.upper().str.strip()

        df['rate'] = pd.to_numeric(df['rate'], errors = 'coerce')

        df['time_last_update_unix'] = pd.to_numeric(df["time_last_update_unix"], errors='coerce')

        df['time_last_update_utc'] = pd.to_datetime(df["time_last_update_utc"], 
                                                    format = "%a, %d %b %Y %H:%M:%S %z",
                                                    utc = True, 
                                                    errors ="coerce")

        required_columns = ['rate', 'time_last_update_unix', 'time_last_update_utc', 'target_currency']

        df = df.dropna(subset = required_columns)

        df = df[df['rate'] > 0]

        df = df.drop_duplicates(['base_currency', 'target_currency', 'time_last_update_unix'])

        df['rate_date'] = df['time_last_update_utc'].dt.date

        final_columns = ['base_currency', 
                         'target_currency',
                         'rate',
                         'rate_date', 
                         'time_last_update_unix', 
                         'time_last_update_utc']

        df = df[final_columns]

        df = df.reset_index(drop = True)

        return df

    def fetch_clean_dataframe(self):
        payload = self.fetch_latest()
        df = self.to_dataframe(payload)
        df = self.clean_dataframe(df)

        return df