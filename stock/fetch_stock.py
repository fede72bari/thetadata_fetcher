import requests
import os
import pandas as pd

class FetchStock:
    BASE_URL = "http://127.0.0.1:25510"

    def __init__(self, symbol, stock_data_dir):
        self.symbol = symbol
        self.stock_data_dir = stock_data_dir
        os.makedirs(self.stock_data_dir, exist_ok=True)

    def fetch_daily_stock_data(self, start_date, end_date):
        """Scarica dati EOD per le azioni."""
        file_name = f"{self.symbol}_stock_eod.parquet"
        file_path = os.path.join(self.stock_data_dir, file_name)

        params = {"root": self.symbol, "start_date": start_date.strftime("%Y%m%d"), "end_date": end_date.strftime("%Y%m%d")}
        response = requests.get(f"{self.BASE_URL}/v2/hist/stock/eod", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        if data:
            df = pd.DataFrame(data)
            df.to_parquet(file_path, compression="zstd")
            print(f"Daily stock data saved to {file_path}")
        else:
            print("No daily stock data available.")

    def fetch_intraday_stock_data(self, start_date, end_date, interval_ms):
        """Scarica dati intraday per le azioni."""
        file_name = f"{self.symbol}_stock_intraday_{interval_ms}ms.parquet"
        file_path = os.path.join(self.stock_data_dir, file_name)

        params = {"root": self.symbol, "start_date": start_date.strftime("%Y%m%d"), "end_date": end_date.strftime("%Y%m%d"), "ivl": interval_ms, "rth": False}
        response = requests.get(f"{self.BASE_URL}/v2/hist/stock/quote", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        if data:
            df = pd.DataFrame(data)
            df.to_parquet(file_path, compression="zstd")
            print(f"Intraday stock data saved to {file_path}")
        else:
            print("No intraday stock data available.")
