import requests
import os
import pandas as pd

class FetchIndex:
    BASE_URL = "http://127.0.0.1:25510"

    def __init__(self, symbol, index_data_dir):
        self.symbol = symbol
        self.index_data_dir = index_data_dir
        os.makedirs(self.index_data_dir, exist_ok=True)

    def fetch_daily_index_data(self, start_date, end_date):
        """Scarica dati EOD per gli indici."""
        file_name = f"{self.symbol}_index_eod.parquet"
        file_path = os.path.join(self.index_data_dir, file_name)

        params = {"root": self.symbol, "start_date": start_date.strftime("%Y%m%d"), "end_date": end_date.strftime("%Y%m%d")}
        response = requests.get(f"{self.BASE_URL}/v2/hist/index/price", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        if data:
            df = pd.DataFrame(data)
            df.to_parquet(file_path, compression="zstd")
            print(f"Index EOD data saved to {file_path}")
        else:
            print("No index EOD data available.")
            
            
    def fetch_intraday_index_data(self, start_date, end_date, interval_ms):
        """Fetches intraday OHLC data for the index."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "ivl": interval_ms
        }
        response = requests.get(f"{self.BASE_URL}/v2/hist/index/ohlc", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None

