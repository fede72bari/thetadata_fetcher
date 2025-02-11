import requests
import os
import pandas as pd

class FetchStock:
    BASE_URL = "http://127.0.0.1:25510"

    def __init__(self, symbol, stock_data_dir):
        self.symbol = symbol
        self.stock_data_dir = stock_data_dir
        os.makedirs(self.stock_data_dir, exist_ok=True)

    def fetch_stock_data_eod(self, start_date, end_date):
        """Fetches daily (EOD) stock data."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d")
        }
        response = requests.get(f"{self.base_url}/v2/hist/stock/eod", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    

    def fetch_stock_data_intraday(self, start_date, end_date, interval_ms):
        """Fetches intraday OHLC stock data."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "ivl": interval_ms
        }
        response = requests.get(f"{self.base_url}/v2/hist/stock/ohlc", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    
    
    def fetch_intraday_underlying_data(self, start_date, end_date, interval_ms):
        """Fetches intraday OHLC data for the underlying asset (stock or index)."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "ivl": interval_ms
        }
        response = requests.get(f"{self.BASE_URL}/v2/hist/stock/ohlc", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    
    
    def fetch_daily_underlying_data(self, start_date, end_date):
        """Fetches daily (EOD) data for the underlying asset (stock or index)."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d")
        }
        response = requests.get(f"{self.BASE_URL}/v2/hist/stock/eod", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None


