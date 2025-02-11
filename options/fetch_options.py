import os
import requests
import pandas as pd
from datetime import datetime

class FetchOptions:
    
    def __init__(self, symbol, options_data_dir, base_url="http://127.0.0.1:25510"):
        self.symbol = symbol
        self.options_data_dir = options_data_dir
        self.base_url = base_url  # ✅ Ora è un parametro della classe


    def fetch_expirations(self):
        """Recupera tutte le date di scadenza disponibili per il simbolo."""
        params = {"root": self.symbol}
        try:
            response = requests.get(f"{self.base_url}/v2/list/expirations", params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Errore nella richiesta: {e}")
            print(f"\tStatus Code: {response.status_code}")
            print(f"\tResponse Text: {response.text}")
            
        expirations = response.json().get("response", [])
        return expirations

    def fetch_strikes(self, expiration):
        """Recupera tutti i prezzi di esercizio disponibili per una data di scadenza specifica."""
        params = {"root": self.symbol, "exp": expiration}
        try: 
            response = requests.get(f"{self.base_url}/v2/list/strikes", params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Errore nella richiesta: {e}")
            print(f"\tStatus Code: {response.status_code}")
            print(f"\tResponse Text: {response.text}")
        
        
        strikes = response.json().get("response", [])
        return strikes
    
    
    def fetch_option_greeks_eod(self, start_date, end_date):
        """Fetches daily (EOD) option Greeks."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d")
        }
        response = requests.get(f"{self.base_url}/v2/bulk_hist/option/eod", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    
    
    def fetch_option_greeks_intraday(self, start_date, end_date, interval_ms):
        """Fetches intraday OHLC option Greeks."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "ivl": interval_ms
        }
        response = requests.get(f"{self.base_url}/v2/bulk_hist/option/ohlc", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    
    
    def fetch_option_open_interest_eod(self, start_date, end_date):
        """Fetches daily (EOD) option open interest."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d")
        }
        response = requests.get(f"{self.base_url}/v2/hist/option/open_interest", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    
    
    def fetch_option_open_interest_intraday(self, start_date, end_date, interval_ms):
        """Fetches intraday OHLC option open interest."""
        params = {
            "root": self.symbol,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "ivl": interval_ms
        }
        response = requests.get(f"{self.base_url}/v2/hist/option/open_interest", params=params)
        response.raise_for_status()

        data = response.json().get("response", [])
        return pd.DataFrame(data) if data else None
    
    
    

    def fetch_daily_option_data(self, start_date, end_date):
        """Scarica i dati EOD per le opzioni iterando su tutte le expiration e strike disponibili, evitando duplicati."""

        expirations = self.fetch_expirations()
        if not expirations:
            print("❌ Nessuna data di scadenza disponibile per questo simbolo.")
            return

        for exp in expirations:
            strikes = self.fetch_strikes(exp)
            if not strikes:
                print(f"⚠️ Nessun prezzo di esercizio disponibile per la scadenza {exp}.")
                continue

            for strike in strikes:
                for right in ['C', 'P']:
                    file_name = f"{self.symbol}_options_eod_{exp}_{strike}_{right}.parquet"
                    file_path = os.path.join(self.options_data_dir, file_name)

                    # **Verifica i dati esistenti**
                    if os.path.exists(file_path):
                        existing_df = pd.read_parquet(file_path)
                        existing_dates = set(pd.to_datetime(existing_df["date"]).dt.strftime("%Y%m%d"))
                    else:
                        existing_df = pd.DataFrame()
                        existing_dates = set()

                    # **Trova solo le date mancanti**
                    requested_dates = set(pd.date_range(start_date, end_date).strftime("%Y%m%d"))
                    missing_dates = sorted(requested_dates - existing_dates)

                    if not missing_dates:
                        print(f"✅ I dati per {exp}, {strike}, {right} sono già completi.")
                        continue

                    print(f"🔄 Scaricando dati per {exp}, {strike}, {right}, date mancanti: {len(missing_dates)}")

                    # **Scarica SOLO i dati mancanti**
                    for date in missing_dates:
                        params = {
                            "root": self.symbol,
                            "exp": exp,
                            "strike": strike,
                            "right": right,
                            "start_date": date,
                            "end_date": date  # Scarica solo un giorno alla volta
                        }
                        
                        try:
                            response = requests.get(f"{self.base_url}/v2/hist/option/eod", params=params)
                            response.raise_for_status()
                        except requests.exceptions.RequestException as e:
                            print(f"Errore nella richiesta: {e}")
                            print(f"\tStatus Code: {response.status_code}")
                            print(f"\tResponse Text: {response.text}")

                        data = response.json().get("response", [])
                        if data:
                            new_df = pd.DataFrame(data)
                            full_df = pd.concat([existing_df, new_df]).drop_duplicates().sort_values("date")
                            full_df.to_parquet(file_path, compression="zstd")
                            print(f"✅ Dati aggiornati salvati in {file_path}")
                        else:
                            print(f"⚠️ Nessun dato nuovo per {exp}, {strike}, {right}.")



    def fetch_intraday_option_data(self, start_date, end_date, interval_ms):
        """Scarica i dati intraday per le opzioni iterando su tutte le expiration e strike disponibili, evitando duplicati."""

        expirations = self.fetch_expirations()
        if not expirations:
            print("❌ Nessuna data di scadenza disponibile per questo simbolo.")
            return

        for exp in expirations:
            strikes = self.fetch_strikes(exp)
            if not strikes:
                print(f"⚠️ Nessun prezzo di esercizio disponibile per la scadenza {exp}.")
                continue

            for strike in strikes:
                for right in ['C', 'P']:
                    file_name = f"{self.symbol}_options_intraday_{exp}_{strike}_{right}_{interval_ms}ms.parquet"
                    file_path = os.path.join(self.options_data_dir, file_name)

                    # **Verifica i dati esistenti**
                    if os.path.exists(file_path):
                        existing_df = pd.read_parquet(file_path)
                        existing_timestamps = set(pd.to_datetime(existing_df["timestamp"]).strftime("%Y%m%d%H%M"))
                    else:
                        existing_df = pd.DataFrame()
                        existing_timestamps = set()

                    # **Trova solo i timestamp mancanti**
                    requested_timestamps = set(pd.date_range(start_date, end_date, freq=f"{interval_ms//60000}min").strftime("%Y%m%d%H%M"))
                    missing_timestamps = sorted(requested_timestamps - existing_timestamps)

                    if not missing_timestamps:
                        print(f"✅ I dati intraday per {exp}, {strike}, {right} sono già completi.")
                        continue

                    print(f"🔄 Scaricando dati intraday per {exp}, {strike}, {right}, date mancanti: {len(missing_timestamps)}")

                    # **Scarica SOLO i dati mancanti**
                    for timestamp in missing_timestamps:
                        params = {
                            "root": self.symbol,
                            "exp": exp,
                            "strike": strike,
                            "right": right,
                            "start_date": timestamp[:8],  # YYYYMMDD
                            "end_date": timestamp[:8],  # Scarica solo un giorno alla volta
                            "ivl": interval_ms
                        }
                        
                        try:
                            response = requests.get(f"{self.BASE_URL}/v2/hist/option/quote", params=params)
                            response.raise_for_status()
                        except requests.exceptions.RequestException as e:
                            print(f"Errore nella richiesta: {e}")
                            print(f"\tStatus Code: {response.status_code}")
                            print(f"\tResponse Text: {response.text}")

                        data = response.json().get("response", [])
                        if data:
                            new_df = pd.DataFrame(data)
                            full_df = pd.concat([existing_df, new_df]).drop_duplicates().sort_values("timestamp")
                            full_df.to_parquet(file_path, compression="zstd")
                            print(f"✅ Dati intraday aggiornati salvati in {file_path}")
                        else:
                            print(f"⚠️ Nessun dato nuovo per {exp}, {strike}, {right}.")
