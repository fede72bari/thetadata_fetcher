import os
import time
import requests
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from options.fetch_options import FetchOptions



class ThetaDataFetcher:
    BASE_URL = "http://127.0.0.1:25510"
    TERMINAL_JAR_PATH = "D:\\Dropbox\\TRADING\\ThetaTerminal.jar"

    def __init__(self, 
                 username, 
                 password, 
                 symbol, 
                 options_dir="options_data", 
                 stock_dir="stock_data", 
                 index_dir="index_data",
                 BASE_URL = "http://127.0.0.1:25510",
                 TERMINAL_JAR_PATH = "D:\\Dropbox\\TRADING\\ThetaTerminal.jar"
                ):
        self.username = username
        self.password = password
        self.symbol = symbol
        self.options_data_dir = options_dir
        self.stock_data_dir = stock_dir
        self.index_data_dir = index_dir
        self.BASE_URL = BASE_URL
        self.TERMINAL_JAR_PATH = TERMINAL_JAR_PATH
        
        self.options_fetcher = FetchOptions(symbol="SPY", options_data_dir="data/options", base_url=self.BASE_URL)


        os.makedirs(self.options_data_dir, exist_ok=True)
        os.makedirs(self.stock_data_dir, exist_ok=True)
        os.makedirs(self.index_data_dir, exist_ok=True)

        self.JAVA_PATH = self.find_java_executable()

        if not self.check_terminal_connection():
            print("Theta Terminal non è attivo. Tentativo di avvio...")
            self.start_terminal()
            time.sleep(20)
            if not self.check_terminal_connection():
                raise ConnectionError("Impossibile avviare Theta Terminal. Controlla i log.")

    def find_java_executable(self):
        """Trova il percorso dell'eseguibile Java installato."""
        try:
            output = subprocess.check_output(["where", "java"], shell=True, text=True).strip()
            java_paths = output.split("\n")
            for path in java_paths:
                try:
                    version_output = subprocess.check_output([path, "-version"], stderr=subprocess.STDOUT, text=True)
                    if "version \"22" in version_output:
                        print(f"Usando Java: {path}")
                        return path
                except subprocess.CalledProcessError:
                    continue
        except Exception as e:
            print(f"Errore nel trovare Java: {e}")
        raise RuntimeError("Impossibile trovare un'installazione Java 22 valida.")

    def start_terminal(self):
        """Avvia Theta Terminal in background."""
        try:
            creation_flags = subprocess.DETACHED_PROCESS if os.name == "nt" else 0
            subprocess.Popen(
                [self.JAVA_PATH, "-jar", self.TERMINAL_JAR_PATH, self.username, self.password],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=creation_flags
            )
            print("✅ Theta Terminal avviato. Attendi qualche secondo per la connessione...")
        except Exception as e:
            print(f"❌ Errore nell'avvio di Theta Terminal: {e}")

    def check_terminal_connection(self):
        """Verifica se Theta Terminal è in esecuzione e raggiungibile."""
        try:
            response = requests.get(f"{self.BASE_URL}/v2/list/roots/option", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
        
    def list_roots_option(self):
        """Recupera la lista dei simboli root delle opzioni disponibili."""
        try:
            response = requests.get(f"{self.BASE_URL}/v2/list/roots/option", timeout=5)
            response.raise_for_status()
            return response.json().get("response", [])
        except requests.exceptions.RequestException as e:
            print(f"Errore nella richiesta: {e}")
            return None

    def update_data(self, selected_timeframes):
        """Aggiorna i dati di opzioni, azioni e indici per i timeframe selezionati."""
        first_date, last_date = self.get_available_dates()

        for timeframe in selected_timeframes:
            print(f"🔄 Aggiornamento per il timeframe: {timeframe}")
            is_intraday = timeframe != "daily"
            interval_ms = self.get_interval_ms(timeframe) if is_intraday else None

            option_file = os.path.join(self.options_data_dir, self.generate_file_name(self.symbol, "options", timeframe))
            missing_option_dates = self.get_missing_dates(option_file, first_date, last_date, is_intraday)

            if not missing_option_dates.empty:
                if is_intraday:
                    new_options = self.fetch_intraday_option_data(first_date, last_date, interval_ms)
                else:
                    new_options = self.options_fetcher.fetch_daily_option_data(first_date, last_date)

                greeks_file = os.path.join(self.options_data_dir, self.generate_file_name(self.symbol, "greeks", timeframe))
                oi_file = os.path.join(self.options_data_dir, self.generate_file_name(self.symbol, "open_interest", timeframe))

                missing_greeks = self.get_missing_dates(greeks_file, first_date, last_date, is_intraday)
                missing_oi = self.get_missing_dates(oi_file, first_date, last_date, is_intraday)

                if missing_greeks:
                    new_greeks = self.fetch_option_greeks_intraday(first_date, last_date, interval_ms) if is_intraday else self.fetch_option_greeks_eod(first_date, last_date)
                    new_options = self.merge_option_data(new_options, new_greeks)

                if missing_oi:
                    new_oi = self.fetch_option_open_interest(first_date, last_date)
                    new_options = self.merge_option_data(new_options, new_oi)

                self.merge_downloaded_data(option_file, new_options)

        print("✅ Aggiornamento dati completato.")

    def get_available_dates(self):
        """Recupera l'intervallo di date disponibili per il simbolo selezionato."""
        try:
            response = requests.get(f"{self.BASE_URL}/v2/list/dates/stock/quote", params={"root": self.symbol})
            response.raise_for_status()
            data = response.json()

            if "response" in data and isinstance(data["response"], list) and len(data["response"]) > 0:
                dates = sorted(pd.to_datetime([str(d) for d in data["response"]], format="%Y%m%d"))
                return dates[0], dates[-1]
            else:
                raise ValueError("❌ Nessuna data disponibile nella risposta dell'API.")

        except requests.exceptions.RequestException as e:
            print(f"❌ Errore nel recupero delle date disponibili: {e}")
            raise
            
            
    def generate_file_name(self, symbol, data_type, timeframe):
        """
        Genera il nome del file nel formato corretto per salvarlo nella cartella giusta.
        Esempio: SPY_options_1minute.parquet, AAPL_stock_daily.parquet
        """
        return f"{symbol}_{data_type}_{timeframe}.parquet"



    def get_missing_dates(self, file_path, first_date, last_date, is_intraday=False):
        """Verifica quali date o minuti mancano nel file locale."""
        if not os.path.exists(file_path):
            return pd.date_range(first_date, last_date, freq="1D" if not is_intraday else "T")

        df = pd.read_parquet(file_path)
        if df.empty:
            return pd.date_range(first_date, last_date, freq="1D" if not is_intraday else "T")

        if is_intraday:
            existing_timestamps = set(pd.to_datetime(df["timestamp"]))
            all_timestamps = set(pd.date_range(first_date, last_date, freq="T"))
            return sorted(all_timestamps - existing_timestamps)

        existing_dates = set(pd.to_datetime(df["date"]).dt.date)
        all_dates = set(pd.date_range(first_date, last_date).date)
        return sorted(all_dates - existing_dates)
