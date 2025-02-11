import os
import time
import requests
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from options.fetch_options import FetchOptions



class ThetaDataFetcher:

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
        
        # 🔹 **Recupera la lista degli stock e degli indici**
        self.stock_list = self.get_stock_list()
        self.index_list = self.get_index_list()

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
        
        
    def get_stock_list(self):
        """Fetches the list of available stock symbols from ThetaData."""
        try:
            response = requests.get(f"{self.BASE_URL}/v2/list/roots/stock")
            response.raise_for_status()
            data = response.json().get("response", [])
            return set(data) if data else set()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error retrieving stock list: {e}")
            return set()

    def get_index_list(self):
        """Fetches the list of available index symbols from ThetaData."""
        try:
            response = requests.get(f"{self.BASE_URL}/v2/list/roots/index")
            response.raise_for_status()
            data = response.json().get("response", [])
            return set(data) if data else set()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error retrieving index list: {e}")
            return set()
        
        
        
    def get_interval_ms(self, timeframe):
        """Converts the timeframe into milliseconds for API requests (only for intraday data)."""
        timeframe_map = {
            "1minute": 60000,
            "5minute": 300000,
            "15minute": 900000,
            "30minute": 1800000,
            "1hour": 3600000
        }
        if timeframe not in timeframe_map:
            raise ValueError(f"Invalid intraday timeframe: {timeframe}")
        return timeframe_map[timeframe]




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
            print(f"\tStatus Code: {response.status_code}")
            print(f"\tResponse Text: {response.text}")
            return None


    def update_data(self, selected_timeframes, recent_only=True):
        """
        Updates options, Greeks (including IV), Open Interest, and underlying data for selected timeframes.

        Args:
            selected_timeframes (list): List of timeframes to update.
            recent_only (bool): 
                - If True, downloads only from the last available date/timestamp in the file onward.
                - If False, downloads all available dates/timestamps and fetches only the missing ones.
        """
        current_date = datetime.utcnow()  # Get current date and time

        for timeframe in selected_timeframes:
            print(f"🔄 Updating for timeframe: {timeframe}")
            is_intraday = timeframe != "daily"

            # 🔹 **Determina se l'underlying è uno stock o un index**
            if self.symbol in self.stock_list:
                fetch_daily = self.fetch_daily_stock_data
                fetch_intraday = self.fetch_intraday_stock_data
                available_dates = self.get_available_dates(is_intraday, data_type="stock")
            elif self.symbol in self.index_list:
                fetch_daily = self.fetch_daily_index_data
                fetch_intraday = self.fetch_intraday_index_data
                available_dates = self.get_available_dates(is_intraday, data_type="index")
            else:
                raise ValueError(f"Symbol {self.symbol} not recognized as stock or index.")

            # 🔹 **Definizione dei file**
            option_file = os.path.join(self.options_data_dir, self.generate_file_name(self.symbol, "options", timeframe))
            greeks_file = os.path.join(self.options_data_dir, self.generate_file_name(self.symbol, "greeks", timeframe))
            underlying_file = os.path.join(self.options_data_dir, self.generate_file_name(self.symbol, "underlying", timeframe))

            # 🔹 **Trova le date mancanti**
            last_dates = {}
            if recent_only:
                last_dates["options"] = self.get_last_available_date(option_file, is_intraday)
                last_dates["greeks"] = self.get_last_available_date(greeks_file, is_intraday)
                last_dates["underlying"] = self.get_last_available_date(underlying_file, is_intraday)

                # 🔹 **Filtra solo le date successive all'ultima presente nel file**
                available_dates = {d for d in available_dates if any(d > last_dates[k] for k in last_dates if last_dates[k])}

            else:
                # 🔹 **Se recent_only=False, controlla quali date/orari sono effettivamente mancanti nel file**
                missing_options = self.get_missing_dates(option_file, available_dates, is_intraday)
                missing_greeks = self.get_missing_dates(greeks_file, available_dates, is_intraday)
                missing_underlying = self.get_missing_dates(underlying_file, available_dates, is_intraday)

                # 🔹 **Rimuove date già presenti nei file**
                available_dates = available_dates - (set(missing_options) | set(missing_greeks) | set(missing_underlying))

            # **Scaricare solo i dati mancanti**
            new_options = pd.DataFrame()
            if not available_dates:
                print(f"✅ No missing dates for timeframe {timeframe}. Skipping update.")
                continue  # Evita download inutili

            if not missing_options.empty:
                new_options = self.fetch_intraday_option_data(missing_options) if is_intraday else self.options_fetcher.fetch_daily_option_data(missing_options)

            new_greeks = pd.DataFrame()
            if not missing_greeks.empty:
                new_greeks = self.fetch_option_greeks_intraday(missing_greeks) if is_intraday else self.fetch_option_greeks_eod(missing_greeks)
                new_options = self.merge_option_data(new_options, new_greeks)

            new_oi = pd.DataFrame()
            if not missing_greeks.empty:  # OI is saved within Greeks file
                new_oi = self.fetch_option_open_interest(missing_greeks)
                new_greeks = self.merge_option_data(new_greeks, new_oi)

            new_underlying = pd.DataFrame()
            if not missing_underlying.empty:
                new_underlying = fetch_intraday(missing_underlying) if is_intraday else fetch_daily(missing_underlying)

            # **Merge and save**
            if not new_options.empty:
                self.merge_downloaded_data(option_file, new_options)
            if not new_greeks.empty:
                self.merge_downloaded_data(greeks_file, new_greeks)
            if not new_underlying.empty:
                self.merge_downloaded_data(underlying_file, new_underlying)

        print("✅ Data update complete.")




    def get_available_dates(self, is_intraday=False, data_type="stock"):
        """
        Retrieves the available dates (daily) or timestamps (intraday) 
        for the given symbol, distinguishing between stock and index.

        Args:
            is_intraday (bool): If True, retrieves intraday timestamps instead of daily dates.
            data_type (str): Must be "stock" or "index" to determine which API endpoint to use.

        Returns:
            list: Sorted list of available dates or timestamps.
        """
        if data_type == "stock":
            endpoint = "/v2/list/dates/stock/quote" if not is_intraday else "/v2/list/timestamps/stock/quote"
        elif data_type == "index":
            endpoint = "/v2/list/dates/index/quote" if not is_intraday else "/v2/list/timestamps/index/quote"
        else:
            raise ValueError(f"Invalid data_type: {data_type}. Must be 'stock' or 'index'.")

        try:
            response = requests.get(f"{self.BASE_URL}{endpoint}", params={"root": self.symbol})
            response.raise_for_status()
            data = response.json()

            if "response" in data and isinstance(data["response"], list) and len(data["response"]) > 0:
                dates = sorted(pd.to_datetime([str(d) for d in data["response"]], format="%Y%m%d" if not is_intraday else "%Y%m%d%H%M"))
                return dates
            else:
                raise ValueError(f"❌ No available dates returned by API for {self.symbol}.")

        except requests.exceptions.RequestException as e:
            print(f"❌ Error retrieving available dates: {e}")
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
