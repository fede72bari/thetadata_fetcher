# Funzioni generiche di supporto

def get_missing_dates(self, file_path, first_date, last_date, is_intraday=False):
    """Restituisce solo le date o i minuti mancanti nel file locale."""
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



