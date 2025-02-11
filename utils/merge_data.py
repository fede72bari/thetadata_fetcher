import os
import pandas as pd
# Funzioni per unire opzioni con greche, IV, OI

def merge_option_data(self, options_df, greeks_df, iv_df, oi_df):
    """Unisce opzioni con greche, IV e OI sulle righe giuste."""
    if options_df is None:
        return None

    # Unione basata su strike, expiration, type (C/P) e timestamp
    merged_df = options_df.copy()

    if greeks_df is not None:
        merged_df = merged_df.merge(greeks_df, on=["date", "symbol", "strike", "expiration", "type"], how="left")

    if iv_df is not None:
        merged_df = merged_df.merge(iv_df, on=["date", "symbol", "strike", "expiration", "type"], how="left")

    if oi_df is not None:
        merged_df = merged_df.merge(oi_df, on=["date", "symbol", "strike", "expiration", "type"], how="left")

    return merged_df


def merge_downloaded_data(self, file_path, new_data):
    """Merges newly downloaded data with existing data, ensuring column consistency."""
    
    if not os.path.exists(file_path):
        new_data.to_parquet(file_path, compression="zstd")
        return

    existing_data = pd.read_parquet(file_path)

    if set(existing_data.columns) != set(new_data.columns):
        raise ValueError(f"Column mismatch detected. Existing: {list(existing_data.columns)}, New: {list(new_data.columns)}")

    merged_data = pd.concat([existing_data, new_data]).drop_duplicates().sort_values("date")
    merged_data.to_parquet(file_path, compression="zstd")

