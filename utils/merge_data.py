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
