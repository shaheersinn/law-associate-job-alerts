def remove_old_jobs(df: pd.DataFrame, max_age_days: int = 40) -> pd.DataFrame:
    """
    Remove jobs older than max_age_days. Jobs with no parseable date are kept.
    Handles strings, datetime/date objects, and pandas Timestamps safely.
    """
    if df.empty:
        return df

    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=max_age_days)

    # Prefer DATE, fall back to DATE_POSTED, otherwise empty
    if "DATE" in df.columns:
        primary = df["DATE"]
    else:
        primary = pd.Series([None] * len(df), index=df.index)

    if "DATE_POSTED" in df.columns:
        fallback = df["DATE_POSTED"]
    else:
        fallback = pd.Series([None] * len(df), index=df.index)

    combined = primary.where(primary.notna(), fallback)

    # Convert whatever we have (date/datetime/strings) into timestamps
    dt = pd.to_datetime(combined, errors="coerce", utc=True)

    # Keep rows with unknown date (NaT) OR fresh enough
    keep = dt.isna() | (dt >= cutoff)

    return df.loc[keep].copy()
