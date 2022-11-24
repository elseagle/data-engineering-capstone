def localize_timestamp(df, time_zone):
    """Localize a timestamp to a given timezone."""
    return df.dt.tz_localize(time_zone, ambiguous="infer")


def convert_timestamp(df, time_zone):
    """Convert a timestamp to a given timezone."""
    return df.dt.tz_convert(time_zone)


def connect_sheet():
    """Connect to the Google Sheet."""
    import gspread

    credential = gspread.service_account_from_dict(Variable.get("GSPREAD_CREDENTIALS"))
    client = gspread.authorize(credential)
    sh = client.open_by_url(Variable.get("SHEET_URL")).sheet1
    return sh


def strip_fields(df):
    """Strip whitespace from fields."""
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def null_validation(df):
    """Check for null values."""
    not_null_cols = [
        "operator_timestamp",
        "title",
        "company",
        "first_name",
        "last_name",
    ]
    failed_validation = []
    for col in not_null_cols:
        if df[col].isnull().any():
            failed_validation.append(col)
    return failed_validation


def datetime_type_validation(df):
    """Check for datetime type."""
    if df["operator_timestamp"].dtype != "datetime64[ns, UTC]":
        return True
    return False


def save_as_parquet(dataframe, file_path, mode="append"):
    """Save a dataframe as a parquet file."""
    dataframe.write.mode(mode).parquet(file_path)
