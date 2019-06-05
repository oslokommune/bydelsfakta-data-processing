import numpy as np


def sum_nans(df):
    """
    Sum aggregation function with special NaN handling. Only accepts series
    that are either all NaN or contains no NaNs.

    Args:
        df: pandas series.

    Returns:
        sum of the series if all values are not NaN, or NaN if all values in
        the series are NaN.

    Raises:
        ValueError: If the series contains a mix of values and NaNs.
    """
    no_nans = df.notna().all()
    all_nans = df.isna().all()

    if no_nans:
        return np.sum(df)
    elif all_nans:
        return np.nan
    else:
        raise ValueError("Mix of NaN and values")
