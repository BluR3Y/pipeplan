from .granular_registry import SetTransform
import pandas as pd

@SetTransform.register_operation("filter_rows")
def filter_rows(df: pd.DataFrame, column: str, threshold: float = 0) -> pd.DataFrame:
    """Filters rows based on a numeric threshold."""
    if column in df.columns:
        return df[pd.to_numeric(df[column], errors='coerce') > threshold]
    return df