from .granular_registry import ElementTransform
import pandas as pd

@ElementTransform.register_operation("clean_strings")
def clean_strings(df: pd.DataFrame, column: str, replace: str = "") -> pd.DataFrame:
    """Cleans a specific column in a dataframe."""
    print(f"Marker: {df}")
    df_copy = df.copy()
    if column in df_copy.columns:
        df_copy[column] = df_copy[column].astype(str).str.replace(replace, "", regex=False).str.strip()
    return df_copy