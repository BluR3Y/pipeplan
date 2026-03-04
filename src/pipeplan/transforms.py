from typing import Any
from .registry import Operation

class TransformOps(Operation, type_id="transform"): pass

@TransformOps.register_operation("clean_strings")
def clean_strings(df: Any, column: str, replace: str = "") -> Any:
    import pandas as pd
    df_copy = df.copy()
    if column in df_copy.columns:
        df_copy[column] = df_copy[column].astype(str).str.replace(replace, "", regex=False).str.strip()
    return df_copy