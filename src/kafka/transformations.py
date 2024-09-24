import os 
from unidecode import unidecode
from src.data import COLUMNS_TO_NORMALIZE, COLUMNS_TO_KEEP

def merge_two_columns(
    col_a: str, col_b: str, row: dict, normalize: bool = True
) -> dict:
    val_col_a = row.get(col_a)
    val_col_b = row.get(col_b)
    new_col = ""
    next_start_char = ""
    if val_col_a:
        new_col = val_col_a
        next_start_char = "\n"
    if val_col_b:
        new_col += next_start_char + val_col_b
    final = new_col or None
    if final and normalize:
        final = normalize_one(final)
    return final

def normalize_one(text: str) -> str:
    
    """
    Remove accents.
    """
    return unidecode(text)

def normalize_columns(api_row: dict) -> dict:
    kafka_row = {}
    for col in COLUMNS_TO_KEEP:
        kafka_row[col] = api_row.get(col)
        