import pandas as pd
import numpy as np

def read_excel_file(file_path: str, sheet_name: str, usecols: list = None) -> pd.DataFrame:
    """
    Reads an Excel file and returns a DataFrame with uppercase column names.

    Parameters:
        file_path (str): Path to the Excel file to be read
        sheet_name (str): Name of the sheet to read from the Excel file
        usecols (list, optional): List of column names to read from the sheet.
                                 If None, all columns will be read.
                                 Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing the specified columns (or all columns if usecols=None)
                     from the Excel sheet, with all column names converted to uppercase.

    Example:
        # Read specific columns
        >>> df = read_excel_file(
        ...     'data.xlsx',
        ...     'Sheet1',
        ...     ['date', 'userId', 'total']
        ... )
        >>> print(df.columns)
        Index(['DATE', 'USERID', 'TOTAL'], dtype='object')

        # Read all columns
        >>> df = read_excel_file('data.xlsx', 'Sheet1')
        >>> print(df.columns)
        Index(['DATE', 'USERID', 'TOTAL', 'COUNTRY', ...], dtype='object')
    """
    excel_file = pd.ExcelFile(file_path)
    df = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        usecols=usecols,
        dtype_backend='numpy_nullable'
    )
    df.columns = df.columns.str.upper()
    return df

def extract_value_after_equals(value):
    try:
        parts = str(value).split('=')
        if len(parts) != 2:
            return np.nan
        return float(parts[1])
    except (ValueError, IndexError):
        return np.nan