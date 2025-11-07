"""BCRA Infomondia parser."""
import io
import pandas as pd

from ingestor_reader.domain.plugins.base import ParserPlugin


def _column_letter_to_index(letter: str) -> int:
    """Convert Excel column letter to 0-based index."""
    result = 0
    for char in letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result - 1


def _extract_series_from_sheet(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    internal_series_code: str,
    header_row: int,
    start_data_row: int | None = None,
    drop_na: bool = True,
    unit: str | None = None,
    frequency: str | None = None,
) -> pd.DataFrame:
    """Extract a single series from a sheet DataFrame."""
    date_col_idx = _column_letter_to_index(date_col)
    value_col_idx = _column_letter_to_index(value_col)
    
    date_col_name = df.columns[date_col_idx]
    value_col_name = df.columns[value_col_idx]
    
    data_start = start_data_row if start_data_row is not None else header_row + 1
    
    result_df = pd.DataFrame({
        "obs_time": df.iloc[data_start:][date_col_name].values,
        "value": df.iloc[data_start:][value_col_name].values,
        "internal_series_code": internal_series_code,
    })
    
    # Add unit and frequency from config if provided
    if unit is not None:
        result_df["unit"] = unit
    if frequency is not None:
        result_df["frequency"] = frequency
    
    if drop_na:
        result_df = result_df.dropna(subset=["obs_time", "value"])
    
    return result_df.reset_index(drop=True)


class ParserBCRAInfomondia(ParserPlugin):
    """BCRA Infomondia parser - extracts multiple series from Excel."""
    
    id = "bcra_infomondia"
    
    def parse(self, config, raw_bytes: bytes) -> pd.DataFrame:
        """Parse BCRA Infomondia Excel file with multiple series."""
        parse_config = getattr(config, "parse_config", None)
        if not parse_config or "series_map" not in parse_config:
            raise ValueError("parse_config.series_map is required")
        
        series_map = parse_config["series_map"]
        all_series = []
        
        for series_config in series_map:
            sheet_name = series_config["sheet"]
            header_row = series_config["header_row"]
            date_col = series_config["date_col"]
            value_col = series_config["value_col"]
            internal_series_code = series_config["internal_series_code"]
            drop_na = series_config.get("drop_na", True)
            start_data_row = series_config.get("start_data_row")
            unit = series_config.get("unit")
            frequency = series_config.get("frequency")
            
            # Read sheet
            df = pd.read_excel(
                io.BytesIO(raw_bytes),
                sheet_name=sheet_name,
                header=header_row,
                engine="openpyxl",
            )
            
            # Extract series
            series_df = _extract_series_from_sheet(
                df, date_col, value_col, internal_series_code,
                header_row, start_data_row, drop_na, unit, frequency
            )
            
            all_series.append(series_df)
        
        if not all_series:
            return pd.DataFrame(columns=["obs_time", "value", "internal_series_code"])
        
        return pd.concat(all_series, ignore_index=True)

