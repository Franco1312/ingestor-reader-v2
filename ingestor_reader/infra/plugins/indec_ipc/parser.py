"""INDEC IPC parser."""
import io
import pandas as pd

from ingestor_reader.domain.plugins.base import ParserPlugin
from ingestor_reader.infra.plugins.bcra_infomondia.parser import _column_letter_to_index


class ParserINDECIPC(ParserPlugin):
    """INDEC IPC parser - extracts series with category mapping."""
    
    id = "indec_ipc"
    
    def parse(self, config, raw_bytes: bytes) -> pd.DataFrame:
        """Parse INDEC IPC Excel file with category-based series."""
        parse_config = getattr(config, "parse_config", None)
        if not parse_config or "series_map" not in parse_config:
            raise ValueError("parse_config.series_map is required")
        
        series_map = parse_config["series_map"]
        all_series = []
        
        for series_config in series_map:
            sheet_name = series_config["sheet"]
            header_row = series_config["header_row"]
            start_data_row = series_config.get("start_data_row", header_row + 1)
            date_col = series_config["date_col"]
            value_col = series_config["value_col"]
            category_col = series_config.get("category_col")
            category_mapping = series_config.get("category_mapping", {})
            internal_series_code_prefix = series_config.get("internal_series_code_prefix")
            skip_headers = series_config.get("skip_headers", [])
            drop_na = series_config.get("drop_na", True)
            
            # Read sheet
            df = pd.read_excel(
                io.BytesIO(raw_bytes),
                sheet_name=sheet_name,
                header=header_row,
                engine="openpyxl",
            )
            
            # Get column indices
            date_col_idx = _column_letter_to_index(date_col)
            value_col_idx = _column_letter_to_index(value_col)
            category_col_idx = _column_letter_to_index(category_col) if category_col else None
            
            date_col_name = df.columns[date_col_idx]
            value_col_name = df.columns[value_col_idx]
            category_col_name = df.columns[category_col_idx] if category_col_idx is not None else None
            
            # Extract data starting from start_data_row
            data_df = df.iloc[start_data_row:].copy()
            
            # Filter out skip headers
            if category_col_name and skip_headers:
                data_df = data_df[~data_df[category_col_name].isin(skip_headers)]
            
            # Get unit and frequency from series config
            unit = series_config.get("unit")
            frequency = series_config.get("frequency")
            
            # Map categories to series codes
            for category_name, series_suffix in category_mapping.items():
                category_data = data_df[data_df[category_col_name] == category_name].copy()
                
                if len(category_data) > 0:
                    internal_series_code = f"{internal_series_code_prefix}_{series_suffix}"
                    
                    series_df = pd.DataFrame({
                        "obs_time": category_data[date_col_name].values,
                        "value": category_data[value_col_name].values,
                        "internal_series_code": internal_series_code,
                    })
                    
                    # Add unit and frequency from config if provided
                    if unit is not None:
                        series_df["unit"] = unit
                    if frequency is not None:
                        series_df["frequency"] = frequency
                    
                    if drop_na:
                        series_df = series_df.dropna(subset=["obs_time", "value"])
                    
                    all_series.append(series_df)
        
        if not all_series:
            return pd.DataFrame(columns=["obs_time", "value", "internal_series_code"])
        
        return pd.concat(all_series, ignore_index=True)

