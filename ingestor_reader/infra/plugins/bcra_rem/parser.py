"""BCRA REM parser."""
import io
import pandas as pd

from ingestor_reader.domain.plugins.base import ParserPlugin
from ingestor_reader.infra.plugins.bcra_infomondia.parser import _extract_series_from_sheet


class ParserBCRAREM(ParserPlugin):
    """BCRA REM parser - extracts series with category titles."""
    
    id = "bcra_rem"
    
    def parse(self, config, raw_bytes: bytes) -> pd.DataFrame:
        """Parse BCRA REM Excel file with category-based series."""
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
            
            # Extract series (start_data_row is used if provided)
            series_df = _extract_series_from_sheet(
                df, date_col, value_col, internal_series_code,
                header_row, start_data_row, drop_na, unit, frequency
            )
            
            all_series.append(series_df)
        
        if not all_series:
            return pd.DataFrame(columns=["obs_time", "value", "internal_series_code"])
        
        return pd.concat(all_series, ignore_index=True)

