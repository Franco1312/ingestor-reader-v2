#!/usr/bin/env python3
"""Script rápido para leer el último archivo publicado de un dataset."""
import sys
import pandas as pd
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestor_reader.infra.configs.app_config_loader import load_app_config
from ingestor_reader.infra.configs.env_loader import load_env_file
from ingestor_reader.infra.s3_storage import S3Storage
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.parquet_io import ParquetIO
from ingestor_reader.domain.entities.manifest import Manifest


def read_latest_dataset(
    dataset_id: str, 
    output_file: str | None = None,
    year: int | None = None,
    month: int | None = None
) -> pd.DataFrame:
    """
    Lee proyecciones consolidadas del dataset.
    
    Args:
        dataset_id: ID del dataset (ej: bcra_infomondia_series)
        output_file: Archivo opcional para guardar el resultado (CSV o Parquet)
        year: Año para leer (requerido si se especifica month)
        month: Mes para leer (requerido si se especifica year)
    
    Returns:
        DataFrame con datos consolidados de la proyección
    """
    # Cargar configuración
    load_env_file()
    app_config = load_app_config()
    
    # Inicializar S3
    s3_storage = S3Storage(bucket=app_config.s3_bucket, region=app_config.aws_region)
    catalog = S3Catalog(s3_storage)
    
    print(f"📊 Leyendo proyecciones consolidadas: {dataset_id}")
    
    # Si se especificó año y mes, leer solo ese mes
    if year is not None and month is not None:
        print(f"🔍 Leyendo proyección: {year}-{month:02d}")
        df = catalog.read_projection(dataset_id, year, month)
        if df is None:
            raise FileNotFoundError(f"No se encontró proyección para {year}-{month:02d}")
        
        print(f"✅ Total de filas: {len(df)}")
        print(f"📊 Columnas: {', '.join(df.columns)}")
        
        if output_file:
            if output_file.endswith('.csv'):
                df.to_csv(output_file, index=False)
                print(f"💾 Guardado en: {output_file}")
            elif output_file.endswith('.parquet'):
                df.to_parquet(output_file, index=False)
                print(f"💾 Guardado en: {output_file}")
            else:
                print(f"⚠️  Formato no reconocido, guardando como CSV: {output_file}.csv")
                df.to_csv(f"{output_file}.csv", index=False)
        
        return df
    
    # Si no se especificó año/mes, leer todos los meses disponibles
    # (Esto requeriría listar todas las proyecciones, por ahora requerimos año/mes)
    raise ValueError("Debe especificar --year y --month para leer proyecciones")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Lee el último archivo publicado de un dataset")
    parser.add_argument("dataset_id", help="ID del dataset (ej: bcra_infomondia_series)")
    parser.add_argument("-o", "--output", help="Archivo de salida (CSV o Parquet)")
    parser.add_argument("--year", type=int, help="Año para filtrar (ej: 2025)")
    parser.add_argument("--month", type=int, help="Mes para filtrar (ej: 11)")
    
    args = parser.parse_args()
    
    try:
        df = read_latest_dataset(args.dataset_id, args.output, args.year, args.month)
        
        # Mostrar preview
        print("\n" + "="*80)
        print("📋 Preview de los datos:")
        print("="*80)
        print(df.head(10).to_string())
        print(f"\n... ({len(df)} filas en total)")
        
    except (FileNotFoundError, ValueError, KeyError) as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

