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
    Lee el último archivo publicado del dataset.
    
    Args:
        dataset_id: ID del dataset (ej: bcra_infomondia_series)
        output_file: Archivo opcional para guardar el resultado (CSV o Parquet)
        year: Año opcional para filtrar (ej: 2025)
        month: Mes opcional para filtrar (ej: 11)
    
    Returns:
        DataFrame con todos los datos de la última versión publicada
    """
    # Cargar configuración
    load_env_file()
    app_config = load_app_config()
    
    # Inicializar S3
    s3_storage = S3Storage(bucket=app_config.s3_bucket, region=app_config.aws_region)
    catalog = S3Catalog(s3_storage)
    parquet_io = ParquetIO()
    
    print(f"📊 Leyendo último dataset publicado: {dataset_id}")
    
    # 1. Leer puntero actual
    current_manifest = catalog.read_current_manifest(dataset_id)
    if current_manifest is None:
        raise FileNotFoundError(f"No se encontró versión publicada para {dataset_id}")
    
    version_ts = current_manifest["current_version"]
    print(f"✅ Versión actual: {version_ts}")
    
    # 2. Leer manifest de la versión
    manifest_key = f"datasets/{dataset_id}/versions/{version_ts}/manifest.json"
    manifest_body = s3_storage.get_object(manifest_key)
    manifest = Manifest.model_validate_json(manifest_body.decode())
    
    print(f"📁 Archivos de salida: {len(manifest.outputs.files)}")
    
    # 3. Filtrar archivos por año/mes si se especificó
    files_to_read = manifest.outputs.files
    if year is not None and month is not None:
        filter_pattern = f"year={year}/month={month:02d}/"
        files_to_read = [f for f in files_to_read if filter_pattern in f]
        print(f"🔍 Filtrando por {year}-{month:02d}: {len(files_to_read)} archivos")
    
    # 4. Leer todos los archivos parquet filtrados
    dataframes = []
    for file_key in files_to_read:
        print(f"  📄 Leyendo: {file_key}")
        parquet_body = s3_storage.get_object(file_key)
        file_df = parquet_io.read_from_bytes(parquet_body)
        dataframes.append(file_df)
    
    # 5. Combinar todos los DataFrames
    if not dataframes:
        print("⚠️  No se encontraron archivos de datos")
        return pd.DataFrame()
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    print(f"✅ Total de filas: {len(combined_df)}")
    print(f"📊 Columnas: {', '.join(combined_df.columns)}")
    
    # 6. Guardar si se especificó archivo de salida
    if output_file:
        if output_file.endswith('.csv'):
            combined_df.to_csv(output_file, index=False)
            print(f"💾 Guardado en: {output_file}")
        elif output_file.endswith('.parquet'):
            combined_df.to_parquet(output_file, index=False)
            print(f"💾 Guardado en: {output_file}")
        else:
            print(f"⚠️  Formato no reconocido, guardando como CSV: {output_file}.csv")
            combined_df.to_csv(f"{output_file}.csv", index=False)
    
    return combined_df


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

