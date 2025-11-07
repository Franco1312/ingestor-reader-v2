# Estructura de Datos en S3

Este documento describe la estructura completa de archivos y carpetas que el pipeline publica en S3.

## Estructura General

```
s3://<bucket>/datasets/<dataset_id>/
├── configs/
│   └── config.yaml                    # Configuración del dataset (opcional)
├── index/
│   └── keys.parquet                   # Índice de primary keys (hash)
├── runs/
│   └── <run_id>/
│       ├── raw/
│       │   └── <filename>             # Archivo fuente original
│       ├── staging/
│       │   └── normalized.parquet     # Datos normalizados (sin metadata)
│       └── delta/
│           └── added.parquet          # Delta con key_hash (debugging)
├── versions/
│   └── <version_ts>/
│       └── manifest.json              # Manifest de la versión
├── outputs/
│   └── <version_ts>/
│       └── data/
│           ├── year=YYYY/
│           │   └── month=MM/
│           │       └── part-0.parquet  # Datos publicados particionados por año/mes
│           └── part-0.parquet          # (solo si no hay columna de fecha)
└── current/
    └── manifest.json                   # Puntero a versión actual (CAS)
```

## Descripción Detallada

### 1. `configs/config.yaml` (Opcional)

**Ruta:** `datasets/<dataset_id>/configs/config.yaml`

**Contenido:** Configuración del dataset en formato YAML. Puede almacenarse en S3 o localmente.

**Ejemplo:**
```yaml
dataset_id: bcra_infomondia_series
frequency: D
source:
  kind: http
  url: https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/series.xlsm
  format: xlsx
parse:
  plugin: bcra_infomondia
normalize:
  plugin: bcra_infomondia
  primary_keys:
    - obs_time
    - internal_series_code
  timezone: America/Argentina/Buenos_Aires
```

---

### 2. `index/keys.parquet`

**Ruta:** `datasets/<dataset_id>/index/keys.parquet`

**Contenido:** Índice de primary keys en formato Parquet. Contiene solo los hashes de las primary keys de todas las filas publicadas.

**Estructura:**
```python
DataFrame:
  key_hash: string  # SHA1 hash de primary keys concatenados
```

**Propósito:**
- Calcular el delta en cada ejecución (anti-join)
- Evitar duplicados
- Mantener consistencia incremental

**Ejemplo:**
```
key_hash
--------
a1b2c3d4e5f6...
f6e5d4c3b2a1...
```

**Actualización:**
- Se actualiza solo después de una publicación exitosa (CAS)
- Si el CAS falla, el índice no se modifica

---

### 3. `runs/<run_id>/raw/<filename>`

**Ruta:** `datasets/<dataset_id>/runs/<run_id>/raw/<filename>`

**Contenido:** Archivo fuente original tal como se descargó o leyó.

**Formato:** Depende del origen (Excel, CSV, etc.)

**Propósito:**
- Auditoría y trazabilidad
- Debugging
- Reprocesamiento si es necesario

**Ejemplo:**
- `runs/eab62f46-2817-4fca-99b1-950151a759bf/raw/series.xlsm`

---

### 4. `runs/<run_id>/staging/normalized.parquet`

**Ruta:** `datasets/<dataset_id>/runs/<run_id>/staging/normalized.parquet`

**Contenido:** Datos normalizados después del paso de parsing y normalización, pero **sin** metadata adicional.

**Estructura:**
```python
DataFrame:
  obs_time: datetime64[ns] (con timezone si está configurado)
  value: float64
  internal_series_code: string
  # Columnas adicionales según el normalizer (ej: frequency, unit)
```

**Propósito:**
- Datos intermedios para debugging
- Permite inspeccionar el estado después de normalización
- Útil para troubleshooting

**Ejemplo:**
```
obs_time                          | value  | internal_series_code
----------------------------------|--------|----------------------
2024-01-01 00:00:00-03:00         | 350.5  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-02 00:00:00-03:00         | 351.2  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
```

---

### 5. `runs/<run_id>/delta/added.parquet`

**Ruta:** `datasets/<dataset_id>/runs/<run_id>/delta/added.parquet`

**Contenido:** Delta de filas nuevas con la columna `key_hash` incluida (para debugging).

**Estructura:**
```python
DataFrame:
  obs_time: datetime64[ns]
  value: float64
  internal_series_code: string
  key_hash: string  # Hash de primary keys (solo para debugging)
  # Columnas adicionales según el normalizer
```

**Propósito:**
- Debugging del cálculo de delta
- Verificar qué filas se consideraron nuevas
- Validar el anti-join

**Nota:** Este archivo solo se escribe si hay filas nuevas (`len(delta_df) > 0`).

---

### 6. `versions/<version_ts>/manifest.json`

**Ruta:** `datasets/<dataset_id>/versions/<version_ts>/manifest.json`

**Contenido:** Manifest JSON con metadata de la versión publicada.

**Estructura:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "version": "2025-11-07T05-51-43",
  "created_at": "2025-11-07T05:52:21.899000+00:00",
  "source": {
    "files": [
      {
        "path": "datasets/bcra_infomondia_series/runs/eab62f46-2817-4fca-99b1-950151a759bf/raw/series.xlsm",
        "sha256": "26d56d91b6cb26eb9503d72b23c2d6dbfbd1a9595e2f18d71e9f9e456...",
        "size": 7505424
      }
    ]
  },
  "outputs": {
    "data_prefix": "datasets/bcra_infomondia_series/outputs/2025-11-07T05-51-43/data/",
    "files": [
      "datasets/bcra_infomondia_series/outputs/2025-11-07T05-51-43/data/part-0.parquet"
    ],
    "rows_total": 85360,
    "rows_added_this_version": 85360
  },
  "index": {
    "path": "datasets/bcra_infomondia_series/index/keys.parquet",
    "key_columns": ["obs_time", "internal_series_code"],
    "hash_column": "key_hash"
  }
}
```

**Propósito:**
- Metadata completa de cada versión
- Trazabilidad de cambios
- Historial de versiones

**Nota:** Cada versión tiene su propio manifest, incluso si no se publicó (CAS falló).

---

### 7. `outputs/<version_ts>/data/year=YYYY/month=MM/part-0.parquet`

**Ruta:** `datasets/<dataset_id>/outputs/<version_ts>/data/year=YYYY/month=MM/part-0.parquet`

**Contenido:** Datos finales publicados con todas las columnas de metadata, particionados por año y mes.

**Estructura:**
```python
DataFrame:
  dataset_id: string
  provider: string
  frequency: string
  unit: string
  source_kind: string ("FILE" / "API")
  obs_time: datetime64[ns, tz]
  obs_date: date
  value: float64
  internal_series_code: string
  version: string
  vintage_date: datetime
  quality_flag: string ("OK" / "OUTLIER" / "IMPUTED")
```

**Propósito:**
- Datos finales para consumo
- Incluye solo el delta (filas nuevas) en modo incremental
- Listo para ser consumido por sistemas downstream

**Ejemplo:**
```
dataset_id | provider | frequency | unit | source_kind | obs_time                          | obs_date   | value  | internal_series_code                    | version | vintage_date | quality_flag
-----------|----------|-----------|------|-------------|----------------------------------|------------|--------|------------------------------------------|---------|--------------|-------------
bcra_...   | BCRA     | D         | ...  | API         | 2024-01-01 00:00:00-03:00         | 2024-01-01 | 350.5  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D        | 2025... | 2025-11-07...| OK
```

**Particionamiento:**
- Los datos se particionan automáticamente por año/mes basado en `obs_time` o `obs_date`
- Formato Hive-style: `year=YYYY/month=MM/part-0.parquet`
- Cada partición contiene solo los datos de ese año/mes
- Si no hay columna de fecha, se escribe un solo archivo sin particionamiento

**Ventajas del particionamiento:**
- **Consultas eficientes:** Permite leer solo los datos de un año/mes específico
- **Costo reducido:** Solo se leen las particiones necesarias
- **Acceso rápido a datos recientes:** Fácil acceso a los meses más actuales
- **Compatibilidad:** Formato Hive-style compatible con Spark, Athena, etc.

**Nota:** 
- La columna `key_hash` **NO** está incluida en los outputs finales
- Solo contiene filas nuevas (delta) en modo incremental
- En la primera ejecución, contiene todas las filas
- Cada versión puede tener múltiples particiones (una por año/mes presente en los datos)

---

### 8. `current/manifest.json`

**Ruta:** `datasets/<dataset_id>/current/manifest.json`

**Contenido:** Puntero JSON a la versión actual publicada.

**Estructura:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "current_version": "2025-11-07T05-51-43"
}
```

**Propósito:**
- Puntero atómico a la versión actual
- Actualizado con CAS (Compare-And-Swap)
- Garantiza atomicidad en la publicación

**Actualización:**
- Se actualiza solo si el CAS es exitoso
- Si el CAS falla, el puntero no cambia
- Permite rollback natural en caso de error

**CAS (Compare-And-Swap):**
- Se usa el ETag del archivo actual para verificar que no cambió
- Si el ETag no coincide, la actualización falla
- Previene condiciones de carrera en publicaciones concurrentes

---

## Flujo de Escritura

### Durante la Ejecución del Pipeline

1. **Fetch & Store Raw:**
   - Escribe `runs/<run_id>/raw/<filename>`

2. **Parse & Normalize:**
   - Escribe `runs/<run_id>/staging/normalized.parquet`

3. **Compute Delta:**
   - Si hay filas nuevas, escribe `runs/<run_id>/delta/added.parquet`

4. **Enrich Metadata:**
   - Prepara DataFrame con metadata (no escribe aún)

5. **Write Outputs:**
   - Particiona y escribe `outputs/<version_ts>/data/year=YYYY/month=MM/part-0.parquet` (solo delta enriquecido)
   - Crea una partición por cada año/mes presente en los datos

6. **Publish Version:**
   - Escribe `versions/<version_ts>/manifest.json`
   - Intenta actualizar `current/manifest.json` con CAS
   - Si CAS exitoso: actualiza `index/keys.parquet`
   - Si CAS falla: no actualiza el índice ni el puntero

### Orden de Operaciones (Atomicidad)

```
1. Write outputs/<version_ts>/data/year=YYYY/month=MM/part-0.parquet (una por partición)
2. Write versions/<version_ts>/manifest.json
3. CAS update: current/manifest.json (con If-Match)
4. If CAS success: write index/keys.parquet
5. If CAS fails: abort (index y pointer no cambian)
```

---

## Convenciones de Nombres

### `run_id`
- Formato: UUID v4
- Ejemplo: `eab62f46-2817-4fca-99b1-950151a759bf`
- Generado automáticamente si no se proporciona

### `version_ts`
- Formato: `YYYY-MM-DDTHH-MM-SS` (ISO 8601 sin separadores de tiempo)
- Ejemplo: `2025-11-07T05-51-43`
- Generado al inicio de cada ejecución

### `dataset_id`
- Formato: snake_case
- Ejemplo: `bcra_infomondia_series`
- Definido en la configuración del dataset

---

## Retención y Limpieza

**Nota:** El pipeline no elimina archivos automáticamente. Se recomienda:

- **Runs:** Retener por un período definido (ej: 30 días) para debugging
- **Versions:** Retener todas las versiones para historial completo
- **Outputs:** Retener todas las versiones para reconstrucción completa
- **Index:** Siempre mantener (es necesario para el cálculo de delta)
- **Current:** Siempre mantener (puntero crítico)

---

## Ejemplo Completo

Para un dataset `bcra_infomondia_series` con dos ejecuciones:

```
s3://bucket/datasets/bcra_infomondia_series/
├── index/
│   └── keys.parquet
├── runs/
│   ├── eab62f46-2817-4fca-99b1-950151a759bf/
│   │   ├── raw/
│   │   │   └── series.xlsm
│   │   ├── staging/
│   │   │   └── normalized.parquet
│   │   └── delta/
│   │       └── added.parquet
│   └── f1c23d45-3928-5gdb-00c2-061262b860cg/
│       ├── raw/
│       │   └── series.xlsm
│       ├── staging/
│       │   └── normalized.parquet
│       └── delta/
│           └── added.parquet
├── versions/
│   ├── 2025-11-07T05-51-43/
│   │   └── manifest.json
│   └── 2025-11-07T06-15-22/
│       └── manifest.json
├── outputs/
│   ├── 2025-11-07T05-51-43/
│   │   └── data/
│   │       ├── year=2024/
│   │       │   ├── month=01/
│   │       │   │   └── part-0.parquet
│   │       │   ├── month=02/
│   │       │   │   └── part-0.parquet
│   │       │   └── ...
│   │       └── year=2025/
│   │           └── month=11/
│   │               └── part-0.parquet
│   └── 2025-11-07T06-15-22/
│       └── data/
│           └── year=2025/
│               └── month=11/
│                   └── part-0.parquet
└── current/
    └── manifest.json  # Apunta a 2025-11-07T06-15-22
```

---

## Lectura de Datos

### Para Consumir Datos Publicados

1. **Leer puntero actual:**
   ```python
   current_manifest = read_json("s3://bucket/datasets/<dataset_id>/current/manifest.json")
   version = current_manifest["current_version"]
   ```

2. **Leer manifest de versión:**
   ```python
   manifest = read_json(f"s3://bucket/datasets/<dataset_id>/versions/{version}/manifest.json")
   output_files = manifest["outputs"]["files"]
   ```

3. **Leer todos los datos de la versión:**
   ```python
   for file_key in output_files:
       df = pd.read_parquet(f"s3://bucket/{file_key}")
   ```

4. **Leer solo datos de un año/mes específico (más eficiente):**
   ```python
   # Leer solo datos de noviembre 2025
   data_prefix = manifest["outputs"]["data_prefix"]
   df = pd.read_parquet(f"s3://bucket/{data_prefix}year=2025/month=11/")
   ```

5. **Leer solo datos recientes (últimos meses):**
   ```python
   # Leer últimos 3 meses
   from datetime import datetime, timedelta
   now = datetime.now()
   for i in range(3):
       month_date = now - timedelta(days=30*i)
       year = month_date.year
       month = month_date.month
       df = pd.read_parquet(f"s3://bucket/{data_prefix}year={year}/month={month:02d}/")
   ```

### Para Reconstruir Dataset Completo

1. Leer todos los manifests en `versions/`
2. Ordenar por `created_at`
3. Leer todos los `outputs/<version>/data/year=*/month=*/part-*.parquet` en orden
4. Concatenar DataFrames

**Nota:** Con particionamiento, puedes reconstruir solo un rango específico:
```python
# Reconstruir solo datos de 2024
data_prefix = "datasets/<dataset_id>/outputs/"
for version in sorted_versions:
    for month in range(1, 13):
        path = f"{data_prefix}{version}/data/year=2024/month={month:02d}/"
        df = pd.read_parquet(f"s3://bucket/{path}")
```

---

## Notas Importantes

1. **Incremental:** Los outputs solo contienen filas nuevas (delta), no el dataset completo
2. **Atomicidad:** El puntero `current/manifest.json` se actualiza con CAS para garantizar atomicidad
3. **Consistencia:** El índice se actualiza solo después de una publicación exitosa
4. **Metadata:** Los outputs finales incluyen todas las columnas de metadata estándar
5. **Debugging:** Los archivos en `runs/` son útiles para debugging pero no son necesarios para consumo

---

## Manejo de Errores y Rollback

### ¿Qué pasa si el pipeline falla?

El pipeline está diseñado para ser **idempotente** y **seguro ante fallos**. Aquí está qué pasa en cada escenario:

#### 1. **Fallo antes de escribir outputs** (líneas 193-199)
- **Estado:** No se escribió nada a S3
- **Resultado:** No hay datos huérfanos, todo limpio
- **Acción:** Simplemente reintentar el pipeline

#### 2. **Fallo después de escribir outputs pero antes de CAS** (líneas 117-120)
- **Estado:**
  - ✅ Outputs escritos en `outputs/<version_ts>/data/...`
  - ✅ Manifest de versión escrito en `versions/<version_ts>/manifest.json`
  - ✅ Datos de staging/delta escritos en `runs/<run_id>/...`
  - ❌ Puntero `current/manifest.json` **NO** actualizado
  - ❌ Índice `index/keys.parquet` **NO** actualizado
- **Resultado:** 
  - Los datos existen pero **no son visibles** (el puntero no apunta a ellos)
  - Son "datos huérfanos" - existen pero no se consumen
  - El índice sigue apuntando a la versión anterior
- **Acción:**
  - Los datos quedan en S3 pero no afectan el consumo
  - Al reintentar, se calculará el delta correctamente (usando el índice anterior)
  - Los datos huérfanos pueden limpiarse manualmente si es necesario

#### 3. **Fallo durante CAS** (línea 97)
- **Estado:**
  - ✅ Outputs escritos
  - ✅ Manifest de versión escrito
  - ❌ CAS falla (ETag no coincide - posible ejecución concurrente)
  - ❌ Puntero **NO** actualizado
  - ❌ Índice **NO** actualizado
- **Resultado:** Igual que el caso 2 - datos huérfanos
- **Acción:** El pipeline detecta el fallo y retorna `published=False`

#### 4. **Fallo después de CAS exitoso pero antes de actualizar índice** (línea 101)
- **Estado:**
  - ✅ Outputs escritos
  - ✅ Manifest de versión escrito
  - ✅ Puntero actualizado (CAS exitoso)
  - ❌ Índice **NO** actualizado (fallo al escribir)
- **Resultado:** 
  - **Inconsistencia:** El puntero apunta a una versión nueva pero el índice no está actualizado
  - En la próxima ejecución, el delta se calculará incorrectamente (usará el índice viejo)
- **Acción:** 
  - **Problema:** Este es el único caso problemático
  - **Solución:** El pipeline debería manejar esto mejor (ver nota abajo)

#### 5. **Fallo después de todo** (línea 209)
- **Estado:** Todo exitoso, solo falla la notificación
- **Resultado:** Los datos están publicados correctamente, solo falta la notificación
- **Acción:** Reintentar notificación o ignorar (los datos ya están disponibles)

### Estrategia de Rollback

El pipeline usa **rollback natural**:

1. **Antes de CAS:** Si algo falla, los datos escritos quedan "huérfanos" pero no afectan el consumo
2. **CAS como barrera:** El CAS es la única operación crítica - si falla, nada cambia
3. **Después de CAS:** Si el CAS es exitoso pero luego falla, hay inconsistencia

### Datos Huérfanos

Los datos huérfanos (escritos pero no publicados) quedan en:
- `outputs/<version_ts>/data/...` - No se consumen (puntero no apunta)
- `versions/<version_ts>/manifest.json` - Existe pero no se usa
- `runs/<run_id>/...` - Siempre se guardan para debugging

**Limpieza:**
- Los datos huérfanos pueden limpiarse manualmente
- No afectan el funcionamiento del pipeline
- Pueden ser útiles para debugging o reprocesamiento

### Mejora Sugerida

Para el caso 4 (fallo después de CAS), se podría:
1. Hacer el CAS y actualización de índice en una transacción (pero S3 no soporta transacciones)
2. Agregar un mecanismo de reconciliación que detecte inconsistencias
3. Hacer el índice más resiliente (leer el manifest actual y reconstruir el índice si es necesario)

**Nota actual:** El código actual no maneja explícitamente el caso 4, pero en la práctica es raro que falle la escritura del índice después de un CAS exitoso (ambos son operaciones S3 simples).

