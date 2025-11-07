# Data Flow Through Pipeline

Este documento describe cómo se transforman los datos a través de cada paso del pipeline.

## Estructura de Datos por Paso

### 1. Parser Output

Después del paso `parse_file`, el DataFrame tiene esta estructura:

```python
DataFrame:
  obs_time: object (raw dates from Excel)
  value: object (raw numbers from Excel)
  internal_series_code: str
```

**Ejemplo:**
```
obs_time          | value  | internal_series_code
------------------|--------|----------------------
2024-01-01        | 350.5  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-02        | 351.2  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-01        | 25000  | BCRA_RESERVAS_USD_M_D
```

### 2. Normalizer Output

Después del paso `normalize_rows`, el DataFrame tiene:

```python
DataFrame:
  obs_time: datetime64[ns] (con timezone si está configurado)
  value: float64
  internal_series_code: str
```

**Ejemplo:**
```
obs_time                          | value  | internal_series_code
----------------------------------|--------|----------------------
2024-01-01 00:00:00-03:00         | 350.5  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-02 00:00:00-03:00         | 351.2  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-01 00:00:00-03:00         | 25000  | BCRA_RESERVAS_USD_M_D
```

### 3. Delta Output

Después del paso `compute_delta`, el DataFrame tiene:

```python
DataFrame:
  obs_time: datetime64[ns]
  value: float64
  internal_series_code: str
  key_hash: str (columna temporal para cálculo)
```

**Ejemplo (solo filas nuevas):**
```
obs_time                          | value  | internal_series_code                    | key_hash
----------------------------------|--------|------------------------------------------|----------
2024-01-03 00:00:00-03:00         | 352.1  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D        | abc123...
2024-01-03 00:00:00-03:00         | 25100  | BCRA_RESERVAS_USD_M_D                    | def456...
```

### 4. Output Final (Parquet)

El DataFrame final que se escribe a `outputs/<version_ts>/data/part-*.parquet`:

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

**Nota:** La columna `key_hash` se elimina antes de escribir los outputs finales - solo se usa internamente para el cálculo del delta. Las columnas de metadata se agregan automáticamente antes de escribir.

## Primary Keys

Los primary keys se definen en `normalize.primary_keys` (ej: `[obs_time, internal_series_code]`).

El sistema calcula `key_hash = SHA1(obs_time|internal_series_code)` para:
- Identificar filas duplicadas
- Calcular el delta (anti-join)
- Mantener el índice `index/keys.parquet`

## Ejemplo Completo: bcra_infomondia_series

### Input (Excel)
- Múltiples hojas con series diferentes
- Cada serie tiene su propia columna de fecha y valor

### Parser Output
```python
obs_time          | value  | internal_series_code
------------------|--------|----------------------
2024-01-01        | 350.5  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-02        | 351.2  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-01        | 25000  | BCRA_RESERVAS_USD_M_D
2024-01-02        | 25100  | BCRA_RESERVAS_USD_M_D
```

### Normalizer Output
```python
obs_time                          | value  | internal_series_code
----------------------------------|--------|----------------------
2024-01-01 00:00:00-03:00         | 350.5  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-02 00:00:00-03:00         | 351.2  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D
2024-01-01 00:00:00-03:00         | 25000  | BCRA_RESERVAS_USD_M_D
2024-01-02 00:00:00-03:00         | 25100  | BCRA_RESERVAS_USD_M_D
```

### Delta Output (si hay filas nuevas)
```python
obs_time                          | value  | internal_series_code                    | key_hash
----------------------------------|--------|------------------------------------------|----------
2024-01-03 00:00:00-03:00         | 352.1  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D        | abc123...
2024-01-03 00:00:00-03:00         | 25100  | BCRA_RESERVAS_USD_M_D                    | def456...
```

### Output Final (Parquet)
```python
dataset_id | provider | frequency | unit | source_kind | obs_time                          | obs_date   | value  | internal_series_code                    | version | vintage_date | quality_flag
-----------|----------|-----------|------|-------------|----------------------------------|------------|--------|------------------------------------------|---------|--------------|-------------
bcra_...   | BCRA     | D         | ...  | API         | 2024-01-03 00:00:00-03:00         | 2024-01-03 | 352.1  | BCRA_TC_OFICIAL_A3500_PESOSxUSD_D        | 2024... | 2024-01-03...| OK
bcra_...   | BCRA     | D         | ...  | API         | 2024-01-03 00:00:00-03:00         | 2024-01-03 | 25100  | BCRA_RESERVAS_USD_M_D                    | 2024... | 2024-01-03...| OK
```

## Index Structure

El índice `index/keys.parquet` contiene solo:

```python
DataFrame:
  key_hash: str
```

**Ejemplo:**
```
key_hash
--------
abc123def456...
def456ghi789...
```

Este índice se usa para calcular el delta en cada run.

