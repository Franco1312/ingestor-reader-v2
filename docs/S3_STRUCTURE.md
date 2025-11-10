# Estructura de Datos en S3

Este documento describe la estructura completa de archivos y carpetas que el pipeline publica en S3.

## Estructura General

```
s3://<bucket>/datasets/<dataset_id>/
├── configs/
│   └── config.yaml                    # Configuración del dataset (opcional)
├── index/
│   └── keys.parquet                   # Índice de primary keys (hash)
├── events/                             # Event Store (Event Sourcing)
│   ├── index/                          # Índice de eventos por mes (optimización)
│   │   └── YYYY/
│   │       └── MM/
│   │           └── versions.json      # Lista de versiones con eventos para el mes
│   └── <version_ts>/
│       ├── manifest.json              # Manifest del evento
│       └── data/
│           ├── year=YYYY/
│           │   └── month=MM/
│           │       └── part-0.parquet  # Delta del evento (inmutable)
│           └── part-0.parquet          # (solo si no hay columna de fecha)
├── projections/                        # Read Models (CQRS)
│   └── windows/
│       └── year=YYYY/month=MM/
│           └── data.parquet            # Proyección consolidada del mes
├── current/
│   └── manifest.json                   # Puntero a versión actual (CAS)
└── metadata/
    └── current.json                    # Estado actual (opcional)
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

### 3. `events/<version_ts>/manifest.json`

**Ruta:** `datasets/<dataset_id>/events/<version_ts>/manifest.json`

**Contenido:** Manifest JSON con metadata del evento (versión incremental).

**Estructura:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "version": "2025-11-07T05-51-43",
  "created_at": "2025-11-07T05:52:21.899000+00:00",
  "source": {
    "files": [
      {
        "sha256": "26d56d91b6cb26eb9503d72b23c2d6dbfbd1a9595e2f18d71e9f9e456...",
        "size": 7505424
      }
    ]
  },
  "outputs": {
    "data_prefix": "datasets/bcra_infomondia_series/events/2025-11-07T05-51-43/data/",
    "files": [
      "datasets/bcra_infomondia_series/events/2025-11-07T05-51-43/data/year=2024/month=11/part-0.parquet"
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
- Metadata completa de cada evento
- Trazabilidad de cambios (Event Sourcing)
- Historial completo de eventos (inmutables)

**Nota:** Cada evento tiene su propio manifest, incluso si no se publicó (CAS falló).

---

### 4. `events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet`

**Ruta:** `datasets/<dataset_id>/events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet`

**Contenido:** Delta del evento (filas nuevas) con todas las columnas de metadata, particionados por año y mes.

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
- Evento inmutable (Event Sourcing)
- Incluye solo el delta (filas nuevas) de esta versión
- Usado para auditoría y trazabilidad
- **No se usa directamente para lectura** (usar proyecciones)

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
- La columna `key_hash` **NO** está incluida en los eventos
- Solo contiene filas nuevas (delta) en modo incremental
- En la primera ejecución, contiene todas las filas
- Cada evento puede tener múltiples particiones (una por año/mes presente en los datos)
- **Inmutable**: Los eventos nunca se modifican

---

### 5. `events/index/YYYY/MM/versions.json` (Índice de Eventos)

**Ruta:** `datasets/<dataset_id>/events/index/YYYY/MM/versions.json`

**Contenido:** Índice JSON que lista todas las versiones que tienen eventos para un mes específico.

**Estructura:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "year": 2007,
  "month": 8,
  "versions": [
    "2025-11-09T23-04-09",
    "2025-11-10T10-00-00",
    "2025-11-11T15-30-00"
  ],
  "last_updated": "2025-11-11T15:30:00Z",
  "event_count": 3
}
```

**Propósito:**
- **Optimización de performance**: Evita listar todos los objetos en S3 para encontrar eventos de un mes
- **Búsqueda rápida**: Permite encontrar rápidamente qué versiones tienen eventos para un mes
- **Escalabilidad**: No depende del número total de versiones, solo del número de versiones por mes

**Actualización:**
- Se actualiza automáticamente cuando se escriben eventos nuevos para el mes
- Se crea automáticamente si no existe cuando se lista eventos (fallback)
- Solo se agregan versiones, nunca se eliminan (acumulativo)

**Comportamiento:**
1. **Ruta rápida (con índice):**
   - Lee el índice JSON (pequeño, rápido)
   - Construye las claves de eventos desde las versiones del índice
   - Retorna las claves sin listar objetos en S3

2. **Ruta lenta (sin índice):**
   - Lista todos los objetos bajo `events/` (puede ser lento con muchas versiones)
   - Filtra por año/mes
   - Crea el índice para futuras consultas

**Ventajas:**
- **Performance**: Una lectura de JSON pequeño vs. listar miles de objetos
- **Escalable**: No depende del número total de versiones
- **Automático**: Se actualiza y crea automáticamente

**Riesgos conocidos:**
- **Desincronización**: Si se eliminan eventos manualmente, el índice puede quedar desactualizado
- **Race conditions**: Si dos procesos escriben eventos simultáneamente, puede haber pérdida de versiones en el índice
- **Acumulación**: El índice solo crece, nunca se limpia automáticamente

**Mitigación:**
- El fallback detecta eventos faltantes y reconstruye el índice si es necesario
- Los errores al leer eventos se manejan en `_read_events_for_month`

---

### 6. `projections/windows/year=YYYY/month=MM/data.parquet`

**Ruta:** `datasets/<dataset_id>/projections/windows/year=YYYY/month=MM/data.parquet`

**Contenido:** Proyección consolidada del mes (todos los datos del mes consolidados desde eventos).

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
- **Datos consolidados para lectura** (Read Model - CQRS)
- Contiene todos los datos del mes (consolidados desde todos los eventos)
- Optimizado para lectura simple
- Se regenera periódicamente desde eventos

**Ventajas:**
- **Lectura simple**: Una lectura por mes
- **Sin concatenación**: No necesitas leer múltiples versiones
- **Eficiente**: Optimizado para consultas por ventana de tiempo

**Nota:**
- Se regenera desde eventos (no se modifica incrementalmente)
- Si se corrompe, puede regenerarse desde eventos
- Puede tener delay (se consolida después de publicar eventos)

---

### 7. `current/manifest.json`

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

1. **Fetch Resource:**
   - Descarga el archivo fuente (no se guarda en S3)

2. **Parse & Normalize:**
   - Procesa el archivo en memoria (no escribe en S3)

3. **Compute Delta:**
   - Calcula qué filas son nuevas usando el índice (no escribe en S3)

4. **Enrich Metadata:**
   - Prepara DataFrame con metadata (no escribe aún)

5. **Write Events:**
   - Particiona y escribe `events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet` (solo delta enriquecido)
   - Crea una partición por cada año/mes presente en los datos
   - **Inmutable**: Cada evento es único, nunca se sobrescribe

6. **Publish Event:**
   - Escribe `events/<version_ts>/manifest.json`
   - Intenta actualizar `current/manifest.json` con CAS
   - Si CAS exitoso: actualiza `index/keys.parquet`
   - Si CAS falla: no actualiza el índice ni el puntero

7. **Consolidate Projection:**
   - Para cada año/mes afectado:
     - Lista todos los eventos del mes
     - Lee todos los eventos
     - Consolida (concatena y elimina duplicados)
     - Escribe `projections/windows/year=YYYY/month=MM/data.parquet`
   - **Regenera**: Proyección se regenera desde eventos, no se modifica incrementalmente

### Orden de Operaciones (Atomicidad)

```
1. Write events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet (una por partición)
2. Write events/<version_ts>/manifest.json
3. CAS update: current/manifest.json (con If-Match)
4. If CAS success: write index/keys.parquet
5. If CAS fails: abort (index y pointer no cambian)
6. If CAS success: consolidate projections/windows/year=YYYY/month=MM/data.parquet
```

**Nota:** El pipeline escribe:
- `events/` - Eventos incrementales (inmutables, para auditoría)
- `projections/` - Proyecciones consolidadas (para lectura)
- `current/` - Puntero actual
- `index/` - Índice de primary keys

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

- **Events:** Retener todos los eventos para historial completo (Event Sourcing)
- **Projections:** Pueden regenerarse desde eventos si es necesario
- **Index:** Siempre mantener (es necesario para el cálculo de delta)
- **Current:** Siempre mantener (puntero crítico)

---

## Ejemplo Completo

Para un dataset `bcra_infomondia_series` con dos ejecuciones:

```
s3://bucket/datasets/bcra_infomondia_series/
├── index/
│   └── keys.parquet
├── events/
│   ├── 2025-11-07T05-51-43/
│   │   ├── manifest.json
│   │   └── data/
│   │       ├── year=2024/month=01/part-0.parquet
│   │       ├── year=2024/month=02/part-0.parquet
│   │       └── ...
│   └── 2025-11-07T06-15-22/
│       ├── manifest.json
│       └── data/
│           └── year=2025/month=11/part-0.parquet
├── projections/
│   └── windows/
│       ├── year=2024/
│       │   ├── month=01/data.parquet  # Consolidado de todos los eventos de enero
│       │   ├── month=02/data.parquet
│       │   └── ...
│       └── year=2025/
│           └── month=11/data.parquet  # Consolidado de todos los eventos de noviembre
└── current/
    └── manifest.json  # Apunta a 2025-11-07T06-15-22
```

---

## Lectura de Datos

### Para Consumir Datos (Recomendado: Usar Proyecciones)

**Lectura simple usando proyecciones consolidadas:**

1. **Leer un mes completo:**
   ```python
   # Leer noviembre 2024
   df = pd.read_parquet("s3://bucket/datasets/<dataset_id>/projections/windows/year=2024/month=11/data.parquet")
   ```

2. **Leer un año completo:**
   ```python
   # Leer 2024 completo
   dataframes = []
   for month in range(1, 13):
       df = pd.read_parquet(f"s3://bucket/datasets/<dataset_id>/projections/windows/year=2024/month={month:02d}/data.parquet")
       dataframes.append(df)
   
   complete_year = pd.concat(dataframes, ignore_index=True)
   ```

3. **Leer múltiples meses:**
   ```python
   # Leer últimos 3 meses
   from datetime import datetime, timedelta
   now = datetime.now()
   dataframes = []
   for i in range(3):
       month_date = now - timedelta(days=30*i)
       year = month_date.year
       month = month_date.month
       df = pd.read_parquet(f"s3://bucket/datasets/<dataset_id>/projections/windows/year={year}/month={month:02d}/data.parquet")
       dataframes.append(df)
   
   recent_data = pd.concat(dataframes, ignore_index=True)
   ```

**Ventajas:**
- ✅ **Simple**: Una lectura por mes
- ✅ **Eficiente**: Sin necesidad de leer múltiples versiones
- ✅ **Rápido**: Datos ya consolidados

### Para Auditoría (Usar Eventos)

Si necesitas auditoría o trazabilidad, puedes leer eventos directamente:

1. **Leer puntero actual:**
   ```python
   current_manifest = read_json("s3://bucket/datasets/<dataset_id>/current/manifest.json")
   version = current_manifest["current_version"]
   ```

2. **Leer manifest del evento:**
   ```python
   manifest = read_json(f"s3://bucket/datasets/<dataset_id>/events/{version}/manifest.json")
   event_files = manifest["outputs"]["files"]
   ```

3. **Leer todos los eventos de un mes:**
   ```python
   # Listar todos los eventos de noviembre 2024
   event_keys = catalog.list_events_for_month(dataset_id, 2024, 11)
   for event_key in event_keys:
       df = pd.read_parquet(f"s3://bucket/{event_key}")
       # Procesar evento
   ```

---

## Notas Importantes

1. **Event Sourcing:** Los eventos son inmutables y contienen solo el delta (filas nuevas)
2. **CQRS:** Las proyecciones son read models optimizados para lectura, regeneradas desde eventos
3. **Atomicidad:** El puntero `current/manifest.json` se actualiza con CAS para garantizar atomicidad
4. **Consistencia:** El índice se actualiza solo después de una publicación exitosa
5. **Metadata:** Los eventos y proyecciones incluyen todas las columnas de metadata estándar
6. **Inmutabilidad:** Los eventos nunca se modifican, solo se agregan nuevos
7. **Regeneración:** Las proyecciones se regeneran desde eventos, no se modifican incrementalmente

---

## Manejo de Errores y Rollback

### ¿Qué pasa si el pipeline falla?

El pipeline está diseñado para ser **idempotente** y **seguro ante fallos**. Aquí está qué pasa en cada escenario:

#### 1. **Fallo antes de escribir eventos** (líneas 193-199)
- **Estado:** No se escribió nada a S3
- **Resultado:** No hay datos huérfanos, todo limpio
- **Acción:** Simplemente reintentar el pipeline

#### 2. **Fallo después de escribir eventos pero antes de CAS** (líneas 117-120)
- **Estado:**
  - ✅ Eventos escritos en `events/<version_ts>/data/...`
  - ✅ Manifest de evento escrito en `events/<version_ts>/manifest.json`
  - ❌ Puntero `current/manifest.json` **NO** actualizado
  - ❌ Índice `index/keys.parquet` **NO** actualizado
  - ❌ Proyecciones **NO** consolidadas
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
  - ✅ Eventos escritos
  - ✅ Manifest de evento escrito
  - ❌ CAS falla (ETag no coincide - posible ejecución concurrente)
  - ❌ Puntero **NO** actualizado
  - ❌ Índice **NO** actualizado
  - ❌ Proyecciones **NO** consolidadas
- **Resultado:** Igual que el caso 2 - datos huérfanos
- **Acción:** El pipeline detecta el fallo y retorna `published=False`

#### 4. **Fallo después de CAS exitoso pero antes de actualizar índice** (línea 101)
- **Estado:**
  - ✅ Eventos escritos
  - ✅ Manifest de evento escrito
  - ✅ Puntero actualizado (CAS exitoso)
  - ❌ Índice **NO** actualizado (fallo al escribir)
  - ❌ Proyecciones **NO** consolidadas
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
- `events/<version_ts>/data/...` - No se consumen (puntero no apunta)
- `events/<version_ts>/manifest.json` - Existe pero no se usa

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

