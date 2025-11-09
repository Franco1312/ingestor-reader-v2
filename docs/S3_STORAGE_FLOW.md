# Almacenamiento en S3: Qué, Cómo y Cuándo

Este documento explica qué datos guardamos en S3, cómo los guardamos y cuándo se guardan durante la ejecución del pipeline.

## Tabla de Contenidos

1. [Resumen Ejecutivo](#resumen-ejecutivo)
2. [Comportamiento Incremental](#comportamiento-incremental)
3. [Qué Guardamos](#qué-guardamos)
4. [Cómo Guardamos](#cómo-guardamos)
5. [Cuándo Guardamos](#cuándo-guardamos)
6. [Flujo Completo del Pipeline](#flujo-completo-del-pipeline)
7. [Atomicidad y Consistencia](#atomicidad-y-consistencia)

---

## Resumen Ejecutivo

El pipeline guarda datos en S3 en **11 pasos principales**, organizados en **4 categorías**:

1. **Eventos** - Eventos incrementales inmutables (Event Sourcing)
2. **Proyecciones** - Proyecciones consolidadas para lectura (CQRS)
3. **Metadata y Manifests** - Información sobre eventos y punteros
4. **Índices** - Hashes de primary keys para cálculo incremental

**Formato principal:** Parquet (datos) y JSON (metadata)

**Estrategia:** Incremental con atomicidad garantizada mediante CAS (Compare-And-Swap)

---

## Comportamiento Incremental

### ¿Se Reemplazan Todos los Datos en Cada Ejecución?

**No.** El pipeline es **incremental**: solo agrega las filas nuevas, nunca reemplaza los datos existentes.

### Primera Ejecución (Día 1)

**Escenario:** Primera vez que se ejecuta el pipeline.

**Proceso:**
1. Descarga el archivo fuente (ej: 1000 filas)
2. **No hay índice previo** → todas las filas se consideran nuevas
3. Procesa las 1000 filas
4. Guarda eventos en `events/2025-11-09T04-33-36/data/` (todas las filas)
5. Consolida proyecciones en `projections/windows/year=YYYY/month=MM/data.parquet`
6. Crea el índice con los hashes de las 1000 filas

**Resultado:** 1000 filas publicadas en la primera versión.

---

### Segunda Ejecución (Día 2, 2 Filas Nuevas)

**Escenario:** Al día siguiente, el archivo fuente tiene 2 filas nuevas (ahora tiene 1002 filas).

**Proceso:**
1. Descarga el archivo fuente (1002 filas)
2. **Lee el índice existente** (1000 hashes de la ejecución anterior)
3. Calcula el **delta** usando anti-join:
   - Compara los hashes de las 1002 filas con el índice
   - Identifica que 2 filas tienen hashes que no están en el índice
   - Estas son las filas nuevas
4. **Solo procesa esas 2 filas nuevas**
5. Guarda eventos en `events/2025-11-10T05-20-15/data/` (solo las 2 filas nuevas)
6. Consolida proyecciones (regenera desde todos los eventos del mes)
7. Actualiza el índice agregando los 2 hashes nuevos

**Resultado:** 2 filas nuevas publicadas en la segunda versión. **No reemplaza las 1000 anteriores.**

---

### Cómo Funciona el Anti-Join

El sistema usa un **anti-join** para identificar filas nuevas:

```python
# ingestor_reader/domain/services/delta_service.py:42-43
existing_hashes = set(index_df[hash_column].values)
added_df = normalized_df[~normalized_df[hash_column].isin(existing_hashes)].copy()
```

**Proceso:**
1. **Calcula hash SHA1** de las primary keys de cada fila:
   ```python
   # Para cada fila:
   primary_key_values = [row[col] for col in primary_keys]
   key_string = '|'.join(str(v) for v in primary_key_values)
   key_hash = SHA1(key_string).hexdigest()
   ```

2. **Compara con el índice existente:**
   - Lee `index/keys.parquet` (contiene todos los hashes de filas ya publicadas)
   - Compara cada hash de las filas nuevas con el índice
   - Si el hash no está en el índice → fila nueva
   - Si el hash está en el índice → fila ya existe (se ignora)

3. **Solo procesa filas nuevas:**
   - Solo las filas con hashes nuevos se procesan y publican
   - Las filas existentes se ignoran completamente

---

### Estructura en S3 (Ejemplo Real)

Cada versión guarda **solo el delta** (filas nuevas):

```
events/
├── 2025-11-09T04-33-36/  (Primera ejecución)
│   ├── manifest.json
│   └── data/
│       ├── year=2003/month=01/part-0.parquet  (1000 filas)
│       ├── year=2003/month=02/part-0.parquet
│       └── ...
projections/
└── windows/
    ├── year=2003/month=01/data.parquet  (consolidado)
    ├── year=2003/month=02/data.parquet  (consolidado)
    └── ...
│
└── 2025-11-10T05-20-15/  (Segunda ejecución - 2 filas nuevas)
    └── data/
        └── year=2024/month=11/part-0.parquet  (2 filas nuevas)
```

**Importante:**
- Los datos de la primera versión **no se reemplazan**
- La segunda versión **solo contiene las 2 filas nuevas**
- Cada versión es **independiente** y contiene solo su delta

---

### Para Consumir el Dataset Completo

Para obtener **todas las filas** (histórico completo), necesitas leer **todas las versiones** y concatenarlas:

```python
import pandas as pd
from ingestor_reader.infra.s3_catalog import S3Catalog

# Leer puntero actual
catalog = S3Catalog(s3_storage)
current_manifest = catalog.read_current_manifest(dataset_id)
current_version = current_manifest["current_version"]

# Leer todas las versiones (desde la primera hasta la actual)
all_data = []
for version in sorted_versions:  # Ordenar por timestamp
    manifest = catalog.read_version_manifest(dataset_id, version)
    for file_key in manifest["outputs"]["files"]:
        df = pd.read_parquet(f"s3://bucket/{file_key}")
        all_data.append(df)

# Concatenar todo
complete_dataset = pd.concat(all_data, ignore_index=True)
```

**Alternativa más eficiente (solo datos recientes):**
```python
# Leer solo las últimas N versiones
recent_versions = sorted_versions[-5:]  # Últimas 5 versiones
for version in recent_versions:
    # ... leer y concatenar
```

---

### Ventajas del Sistema Incremental

✅ **Eficiencia:** Solo procesa filas nuevas, no todo el dataset

✅ **Velocidad:** Ejecuciones rápidas cuando hay pocos cambios

✅ **Costo:** Menor uso de recursos (CPU, memoria, S3 storage)

✅ **Trazabilidad:** Historial completo de cambios (cada versión es inmutable)

✅ **Idempotencia:** Re-ejecutar es seguro (calcula delta correctamente)

✅ **Concurrencia:** Múltiples ejecuciones no corrompen datos (CAS previene conflictos)

---

### Casos Especiales

#### ¿Qué pasa si una fila cambia (mismo primary key, valor diferente)?

**Respuesta:** El sistema **no detecta cambios en valores**, solo detecta filas nuevas.

- Si el primary key es el mismo → el hash es el mismo → se ignora
- Si necesitas detectar cambios de valores, necesitarías incluir el valor en el primary key o usar un sistema diferente

#### ¿Qué pasa si quiero reprocesar todo desde cero?

**Opciones:**
1. **Eliminar el índice:** Borra `index/keys.parquet` → la próxima ejecución procesará todo como nuevo
2. **Flag `--full-reload`:** Fuerza procesamiento aunque el hash del archivo no cambió, pero aún así usa el índice para calcular delta
3. **Eliminar versión específica:** Borra una versión y su manifest, pero el índice seguirá teniendo esos hashes

**Recomendación:** Para reprocesar todo, elimina el índice y el puntero actual, luego ejecuta el pipeline.

---

### Resumen del Comportamiento Incremental

| Ejecución | Filas en Fuente | Filas en Índice | Delta Calculado | Filas Publicadas |
|-----------|-----------------|-----------------|-----------------|------------------|
| 1ra | 1000 | 0 (primera vez) | 1000 (todas) | 1000 |
| 2da | 1002 | 1000 | 2 (nuevas) | 2 |
| 3ra | 1005 | 1002 | 3 (nuevas) | 3 |
| 4ta | 1005 | 1005 | 0 (sin cambios) | 0 (no publica) |

**Nota:** Si el delta es 0, el pipeline no publica una nueva versión (aunque puede escribir los archivos de staging para debugging).

---

### Dónde y Cuándo se Crea el Índice

El índice se crea y actualiza en el **Paso 9: Publish Version**, específicamente:

#### Primera Ejecución (Creación del Índice)

**Cuándo:** Después de un CAS exitoso en el paso de publicación.

**Proceso:**
1. Se calculan los hashes de las filas nuevas (en el paso 6: Compute Delta)
2. Se prepara el índice actualizado llamando a `update_index()`:
   ```python
   # ingestor_reader/use_cases/steps/publish_version.py:58
   updated_index_df = update_index(current_index_df, delta_df)
   ```
   - Como `current_index_df` es `None` (primera vez), se crea un nuevo DataFrame con solo los hashes del delta
3. Se escribe el índice **solo después de un CAS exitoso**:
   ```python
   # ingestor_reader/use_cases/steps/publish_version.py:101
   catalog.write_index(dataset_id, updated_index_df)
   ```

**Ubicación:** `datasets/<dataset_id>/index/keys.parquet`

**Contenido inicial:**
```python
DataFrame:
  key_hash: string  # SHA1 hash de primary keys
```

**Ejemplo (primera ejecución con 1000 filas):**
```
key_hash
--------
a1b2c3d4e5f6...  (hash de fila 1)
f6e5d4c3b2a1...  (hash de fila 2)
...
(1000 hashes en total)
```

#### Ejecuciones Posteriores (Actualización del Índice)

**Cuándo:** Después de un CAS exitoso, solo si hay filas nuevas.

**Proceso:**
1. Se lee el índice existente:
   ```python
   # ingestor_reader/use_cases/steps/compute_delta.py
   current_index_df = catalog.read_index(config.dataset_id)
   ```

2. Se calculan los hashes de las filas nuevas (paso 6)

3. Se prepara el índice actualizado:
   ```python
   # ingestor_reader/domain/services/delta_service.py:48-70
   def update_index(current_index_df, added_df):
       if current_index_df is None or len(current_index_df) == 0:
           return added_df[[hash_column]].copy()  # Primera vez
       
       # Append new hashes
       new_hashes = added_df[[hash_column]].copy()
       updated_index = pd.concat([current_index_df, new_hashes], ignore_index=True)
       return updated_index.drop_duplicates(subset=[hash_column], keep="first")
   ```

4. Se escribe el índice actualizado **solo después de un CAS exitoso**:
   ```python
   # ingestor_reader/use_cases/steps/publish_version.py:101
   catalog.write_index(dataset_id, updated_index_df)
   ```

**Ejemplo (segunda ejecución con 2 filas nuevas):**
- Índice anterior: 1000 hashes
- Nuevos hashes: 2
- Índice actualizado: 1002 hashes (1000 + 2)

#### Orden Crítico de Operaciones

El índice se actualiza en un orden específico para garantizar consistencia:

```
1. Escribir events/<version_ts>/data/... (paso 8)
2. Escribir events/<version_ts>/manifest.json (paso 9)
3. CAS update: current/manifest.json (paso 9)
4. ✅ Si CAS exitoso: Escribir index/keys.parquet (paso 9)
5. ✅ Si CAS exitoso: Consolidar projections/windows/year=YYYY/month=MM/data.parquet (paso 10)
6. ❌ Si CAS falla: NO escribir índice ni consolidar proyecciones (mantiene consistencia)
```

**Importante:**
- El índice **solo se actualiza después de un CAS exitoso**
- Si el CAS falla, el índice **no se modifica** (mantiene consistencia)
- Esto garantiza que el índice siempre refleje solo las versiones publicadas

#### Código de Referencia

**Creación/Actualización del índice:**
```python
# ingestor_reader/use_cases/steps/publish_version.py:95-101
try:
    # Try CAS update - this is the critical atomic operation
    catalog.put_current_manifest_pointer(dataset_id, pointer_body, current_manifest_etag)
    
    # Only update index AFTER successful pointer update
    # This ensures consistency: if CAS fails, index stays unchanged
    catalog.write_index(dataset_id, updated_index_df)
```

**Función que actualiza el índice:**
```python
# ingestor_reader/domain/services/delta_service.py:48-70
def update_index(current_index_df, added_df, hash_column="key_hash"):
    # Primera vez: crear nuevo índice
    if current_index_df is None or len(current_index_df) == 0:
        return added_df[[hash_column]].copy()
    
    # Ejecuciones posteriores: agregar nuevos hashes
    new_hashes = added_df[[hash_column]].copy()
    updated_index = pd.concat([current_index_df, new_hashes], ignore_index=True)
    return updated_index.drop_duplicates(subset=[hash_column], keep="first")
```

**Escritura del índice a S3:**
```python
# ingestor_reader/infra/s3_catalog.py:160-164
def write_index(self, dataset_id: str, df: pd.DataFrame) -> None:
    """Write index DataFrame."""
    key = self._index_key(dataset_id)  # datasets/<dataset_id>/index/keys.parquet
    body = self.parquet_io.write_to_bytes(df)
    self.s3.put_object(key, body, content_type="application/x-parquet")
```

---

### ¿Puedo Usar el Índice para Consumir Datos Nuevos?

**Respuesta corta:** El índice es principalmente para **manejo interno** del pipeline, pero técnicamente está disponible en S3. Sin embargo, **no es la forma recomendada** para consumir datos nuevos.

#### ¿El Índice Está Disponible para Consumidores?

**Sí, técnicamente está disponible:**
- Ubicación: `datasets/<dataset_id>/index/keys.parquet`
- Formato: Parquet con una columna `key_hash`
- Puede ser leído por cualquier consumidor con acceso a S3

**Pero tiene limitaciones:**
- Solo contiene **hashes**, no los datos reales
- No indica **qué versión** contiene cada hash
- No tiene información sobre **cuándo** se agregó cada hash
- Requiere calcular hashes de tus datos para comparar

#### Forma Recomendada para Consumir Datos Nuevos

**1. Usar el mensaje SNS (notificación automática):**
```python
# Cuando recibes un mensaje SNS:
{
  "type": "DATASET_UPDATED",
  "timestamp": "2025-11-09T04:33:34+00:00",
  "dataset_id": "bcra_infomondia_series",
  "manifest_pointer": "bcra_infomondia_series/versions/2025-11-09T04-33-36/manifest.json"
}

# Leer el manifest para ver qué archivos leer
manifest = read_json(f"s3://bucket/{manifest_pointer}")
output_files = manifest["outputs"]["files"]  # Lista de archivos con datos nuevos
rows_added = manifest["outputs"]["rows_added_this_version"]  # Cuántas filas nuevas
```

**2. Leer los outputs de la versión (solo contiene filas nuevas):**
```python
# Cada versión solo contiene el delta (filas nuevas)
for file_key in manifest["outputs"]["files"]:
    df = pd.read_parquet(f"s3://bucket/{file_key}")
    # Este DataFrame solo contiene filas nuevas de esta versión
```

**3. Seguir el puntero actual:**
```python
# Leer puntero actual
current_manifest = read_json("s3://bucket/datasets/<dataset_id>/current/manifest.json")
current_version = current_manifest["current_version"]

# Leer manifest de la versión actual
manifest = read_json(f"s3://bucket/datasets/<dataset_id>/versions/{current_version}/manifest.json")

# Leer los outputs (solo filas nuevas de la última versión)
for file_key in manifest["outputs"]["files"]:
    df = pd.read_parquet(f"s3://bucket/{file_key}")
```

#### ¿Cuándo Sería Útil Usar el Índice?

El índice podría ser útil en casos muy específicos:

**1. Verificar duplicados antes de insertar:**
```python
# Leer índice
index_df = pd.read_parquet("s3://bucket/datasets/<dataset_id>/index/keys.parquet")
existing_hashes = set(index_df["key_hash"].values)

# Calcular hashes de tus datos
def compute_key_hash(row, primary_keys):
    key_values = [str(row[col]) for col in primary_keys]
    key_string = "|".join(key_values)
    return hashlib.sha1(key_string.encode()).hexdigest()

# Verificar qué filas ya existen
your_data["key_hash"] = your_data.apply(
    lambda row: compute_key_hash(row, primary_keys), axis=1
)
new_rows = your_data[~your_data["key_hash"].isin(existing_hashes)]
```

**2. Calcular tu propio delta:**
```python
# Si quieres calcular qué filas son nuevas sin leer todas las versiones
# (pero esto es más complejo que simplemente leer las versiones)
```

**Limitaciones de usar el índice:**
- ❌ No sabes **qué versión** contiene cada hash
- ❌ No sabes **cuándo** se agregó cada hash
- ❌ Requieres calcular hashes de tus datos (mismo algoritmo que el pipeline)
- ❌ Más complejo que leer las versiones directamente

#### Comparación: Índice vs Versiones

| Aspecto | Índice | Versiones |
|---------|--------|-----------|
| **Contenido** | Solo hashes | Datos completos |
| **Información de versión** | ❌ No | ✅ Sí (cada versión es independiente) |
| **Fácil de usar** | ❌ Requiere calcular hashes | ✅ Solo leer archivos |
| **Identificar datos nuevos** | ❌ Complejo | ✅ Directo (cada versión = delta) |
| **Trazabilidad** | ❌ No | ✅ Sí (metadata en manifest) |
| **Recomendado para consumo** | ❌ No | ✅ Sí |

#### Ejemplo: Consumir Datos Nuevos (Forma Recomendada)

```python
import pandas as pd
from ingestor_reader.infra.s3_catalog import S3Catalog
from ingestor_reader.infra.s3_storage import S3Storage

# Inicializar
s3_storage = S3Storage(bucket="my-bucket", region="us-east-1")
catalog = S3Catalog(s3_storage)

# Opción 1: Leer última versión (solo filas nuevas)
current_manifest = catalog.read_current_manifest(dataset_id)
version = current_manifest["current_version"]
manifest = catalog.read_version_manifest(dataset_id, version)

# Leer solo los archivos de la última versión (solo delta)
all_new_data = []
for file_key in manifest["outputs"]["files"]:
    df = pd.read_parquet(f"s3://bucket/{file_key}")
    all_new_data.append(df)

new_data = pd.concat(all_new_data, ignore_index=True)
print(f"Filas nuevas: {len(new_data)}")

# Opción 2: Leer todas las versiones desde una fecha
# (más complejo, requiere iterar sobre versiones)
```

#### Resumen

✅ **Para consumir datos nuevos:** Usa las **versiones** y los **manifests**
- Cada versión contiene solo el delta (filas nuevas)
- El manifest indica exactamente qué archivos leer
- Es simple y directo

❌ **No uses el índice** para consumo normal:
- Es principalmente para uso interno del pipeline
- Solo contiene hashes, no datos reales
- Requiere calcular hashes de tus datos
- Más complejo que leer versiones

✅ **El índice podría ser útil** solo en casos muy específicos:
- Verificar duplicados antes de insertar
- Calcular tu propio delta (pero es más complejo)

---

## Qué Guardamos

**Nota:** El pipeline usa Event Sourcing + CQRS. Solo se guardan eventos inmutables y proyecciones consolidadas.

### 1. Eventos (`events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet`)

**Qué:** Eventos incrementales inmutables con **todas las columnas de metadata**, particionados por año y mes.

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
  version: string  # version_ts de esta publicación
  vintage_date: datetime  # Fecha de publicación
  quality_flag: string ("OK" / "OUTLIER" / "IMPUTED")
```

**Particionamiento:**
- **Hive-style:** `year=YYYY/month=MM/part-0.parquet`
- Una partición por cada combinación año/mes presente en los datos
- Si no hay columna de fecha, se escribe un solo archivo sin particionamiento

**Propósito:**
- Eventos inmutables (Event Sourcing)
- Incluye solo el delta (filas nuevas) de esta versión
- Usado para auditoría y trazabilidad
- **No se usa directamente para lectura** (usar proyecciones)

**Ejemplo de estructura:**
```
events/2025-11-09T04-33-36/
├── manifest.json
└── data/
├── year=2003/
│   ├── month=01/
│   │   └── part-0.parquet
│   ├── month=02/
│   │   └── part-0.parquet
│   └── ...
├── year=2004/
│   └── ...
└── year=2025/
    └── month=11/
        └── part-0.parquet
```

**Ventajas del particionamiento:**
- **Consultas eficientes:** Permite leer solo los datos de un año/mes específico
- **Costo reducido:** Solo se leen las particiones necesarias
- **Acceso rápido a datos recientes:** Fácil acceso a los meses más actuales
- **Compatibilidad:** Formato Hive-style compatible con Spark, Athena, etc.

**Nota importante:**
- La columna `key_hash` **NO** está incluida en los eventos
- Solo contiene filas nuevas (delta) en modo incremental
- En la primera ejecución, contiene todas las filas
- **Inmutable**: Los eventos nunca se modifican

#### ¿Qué Pasa si se Agregan Filas a un Mes que Ya Tiene Datos?

**Pregunta:** Si ya hay un archivo de diciembre en una versión anterior, y se agregan 2 filas nuevas de diciembre, ¿se genera una nueva partición o se guarda en la que ya había?

**Respuesta:** Se genera una **nueva partición en una nueva versión**. Cada versión es **independiente** y solo contiene el delta (filas nuevas).

**Ejemplo:**

**Versión 1 (1 de diciembre):**
```
events/2025-12-01T10-00-00/data/
└── year=2024/
    └── month=12/
        └── part-0.parquet  (100 filas de diciembre)
```

**Versión 2 (2 de diciembre, 2 filas nuevas):**
```
events/2025-12-02T10-00-00/data/
└── year=2024/
    └── month=12/
        └── part-0.parquet  (2 filas nuevas de diciembre)
```

**Resultado:**
- ✅ Se crea un **nuevo archivo** en la nueva versión
- ❌ **No se sobrescribe** la partición anterior
- ✅ Cada versión es **independiente** y contiene solo su delta

**Estructura de paths:**
Cada evento tiene su propio directorio con el `version_ts` en el path:
```
datasets/<dataset_id>/events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet
```

**Ejemplo completo:**
- **Evento 1:** `events/2025-12-01T10-00-00/data/year=2024/month=12/part-0.parquet`
- **Evento 2:** `events/2025-12-02T10-00-00/data/year=2024/month=12/part-0.parquet`

**Nota importante:** Aunque ambas particiones son de `year=2024/month=12`, están en **diferentes eventos** (`2025-12-01T10-00-00` vs `2025-12-02T10-00-00`). Son archivos **completamente separados** en S3.

**Para obtener todos los datos de diciembre (usar proyecciones):**
```python
# Usar proyección consolidada (recomendado)
df = pd.read_parquet("s3://bucket/datasets/<dataset_id>/projections/windows/year=2024/month=12/data.parquet")
# 102 filas totales (ya consolidadas)
```

**Para auditoría (leer eventos directamente):**
```python
# Leer todos los eventos de diciembre
event_keys = catalog.list_events_for_month(dataset_id, 2024, 12)
dataframes = []
for event_key in event_keys:
    df = pd.read_parquet(f"s3://bucket/{event_key}")
    dataframes.append(df)

# Concatenar
all_december_data = pd.concat(dataframes, ignore_index=True)
# 102 filas totales
```

### 2. Proyecciones (`projections/windows/year=YYYY/month=MM/data.parquet`)

**Qué:** Proyecciones consolidadas del mes (todos los datos del mes consolidados desde eventos).

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

**Ejemplo de estructura:**
```
projections/windows/
├── year=2024/
│   ├── month=01/data.parquet  # Consolidado de todos los eventos de enero
│   ├── month=02/data.parquet
│   └── ...
└── year=2025/
    └── month=11/data.parquet  # Consolidado de todos los eventos de noviembre
```

**Para leer un mes completo:**
```python
# Leer noviembre 2024
df = pd.read_parquet("s3://bucket/datasets/<dataset_id>/projections/windows/year=2024/month=11/data.parquet")
```

**Para leer un año completo:**
```python
# Leer 2024 completo
dataframes = []
for month in range(1, 13):
    df = pd.read_parquet(f"s3://bucket/datasets/<dataset_id>/projections/windows/year=2024/month={month:02d}/data.parquet")
    dataframes.append(df)

complete_year = pd.concat(dataframes, ignore_index=True)
```

# Para cada versión que tenga datos de diciembre:
all_december_data = []
for version in all_versions:
    manifest = catalog.read_version_manifest(dataset_id, version)
    if manifest:
        # Filtrar archivos que contengan year=2024/month=12
        december_files = [
            f for f in manifest["outputs"]["files"] 
            if "year=2024/month=12" in f
        ]
        for file_key in december_files:
            df = pd.read_parquet(f"s3://bucket/{file_key}")
            all_december_data.append(df)

# Concatenar todo
complete_december = pd.concat(all_december_data, ignore_index=True)
```

**Por qué funciona así:**
- Cada versión es **inmutable** (no se modifica después de publicada)
- Permite **trazabilidad completa** (saber qué versión agregó qué datos)
- Permite **rollback** (volver a una versión anterior)
- Cada versión solo contiene el **delta** (filas nuevas), no todo el dataset

**Estructura completa en S3:**
```
outputs/
├── 2025-12-01T10-00-00/  (Versión 1)
│   └── data/
│       └── year=2024/
│           └── month=12/
│               └── part-0.parquet  (100 filas)
│
└── 2025-12-02T10-00-00/  (Versión 2)
    └── data/
        └── year=2024/
            └── month=12/
                └── part-0.parquet  (2 filas nuevas)
```

**Nota:** Para consumir el dataset completo de diciembre, necesitas leer **todas las versiones** que contengan datos de diciembre y concatenarlas. El manifest de cada versión indica exactamente qué archivos leer.

---

### 2. Manifest de Versión (`versions/<version_ts>/manifest.json`)

**Qué:** Metadata completa de cada versión publicada.

**Estructura:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "version": "2025-11-09T04-33-36",
  "created_at": "2025-11-09T04:36:33.123456+00:00",
  "source": {
    "files": [
      {
        "sha256": "b4d24eb9...",
        "size": 7511210
      }
    ]
  },
  "outputs": {
    "data_prefix": "datasets/bcra_infomondia_series/outputs/2025-11-09T04-33-36/data/",
    "files": [
      "datasets/bcra_infomondia_series/outputs/2025-11-09T04-33-36/data/year=2003/month=01/part-0.parquet",
      "datasets/bcra_infomondia_series/outputs/2025-11-09T04-33-36/data/year=2003/month=02/part-0.parquet",
      ...
    ],
    "rows_total": 85288,
    "rows_added_this_version": 85288
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
- Permite reconstruir el dataset completo

**Nota:** Cada versión tiene su propio manifest, incluso si no se publicó (CAS falló).

---

### 3. Puntero a Versión Actual (`current/manifest.json`)

**Qué:** Puntero JSON a la versión actual publicada.

**Estructura:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "current_version": "2025-11-09T04-33-36"
}
```

**Propósito:**
- Puntero atómico a la versión actual
- Actualizado con CAS (Compare-And-Swap)
- Garantiza atomicidad en la publicación
- Permite a los consumidores saber qué versión leer

**Actualización:**
- Se actualiza solo si el CAS es exitoso
- Si el CAS falla, el puntero no cambia
- Permite rollback natural en caso de error

---

### 4. Índice de Primary Keys (`index/keys.parquet`)

**Qué:** Índice de primary keys en formato Parquet. Contiene solo los hashes de las primary keys de todas las filas publicadas.

**Estructura:**
```python
DataFrame:
  key_hash: string  # SHA1 hash de primary keys concatenados
```

**Ejemplo:**
```
key_hash
--------
a1b2c3d4e5f6...
f6e5d4c3b2a1...
```

**Propósito:**
- Calcular el delta en cada ejecución (anti-join)
- Evitar duplicados
- Mantener consistencia incremental
- Permite identificar qué filas son nuevas

**Actualización:**
- Se actualiza solo después de una publicación exitosa (CAS)
- Si el CAS falla, el índice no se modifica
- Contiene el hash de todas las filas publicadas hasta el momento

**Cálculo del hash:**
```python
# Para cada fila:
primary_key_values = [row[col] for col in primary_keys]
key_string = '|'.join(str(v) for v in primary_key_values)
key_hash = SHA1(key_string).hexdigest()
```

---

## Cómo Guardamos

### Formato de Archivos

#### Parquet (Datos)
- **Formato:** Apache Parquet
- **Compresión:** Snappy (por defecto)
- **Ventajas:**
  - Eficiente para columnas
  - Compresión excelente
  - Compatible con Spark, Athena, Pandas, etc.
  - Soporta tipos complejos (datetime con timezone)

#### JSON (Metadata)
- **Formato:** JSON estándar
- **Encoding:** UTF-8
- **Indentación:** 2 espacios (para legibilidad)
- **Content-Type:** `application/json`

### Estructura de Paths

Todos los paths siguen el patrón:
```
s3://<bucket>/datasets/<dataset_id>/<categoria>/<identificador>/<archivo>
```

**Categorías:**
- `configs/` - Configuraciones (opcional)
- `versions/` - Manifests de versiones
- `outputs/` - Datos publicados
- `index/` - Índices
- `current/` - Puntero actual

### Particionamiento

Los datos publicados se particionan automáticamente por año y mes usando **Hive-style partitioning**:

```
outputs/<version_ts>/data/year=YYYY/month=MM/part-0.parquet
```

**Lógica:**
1. Se extrae año y mes de la columna `obs_time` o `obs_date`
2. Se agrupan las filas por `(year, month)`
3. Se escribe un archivo Parquet por cada grupo
4. Las columnas `year` y `month` se eliminan antes de escribir (son implícitas en el path)

**Ventajas:**
- Consultas eficientes por rango de fechas
- Solo se leen las particiones necesarias
- Compatible con herramientas de Big Data

### Naming Conventions

#### `run_id`
- **Formato:** UUID v4
- **Ejemplo:** `c735f733-b0bd-4b5e-b803-978111c526c7`
- **Generación:** Automática si no se proporciona
- **Uso:** Identifica una ejecución única del pipeline

#### `version_ts`
- **Formato:** `YYYY-MM-DDTHH-MM-SS` (ISO 8601 sin separadores de tiempo)
- **Ejemplo:** `2025-11-09T04-33-36`
- **Generación:** Al inicio de cada ejecución
- **Uso:** Identifica una versión publicada

#### `dataset_id`
- **Formato:** snake_case
- **Ejemplo:** `bcra_infomondia_series`
- **Definición:** En la configuración del dataset

---

## Cuándo Guardamos

El pipeline guarda datos en S3 en **10 pasos secuenciales**:

### Paso 1: Fetch Resource

**Cuándo:** Inmediatamente después de descargar el archivo fuente.

**Qué se guarda:** Nada (solo se descarga en memoria).

**Código:**
```python
# ingestor_reader/use_cases/run_pipeline.py:74-76
content, file_hash, file_size = step_fetch_resource(
    catalog, config, app_config, run_id
)
```

**Detalles:**
- Se descarga el contenido en memoria
- Se calcula el hash SHA256 para verificación
- No se guarda en S3 (simplificado)

**Si falla:** El pipeline se detiene, no se guarda nada.

---

### Paso 2: Check Source Changed

**Cuándo:** Después de guardar el raw, antes de procesar.

**Qué se guarda:** Nada (solo lectura).

**Propósito:** Verificar si el archivo fuente cambió comparando hashes.

**Si no cambió:** El pipeline se detiene aquí, no se procesa nada más.

---

### Paso 3: Parse File

**Cuándo:** Después de verificar que el fuente cambió.

**Qué se guarda:** Nada (solo procesamiento en memoria).

**Propósito:** Convertir el archivo raw en un DataFrame.

---

### Paso 4: Filter New Data

**Cuándo:** Después de parsear, antes de normalizar.

**Qué se guarda:** Nada (solo lectura del índice si existe).

**Propósito:** Filtrar filas por fecha (solo procesar datos nuevos según `lag_days`).

**Si no hay datos nuevos:** El pipeline se detiene aquí.

---

### Paso 5: Normalize Rows

**Cuándo:** Después de filtrar, antes de calcular delta.

**Qué se guarda:** Nada (solo procesamiento en memoria).

**Propósito:** Normalizar los datos según el plugin configurado.

---

### Paso 6: Compute Delta

**Cuándo:** Después de normalizar, antes de enriquecer metadata.

**Qué se guarda:** Nada (solo lectura del índice).

**Propósito:** Calcular qué filas son nuevas usando anti-join contra el índice.

**Si no hay filas nuevas:** El pipeline continúa pero no publicará (0 rows).

---

### Paso 7: Enrich Metadata

**Cuándo:** Después de calcular delta, antes de escribir outputs.

**Qué se guarda:** Nada (solo procesamiento en memoria).

**Propósito:** Agregar columnas de metadata estándar (dataset_id, provider, version, etc.).

---

### Paso 8: Write Outputs

**Cuándo:** Después de enriquecer metadata, antes de publicar.

**Qué se guarda:**
- `outputs/<version_ts>/data/year=YYYY/month=MM/part-0.parquet` - Datos publicados particionados

**Código:**
```python
# ingestor_reader/use_cases/run_pipeline.py:102-104
output_keys, rows_added = step_write_outputs(
    catalog, config, run_id, version_ts, normalized_df, enriched_delta_df, delta_df
)
```

**Detalles:**
- Se escriben múltiples archivos Parquet (uno por partición año/mes)
- Solo se escriben las filas del delta (nuevas)
- Se retorna la lista de keys escritas

**Si falla:** Los archivos escritos quedan huérfanos, pero no afectan el consumo (aún no están publicados).

---

### Paso 9: Publish Version

**Cuándo:** Después de escribir outputs, antes de notificar.

**Qué se guarda:**
1. `versions/<version_ts>/manifest.json` - Manifest de la versión
2. `current/manifest.json` - Puntero actualizado (con CAS)
3. `index/keys.parquet` - Índice actualizado (solo si CAS exitoso)

**Código:**
```python
# ingestor_reader/use_cases/run_pipeline.py:108-111
published = step_publish_version(
    catalog, config, version_ts, source_file, output_keys, rows_added,
    current_index_df, delta_df
)
```

**Detalles:**
- **Orden crítico:**
  1. Escribir manifest de versión (seguro, bajo `versions/`)
  2. Intentar CAS update de `current/manifest.json` (con If-Match ETag)
  3. Si CAS exitoso: actualizar `index/keys.parquet`
  4. Si CAS falla: abortar, no actualizar índice

**Atomicidad:**
- El CAS garantiza que solo una ejecución concurrente puede actualizar el puntero
- Si el CAS falla, el índice no se actualiza (consistencia)
- Los outputs quedan huérfanos pero no afectan el consumo

**Si falla el CAS:** 
- El manifest de versión existe pero no es visible
- El puntero no cambia
- El índice no se actualiza
- Los outputs quedan huérfanos

**Si falla después del CAS:**
- El puntero apunta a la nueva versión
- El índice podría no estar actualizado (inconsistencia rara)

---

### Paso 10: Notify Consumers

**Cuándo:** Después de publicar exitosamente, al final del pipeline.

**Qué se guarda:** Nada (solo envía mensaje SNS).

**Código:**
```python
# ingestor_reader/use_cases/run_pipeline.py:114-115
if published:
    step_notify_consumers(catalog, publisher, config, app_config, version_ts)
```

**Propósito:** Notificar a consumidores vía SNS que hay una nueva versión disponible.

**Mensaje SNS:**
```json
{
  "type": "DATASET_UPDATED",
  "timestamp": "2025-11-09T04:33:34+00:00",
  "dataset_id": "bcra_infomondia_series",
  "manifest_pointer": "bcra_infomondia_series/versions/2025-11-09T04-33-36/manifest.json"
}
```

**Si falla:** Los datos ya están publicados, solo falta la notificación. Puede reintentarse o ignorarse.

---

## Flujo Completo del Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    EJECUCIÓN DEL PIPELINE                        │
└─────────────────────────────────────────────────────────────────┘

1. Fetch Resource
   └─> Guarda: runs/<run_id>/raw/<filename>
   
2. Check Source Changed
   └─> Lee: current/manifest.json (si existe)
   └─> Compara: hash del archivo fuente
   └─> Si no cambió: STOP (no guarda nada más)
   
3. Parse File
   └─> Procesa en memoria (no guarda)
   
4. Filter New Data
   └─> Lee: index/keys.parquet (si existe)
   └─> Filtra por fecha (lag_days)
   └─> Si no hay datos nuevos: STOP
   
5. Normalize Rows
   └─> Procesa en memoria (no guarda)
   
6. Compute Delta
   └─> Lee: index/keys.parquet (si existe)
   └─> Calcula anti-join
   └─> Si no hay filas nuevas: continúa (pero no publicará)
   
7. Enrich Metadata
   └─> Procesa en memoria (no guarda)
   
8. Write Outputs
   └─> Guarda: runs/<run_id>/staging/normalized.parquet
   └─> Guarda: runs/<run_id>/delta/added.parquet (si hay delta)
   └─> Guarda: outputs/<version_ts>/data/year=*/month=*/part-0.parquet
   
9. Publish Version
   └─> Guarda: versions/<version_ts>/manifest.json
   └─> CAS Update: current/manifest.json
   └─> Si CAS exitoso: Guarda: index/keys.parquet
   └─> Si CAS falla: Aborta (no actualiza índice)
   
10. Notify Consumers
    └─> Envía: Mensaje SNS (no guarda en S3)
```

---

## Atomicidad y Consistencia

### Estrategia de Atomicidad

El pipeline usa **CAS (Compare-And-Swap)** para garantizar atomicidad:

1. **Escribir outputs primero** (bajo `outputs/<version_ts>/`)
2. **Escribir manifest de versión** (bajo `versions/<version_ts>/`)
3. **CAS update del puntero** (`current/manifest.json` con If-Match ETag)
4. **Actualizar índice solo si CAS exitoso**

### Garantías

✅ **Atomicidad:** El puntero solo cambia si el CAS es exitoso

✅ **Consistencia:** El índice se actualiza solo después de un CAS exitoso

✅ **Idempotencia:** Re-ejecutar el pipeline es seguro (calcula delta correctamente)

✅ **Concurrencia:** Solo una ejecución puede publicar (CAS previene condiciones de carrera)

### Manejo de Errores

#### Escenario 1: Fallo antes de escribir outputs
- **Estado:** No se guardó nada
- **Resultado:** Limpio, reintentar es seguro

#### Escenario 2: Fallo después de escribir outputs pero antes de CAS
- **Estado:** 
  - ✅ Outputs escritos
  - ✅ Manifest de versión escrito
  - ❌ Puntero no actualizado
  - ❌ Índice no actualizado
- **Resultado:** Datos huérfanos (no visibles, no afectan consumo)
- **Acción:** Reintentar es seguro (calcula delta correctamente)

#### Escenario 3: Fallo durante CAS
- **Estado:** Igual que Escenario 2
- **Resultado:** Datos huérfanos
- **Acción:** Reintentar es seguro

#### Escenario 4: Fallo después de CAS exitoso pero antes de actualizar índice
- **Estado:**
  - ✅ Outputs escritos
  - ✅ Manifest de versión escrito
  - ✅ Puntero actualizado
  - ❌ Índice no actualizado
- **Resultado:** Inconsistencia (rara, pero posible)
- **Acción:** El pipeline debería manejar esto mejor (mejora futura)

#### Escenario 5: Fallo en notificación
- **Estado:** Todo exitoso
- **Resultado:** Datos publicados, solo falta notificación
- **Acción:** Reintentar notificación o ignorar

### Datos Huérfanos

Los datos huérfanos (escritos pero no publicados) quedan en:
- `outputs/<version_ts>/data/...` - No se consumen (puntero no apunta)
- `versions/<version_ts>/manifest.json` - Existe pero no se usa
- `runs/<run_id>/...` - Siempre se guardan para debugging

**Limpieza:**
- Pueden limpiarse manualmente
- No afectan el funcionamiento del pipeline
- Pueden ser útiles para debugging o reprocesamiento

---

## Resumen de Escrituras a S3

| Paso | Qué se Guarda | Cuándo | Condición |
|------|---------------|--------|-----------|
| 1. Fetch | Nada (solo descarga en memoria) | Inmediatamente | Siempre |
| 2. Check | Nada (solo lectura) | Después de fetch | - |
| 3. Parse | Nada | Después de check | - |
| 4. Filter | Nada (solo lectura) | Después de parse | - |
| 5. Normalize | Nada | Después de filter | - |
| 6. Delta | Nada (solo lectura) | Después de normalize | - |
| 7. Enrich | Nada | Después de delta | - |
| 8. Write | `outputs/<version_ts>/data/year=*/month=*/part-0.parquet` | Después de enrich | Si hay filas nuevas |
| 9. Publish | `versions/<version_ts>/manifest.json`<br>`current/manifest.json` (CAS)<br>`index/keys.parquet` | Después de write | Si CAS exitoso |
| 10. Notify | Nada (SNS) | Después de publish | Si publicado |

---

## Notas Finales

1. **Incremental:** Los outputs solo contienen filas nuevas (delta), no el dataset completo
2. **Atomicidad:** El puntero `current/manifest.json` se actualiza con CAS para garantizar atomicidad
3. **Consistencia:** El índice se actualiza solo después de una publicación exitosa
4. **Metadata:** Los outputs finales incluyen todas las columnas de metadata estándar
5. **Estructura simplificada:** El pipeline solo escribe datos esenciales, eliminando carpetas de debugging (`runs/`)
6. **Particionamiento:** Los datos se particionan automáticamente por año/mes para consultas eficientes
7. **Retención:** El pipeline no elimina archivos automáticamente (limpieza manual recomendada)

