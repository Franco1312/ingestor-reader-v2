# Flujo del Sistema: Secuencia Completa

## Flujo del Pipeline (Escritura)

### Secuencia Paso a Paso

```
┌─────────────────────────────────────────────────────────────────┐
│                    PIPELINE DE INGESTA                            │
└─────────────────────────────────────────────────────────────────┘

1. [Pipeline] → Fetch Resource
   └─> Descarga archivo fuente desde URL
   └─> Calcula hash SHA256
   └─> Retorna: content, file_hash, file_size

2. [Pipeline] → Check Source Changed
   └─> Lee: current/manifest.json (si existe)
   └─> Compara hash del archivo fuente
   └─> Si no cambió: STOP (no procesa)
   └─> Si cambió: CONTINUA

3. [Pipeline] → Parse File
   └─> Parsea archivo (Excel, CSV, etc.)
   └─> Convierte a DataFrame
   └─> Retorna: parsed_df

4. [Pipeline] → Filter New Data
   └─> Lee: index/keys.parquet (si existe)
   └─> Filtra por fecha (lag_days)
   └─> Si no hay datos nuevos: STOP
   └─> Si hay datos nuevos: CONTINUA

5. [Pipeline] → Normalize Rows
   └─> Normaliza datos según plugin
   └─> Retorna: normalized_df

6. [Pipeline] → Compute Delta
   └─> Lee: index/keys.parquet
   └─> Calcula anti-join (identifica filas nuevas)
   └─> Retorna: delta_df, current_index_df

7. [Pipeline] → Enrich Metadata
   └─> Agrega columnas de metadata (dataset_id, provider, version, etc.)
   └─> Retorna: enriched_delta_df

8. [Pipeline] → Write Event
   └─> Escribe: events/<version_ts>/data/year=YYYY/month=MM/part-0.parquet
   └─> Cada versión escribe su propio evento (inmutable)
   └─> Retorna: event_keys

9. [Pipeline] → Publish Event
   └─> Escribe: events/<version_ts>/manifest.json
   └─> CAS Update: current/manifest.json (con If-Match ETag)
   └─> Si CAS exitoso: actualiza index/keys.parquet
   └─> Si CAS falla: ABORTA (no actualiza índice)

10. [Pipeline] → Consolidate Projection
    └─> Para cada año/mes afectado:
        ├─> Lista: events/*/data/year=YYYY/month=MM/part-0.parquet
        ├─> Lee: todos los eventos del mes
        ├─> Consolida: concatena y elimina duplicados
        └─> Escribe: projections/windows/year=YYYY/month=MM/data.parquet
    └─> Sobrescribe proyección anterior (regenera desde eventos)

11. [Pipeline] → Update Metadata
    └─> Escribe: metadata/current.json
    └─> Actualiza: last_update, total_rows, current_version

12. [Pipeline] → Notify Consumers
    └─> Envía: mensaje SNS
    └─> Notifica: nueva versión disponible
```

## Flujo de Consolidación (Detalle)

### Secuencia de Consolidación de un Mes

```
┌─────────────────────────────────────────────────────────────────┐
│              CONSOLIDACIÓN DE PROYECCIÓN MENSUAL                 │
└─────────────────────────────────────────────────────────────────┘

Para consolidar: year=2024, month=11

1. [ConsolidationService] → List Events
   └─> Busca: events/*/data/year=2024/month=11/part-0.parquet
   └─> Retorna: [
         "events/2025-11-01T10-00-00/data/year=2024/month=11/part-0.parquet",
         "events/2025-11-02T10-00-00/data/year=2024/month=11/part-0.parquet",
         "events/2025-11-03T10-00-00/data/year=2024/month=11/part-0.parquet",
         ...
       ]

2. [ConsolidationService] → Read All Events
   └─> Para cada evento:
       ├─> Lee: events/<version>/data/year=2024/month=11/part-0.parquet
       └─> Agrega a lista: all_events = [df1, df2, df3, ...]

3. [ConsolidationService] → Consolidate
   └─> Concatena: consolidated = pd.concat(all_events)
   └─> Elimina duplicados: consolidated.drop_duplicates(primary_keys, keep="last")
   └─> Retorna: consolidated_df

4. [ConsolidationService] → Write Projection
   └─> Escribe: projections/windows/year=2024/month=11/data.parquet
   └─> Sobrescribe proyección anterior (regenera desde cero)
```

## Flujo de Lectura (Consumidor)

### Secuencia de Lectura de un Mes

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUMIDOR LEE DATOS                           │
└─────────────────────────────────────────────────────────────────┘

Consumidor quiere leer: year=2024, month=11

1. [Consumer] → Read Projection
   └─> Lee: projections/windows/year=2024/month=11/data.parquet
   └─> Retorna: DataFrame con todos los datos del mes consolidados

2. [Consumer] → Process Data
   └─> Procesa DataFrame
   └─> Usa datos para análisis, reportes, etc.
```

### Secuencia de Lectura de un Año

```
Consumidor quiere leer: year=2024

1. [Consumer] → Read All Monthly Projections
   └─> Para cada mes (1-12):
       ├─> Lee: projections/windows/year=2024/month=01/data.parquet
       ├─> Lee: projections/windows/year=2024/month=02/data.parquet
       ├─> ...
       └─> Lee: projections/windows/year=2024/month=12/data.parquet

2. [Consumer] → Concatenate
   └─> Concatena: complete_year = pd.concat([df1, df2, ..., df12])
   └─> Retorna: DataFrame con todos los datos del año
```

## Flujo Completo con Ejemplo

### Ejemplo: Primera Ejecución (Día 1)

```
┌─────────────────────────────────────────────────────────────────┐
│                    PRIMERA EJECUCIÓN                              │
└─────────────────────────────────────────────────────────────────┘

Pipeline ejecuta:
├─> 1. Fetch: Descarga archivo (1000 filas)
├─> 2. Check: No hay manifest anterior → CONTINUA
├─> 3. Parse: Parsea 1000 filas
├─> 4. Filter: No hay índice → procesa todas
├─> 5. Normalize: Normaliza 1000 filas
├─> 6. Delta: No hay índice → todas son nuevas (1000 filas)
├─> 7. Enrich: Enriquece 1000 filas
├─> 8. Write Event:
│   └─> Escribe: events/2025-11-01T10-00-00/data/year=2024/month=11/part-0.parquet
│   └─> Contiene: 1000 filas
├─> 9. Publish Event:
│   ├─> Escribe: events/2025-11-01T10-00-00/manifest.json
│   ├─> CAS Update: current/manifest.json → "2025-11-01T10-00-00"
│   └─> Actualiza: index/keys.parquet (1000 hashes)
├─> 10. Consolidate Projection:
│   ├─> Lista eventos: [events/2025-11-01T10-00-00/data/year=2024/month=11/part-0.parquet]
│   ├─> Lee: 1000 filas
│   ├─> Consolida: 1000 filas (sin duplicados)
│   └─> Escribe: projections/windows/year=2024/month=11/data.parquet (1000 filas)
└─> 11. Notify: Envía SNS

Resultado:
├─> events/2025-11-01T10-00-00/ → Evento con 1000 filas
├─> projections/windows/year=2024/month=11/data.parquet → Proyección con 1000 filas
└─> index/keys.parquet → 1000 hashes
```

### Ejemplo: Segunda Ejecución (Día 2)

```
┌─────────────────────────────────────────────────────────────────┐
│                    SEGUNDA EJECUCIÓN (2 filas nuevas)            │
└─────────────────────────────────────────────────────────────────┘

Pipeline ejecuta:
├─> 1. Fetch: Descarga archivo (1002 filas)
├─> 2. Check: Hash cambió → CONTINUA
├─> 3. Parse: Parsea 1002 filas
├─> 4. Filter: Filtra por fecha → 1002 filas
├─> 5. Normalize: Normaliza 1002 filas
├─> 6. Delta:
│   ├─> Lee: index/keys.parquet (1000 hashes)
│   ├─> Compara: 1002 filas vs 1000 hashes
│   └─> Identifica: 2 filas nuevas
├─> 7. Enrich: Enriquece 2 filas
├─> 8. Write Event:
│   └─> Escribe: events/2025-11-02T10-00-00/data/year=2024/month=11/part-0.parquet
│   └─> Contiene: 2 filas nuevas
├─> 9. Publish Event:
│   ├─> Escribe: events/2025-11-02T10-00-00/manifest.json
│   ├─> CAS Update: current/manifest.json → "2025-11-02T10-00-00"
│   └─> Actualiza: index/keys.parquet (1002 hashes)
├─> 10. Consolidate Projection:
│   ├─> Lista eventos: [
│   │     "events/2025-11-01T10-00-00/data/year=2024/month=11/part-0.parquet",
│   │     "events/2025-11-02T10-00-00/data/year=2024/month=11/part-0.parquet"
│   │   ]
│   ├─> Lee: 1000 filas (evento 1) + 2 filas (evento 2) = 1002 filas
│   ├─> Consolida: 1002 filas (sin duplicados)
│   └─> Escribe: projections/windows/year=2024/month=11/data.parquet (1002 filas)
└─> 11. Notify: Envía SNS

Resultado:
├─> events/2025-11-01T10-00-00/ → Evento con 1000 filas (inmutable)
├─> events/2025-11-02T10-00-00/ → Evento con 2 filas nuevas (inmutable)
├─> projections/windows/year=2024/month=11/data.parquet → Proyección con 1002 filas (regenerada)
└─> index/keys.parquet → 1002 hashes
```

### Ejemplo: Consumidor Lee Datos

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONSUMIDOR LEE NOVIEMBRE 2024                 │
└─────────────────────────────────────────────────────────────────┘

Consumidor ejecuta:
├─> 1. Read Projection:
│   └─> Lee: projections/windows/year=2024/month=11/data.parquet
│   └─> Obtiene: DataFrame con 1002 filas (todas consolidadas)
│
└─> 2. Process:
    └─> Procesa 1002 filas
    └─> Genera reportes, análisis, etc.

Simple: Una sola lectura, sin necesidad de leer múltiples versiones.
```

## Diagrama de Secuencia Completo

```
Pipeline          EventStore        ProjectionManager    ConsolidationService    Consumer
   │                   │                    │                    │                  │
   │──Fetch───────────>│                    │                    │                  │
   │<──content─────────│                    │                    │                  │
   │                   │                    │                    │                  │
   │──Compute Delta───>│                    │                    │                  │
   │<──delta_df────────│                    │                    │                  │
   │                   │                    │                    │                  │
   │──Write Event─────>│                    │                    │                  │
   │                   │──Write────────────>│                    │                  │
   │                   │<──OK───────────────│                    │                  │
   │<──event_keys──────│                    │                    │                  │
   │                   │                    │                    │                  │
   │──Publish Event───>│                    │                    │                  │
   │                   │──Write Manifest──>│                    │                  │
   │                   │──CAS Update───────>│                    │                  │
   │                   │──Update Index─────>│                    │                  │
   │<──published───────│                    │                    │                  │
   │                   │                    │                    │                  │
   │──Consolidate─────>│                    │                    │                  │
   │                   │                    │                    │──List Events────>│
   │                   │                    │                    │<──event_list─────│
   │                   │                    │                    │──Read Events────>│
   │                   │<──Read─────────────│                    │                  │
   │                   │──Data─────────────>│                    │                  │
   │                   │                    │                    │<──all_events─────│
   │                   │                    │                    │──Consolidate────>│
   │                   │                    │                    │<──consolidated───│
   │                   │                    │──Write Projection─>│                  │
   │                   │                    │<──OK───────────────│                  │
   │<──consolidated────│                    │                    │                  │
   │                   │                    │                    │                  │
   │                   │                    │                    │                  │
   │                   │                    │                    │                  │──Read Projection──>│
   │                   │                    │<──Read─────────────│                  │                  │
   │                   │                    │──Data─────────────>│                  │<──DataFrame───────│
   │                   │                    │                    │                  │                  │
```

## Puntos Clave del Flujo

### 1. Eventos (Inmutables)

- **Cada versión escribe su propio evento**
- **Eventos nunca se modifican**
- **Historial completo disponible**

### 2. Proyecciones (Regenerables)

- **Se regeneran desde eventos**
- **No se modifican incrementalmente**
- **Siempre reflejan todos los eventos**

### 3. Consolidación (Idempotente)

- **Puede ejecutarse múltiples veces**
- **Siempre produce el mismo resultado**
- **Regenera desde cero cada vez**

### 4. Lectura (Simple)

- **Una lectura por mes**
- **Sin necesidad de leer múltiples versiones**
- **Datos ya consolidados**

## Ventajas del Flujo

✅ **Separación clara**: Eventos para auditoría, proyecciones para lectura
✅ **Inmutabilidad**: Eventos nunca se modifican
✅ **Consistencia**: Proyecciones siempre derivadas de eventos
✅ **Simplicidad**: Lectura simple para consumidores
✅ **Trazabilidad**: Historial completo disponible

