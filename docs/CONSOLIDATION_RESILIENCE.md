# Consolidación Resiliente ante Reinicios

## Resumen

La consolidación de proyecciones ahora es **resiliente ante reinicios del sistema** usando un patrón **Write-Ahead Log (WAL)** y **manifests de consolidación**.

## Problema Resuelto

**Antes:** Si el sistema se reiniciaba durante la consolidación, algunas proyecciones quedaban actualizadas y otras no, causando inconsistencias.

**Ahora:** La consolidación es atómica e idempotente. Si falla, puede re-ejecutarse de forma segura.

---

## Solución Implementada

### 1. Write-Ahead Log (WAL) Pattern

**Estrategia:** Escribir primero a un directorio temporal (`.tmp/`), luego mover atómicamente a la ubicación final.

**Flujo:**
1. Escribir todas las proyecciones a `.tmp/data.parquet`
2. Verificar que todas se escribieron correctamente
3. Mover atómicamente de `.tmp/` a la ubicación final
4. Limpiar `.tmp/` después del movimiento exitoso

**Estructura en S3:**
```
projections/windows/{series_code}/year={year}/month={month}/
  ├── data.parquet          # Proyección final (visible)
  └── .tmp/
      └── data.parquet      # Proyección temporal (en construcción)
```

### 2. Manifest de Consolidación

**Propósito:** Indicar el estado de consolidación para cada mes.

**Ubicación:**
```
projections/consolidation/{year}/{month}/manifest.json
```

**Contenido:**
```json
{
  "dataset_id": "bcra_infomondia_series",
  "year": 2007,
  "month": 8,
  "status": "completed",
  "timestamp": "2025-11-09T19:30:45Z"
}
```

**Estados:**
- `in_progress`: Consolidación en curso
- `completed`: Consolidación completada exitosamente

### 3. Idempotencia

**Comportamiento:** Antes de consolidar, se verifica el manifest:
- Si `status == "completed"` → se omite la consolidación
- Si no existe o `status == "in_progress"` → se ejecuta la consolidación

**Ventaja:** Permite re-ejecutar la consolidación de forma segura.

---

## Flujo de Consolidación

### Paso 1: Verificar Manifest

```python
manifest = catalog.read_consolidation_manifest(dataset_id, year, month)
if manifest and manifest.get("status") == "completed":
    # Ya consolidado, omitir
    return
```

### Paso 2: Limpiar Archivos Temporales

```python
# Limpiar cualquier archivo .tmp/ huérfano de ejecuciones anteriores
catalog.cleanup_temp_projections(dataset_id, year, month)
```

### Paso 3: Marcar como "in_progress"

```python
catalog.write_consolidation_manifest(dataset_id, year, month, status="in_progress")
```

### Paso 4: Consolidar Eventos

```python
# Leer todos los eventos del mes y consolidar por serie
series_projections = consolidate_month_projections(...)
```

### Paso 5: Escribir a Temporal (WAL)

```python
# Escribir todas las proyecciones a .tmp/
for series_code, df in series_projections.items():
    catalog.write_series_projection_temp(dataset_id, series_code, year, month, df)
```

### Paso 6: Mover Atómicamente

```python
# Mover todas las proyecciones de .tmp/ a final
for series_code in series_projections.keys():
    catalog.move_series_projection_from_temp(dataset_id, series_code, year, month)
```

### Paso 7: Marcar como "completed"

```python
catalog.write_consolidation_manifest(dataset_id, year, month, status="completed")
```

### Manejo de Errores

Si ocurre un error en cualquier paso:
1. Se limpian los archivos temporales (`.tmp/`)
2. El manifest queda en `in_progress` (o no existe)
3. La próxima ejecución detectará el estado incompleto y re-ejecutará

---

## Escenarios de Reinicio

### Escenario 1: Reinicio antes de escribir a .tmp/

**Estado:**
- ❌ No se escribió nada
- ❌ Manifest no existe o está en `in_progress`

**Resultado:**
- ✅ Limpio, reintentar es seguro
- ✅ La próxima ejecución consolidará normalmente

### Escenario 2: Reinicio después de escribir a .tmp/, antes de mover

**Estado:**
- ✅ Proyecciones escritas en `.tmp/`
- ✅ Manifest en `in_progress`
- ❌ Proyecciones no movidas a final

**Resultado:**
- ✅ La próxima ejecución:
  1. Limpiará `.tmp/` (paso 2)
  2. Re-ejecutará la consolidación completa
  3. Escribirá nuevas proyecciones a `.tmp/`
  4. Moverá a final

### Escenario 3: Reinicio durante el movimiento

**Estado:**
- ✅ Algunas proyecciones movidas a final
- ✅ Otras todavía en `.tmp/`
- ✅ Manifest en `in_progress`

**Resultado:**
- ✅ La próxima ejecución:
  1. Limpiará `.tmp/` (incluye las que no se movieron)
  2. Re-ejecutará la consolidación completa
  3. Todas las proyecciones se regenerarán desde eventos (consistencia garantizada)

### Escenario 4: Reinicio después de mover, antes de marcar "completed"

**Estado:**
- ✅ Todas las proyecciones movidas a final
- ✅ Manifest en `in_progress`

**Resultado:**
- ✅ La próxima ejecución:
  1. Verificará el manifest (paso 1)
  2. Como está en `in_progress`, re-ejecutará
  3. Regenerará proyecciones desde eventos (idempotente)
  4. Marcará como `completed`

**Nota:** Este escenario es seguro porque la consolidación es idempotente (regenera desde eventos).

---

## Garantías

✅ **Atomicidad:** Todas las proyecciones se actualizan juntas o ninguna

✅ **Idempotencia:** Re-ejecutar la consolidación es seguro

✅ **Consistencia:** Las proyecciones siempre coinciden con los eventos

✅ **Recuperabilidad:** Si falla, puede re-ejecutarse automáticamente

✅ **Simplicidad:** Código simple y fácil de mantener

---

## Métodos Agregados a S3Catalog

### `write_series_projection_temp()`
Escribe una proyección a la ubicación temporal (`.tmp/data.parquet`).

### `move_series_projection_from_temp()`
Mueve una proyección de temporal a final (atómico).

### `cleanup_temp_projections()`
Limpia todos los archivos temporales de un mes.

### `read_consolidation_manifest()`
Lee el manifest de consolidación de un mes.

### `write_consolidation_manifest()`
Escribe el manifest de consolidación de un mes.

---

## Consideraciones

### Performance

- **Escritura adicional:** Se escribe primero a `.tmp/` y luego se mueve (2 operaciones por proyección)
- **Impacto:** Mínimo, ya que las operaciones son secuenciales y rápidas
- **Beneficio:** Garantiza atomicidad y consistencia

### Espacio en S3

- **Archivos temporales:** Se limpian automáticamente después del movimiento
- **Manifests:** Ocupan ~200 bytes por mes (insignificante)

### Limpieza de Archivos Huérfanos

- Los archivos `.tmp/` se limpian automáticamente al inicio de cada consolidación
- Si quedan archivos huérfanos (muy raro), se limpian en la próxima ejecución

---

## Ejemplo de Uso

```python
# La consolidación se ejecuta automáticamente en el pipeline
# No requiere cambios en el código de uso

# Si necesitas consolidar manualmente:
from ingestor_reader.use_cases.steps.consolidate_projection import _consolidate_month

_consolidate_month(
    catalog=catalog,
    config=dataset_config,
    year=2007,
    month=8,
    primary_keys=["key1", "key2"]
)
```

---

## Conclusión

La consolidación ahora es **resiliente ante reinicios** usando:
1. **WAL pattern** para escrituras atómicas
2. **Manifests** para idempotencia
3. **Limpieza automática** de archivos temporales

**Resultado:** La consolidación puede re-ejecutarse de forma segura sin causar inconsistencias.

