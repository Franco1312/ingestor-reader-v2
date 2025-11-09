# Escenarios de Reinicio del Sistema

Este documento describe qué sucede si el sistema se reinicia durante la ejecución del pipeline.

## Resumen Ejecutivo

El pipeline está diseñado para ser **resiliente ante reinicios**:

✅ **Locks**: Se liberan automáticamente por TTL (1 hora por defecto)
✅ **Datos huérfanos**: No afectan el consumo (no son visibles)
✅ **Idempotencia**: Re-ejecutar es seguro
⚠️ **Consolidación parcial**: Puede requerir regeneración manual

---

## Escenarios de Reinicio

### Escenario 1: Reinicio antes de escribir eventos

**Momento del reinicio:** Durante fetch, parse, normalize, o compute delta (antes de `step_write_events`)

**Estado:**
- ❌ No se escribió nada a S3
- ❌ Lock no adquirido o ya liberado

**Resultado:**
- ✅ **Limpio**: No hay datos huérfanos
- ✅ **Lock**: No hay lock bloqueante (o expira por TTL)

**Acción:**
- Simplemente reintentar el pipeline
- El pipeline calculará el delta correctamente desde el índice actual

---

### Escenario 2: Reinicio después de escribir eventos, antes de CAS

**Momento del reinicio:** Después de `step_write_events` pero antes de `step_publish_version` (CAS)

**Estado:**
- ✅ Eventos escritos en `events/<version_ts>/data/...`
- ✅ Manifest de evento escrito en `events/<version_ts>/manifest.json`
- ❌ Puntero `current/manifest.json` **NO** actualizado
- ❌ Índice `index/keys.parquet` **NO** actualizado
- ❌ Proyecciones **NO** consolidadas
- ⚠️ Lock todavía activo (expirará por TTL en 1 hora)

**Resultado:**
- ✅ **Datos huérfanos**: Existen pero no son visibles (el puntero no apunta a ellos)
- ✅ **Consumo no afectado**: Los consumidores siguen leyendo la versión anterior
- ⚠️ **Lock bloqueante**: Otro proceso no podrá ejecutar hasta que expire el TTL (1 hora)

**Acción:**
1. **Esperar TTL del lock** (1 hora) o **liberar manualmente** el lock en DynamoDB
2. **Reintentar el pipeline**: 
   - Calculará el delta correctamente (usando el índice anterior)
   - Escribirá nuevos eventos con un nuevo `version_ts`
   - Los datos huérfanos quedarán en S3 pero no se consumirán

**Limpieza opcional:**
- Los eventos huérfanos pueden eliminarse manualmente:
  ```bash
  aws s3 rm s3://bucket/datasets/<dataset_id>/events/<version_ts>/ --recursive
  ```

---

### Escenario 3: Reinicio durante CAS (publish_version)

**Momento del reinicio:** Durante `step_publish_version` (CAS)

**Estado:**
- ✅ Eventos escritos
- ✅ Manifest de evento escrito
- ❓ CAS puede haber fallado o no haberse intentado
- ❌ Puntero **NO** actualizado (o inconsistente)
- ❌ Índice **NO** actualizado
- ❌ Proyecciones **NO** consolidadas
- ⚠️ Lock todavía activo

**Resultado:**
- Igual que Escenario 2: datos huérfanos
- El CAS es atómico, así que o se completa o no se completa

**Acción:**
- Igual que Escenario 2

---

### Escenario 4: Reinicio después de CAS exitoso, antes de consolidación

**Momento del reinicio:** Después de `step_publish_version` exitoso pero antes de `step_consolidate_projection`

**Estado:**
- ✅ Eventos escritos
- ✅ Manifest de evento escrito
- ✅ Puntero actualizado (CAS exitoso)
- ✅ Índice actualizado
- ❌ Proyecciones **NO** consolidadas o **parcialmente** consolidadas
- ⚠️ Lock todavía activo

**Resultado:**
- ✅ **Datos publicados**: Los eventos son visibles (el puntero apunta a ellos)
- ✅ **Proyecciones recuperables**: 
  - La consolidación es idempotente y resiliente ante reinicios
  - La próxima ejecución detectará el estado incompleto y re-ejecutará automáticamente
- ⚠️ **Lock bloqueante**: Otro proceso no podrá ejecutar hasta que expire el TTL

**Acción:**
1. **Esperar TTL del lock** o **liberar manualmente**
2. **Reintentar el pipeline**:
   - El pipeline detectará que el archivo fuente no cambió (si no cambió)
   - La consolidación se re-ejecutará automáticamente (idempotente)
   - Las proyecciones se regenerarán desde eventos (consistencia garantizada)

**Nota:** Con la nueva implementación de consolidación resiliente, este escenario ya no es problemático. Las proyecciones se regenerarán automáticamente desde eventos.

---

### Escenario 5: Reinicio durante consolidación

**Momento del reinicio:** Durante `step_consolidate_projection`

**Estado:**
- ✅ Eventos escritos y publicados
- ✅ Puntero e índice actualizados
- ⚠️ Proyecciones **parcialmente** consolidadas:
  - Algunos meses pueden estar consolidados (manifest en `completed`)
  - Otros meses pueden estar en progreso (manifest en `in_progress` o archivos en `.tmp/`)
- ⚠️ Lock todavía activo

**Resultado:**
- ✅ **Datos publicados**: Los eventos son visibles
- ✅ **Proyecciones recuperables**: 
  - La consolidación es idempotente y resiliente ante reinicios
  - Los meses con manifest `completed` se omitirán (ya consolidados)
  - Los meses con manifest `in_progress` o sin manifest se re-ejecutarán automáticamente
- ⚠️ **Lock bloqueante**

**Acción:**
1. **Esperar TTL del lock** o **liberar manualmente**
2. **Reintentar el pipeline**:
   - Si el archivo fuente no cambió, el pipeline detectará "no changes" y no procesará
   - **O ejecutar consolidación manual**:
     ```python
     # La consolidación es idempotente, puede ejecutarse de forma segura
     consolidate_projection_step(catalog, config, enriched_delta_df)
     ```

**Nota:** Con la nueva implementación de consolidación resiliente, este escenario se maneja automáticamente. Los meses incompletos se re-ejecutarán, los completos se omitirán.

---

### Escenario 6: Reinicio después de consolidación, antes de notificación

**Momento del reinicio:** Después de `step_consolidate_projection` pero antes de `step_notify_consumers`

**Estado:**
- ✅ Todo exitoso
- ✅ Eventos publicados
- ✅ Proyecciones consolidadas
- ❌ Notificación **NO** enviada
- ⚠️ Lock todavía activo

**Resultado:**
- ✅ **Datos completos**: Todo está correcto
- ⚠️ **Consumidores no notificados**: No recibirán notificación de actualización
- ⚠️ **Lock bloqueante**

**Acción:**
1. **Esperar TTL del lock** o **liberar manualmente**
2. **Reintentar notificación** (opcional):
   - Los datos ya están disponibles
   - Los consumidores pueden descubrir actualizaciones por otros medios (polling, etc.)

---

## Manejo de Locks

### TTL (Time-To-Live)

Los locks tienen un **TTL de 1 hora por defecto**:

- Si el sistema se reinicia, el lock **expirará automáticamente** después de 1 hora
- Otro proceso podrá adquirir el lock después de la expiración

### Liberación Manual del Lock

Si necesitas liberar el lock inmediatamente (sin esperar TTL):

```python
from ingestor_reader.infra.locks import DynamoDBLock

lock_manager = DynamoDBLock(
    table_name="your-lock-table",
    region="us-east-1"
)

# Liberar lock manualmente
lock_manager.release("pipeline:bcra_infomondia_series", "run-id")
```

O desde AWS CLI:

```bash
aws dynamodb delete-item \
  --table-name your-lock-table \
  --key '{"lock_key": {"S": "pipeline:bcra_infomondia_series"}}'
```

**Nota:** Solo libera el lock si estás seguro de que el proceso original ya no está ejecutándose.

---

## Estrategias de Recuperación

### Estrategia 1: Reintentar Automáticamente

**Cuándo usar:** Escenarios 1, 2, 3

**Cómo:**
- Simplemente reintentar el pipeline
- El pipeline calculará el delta correctamente
- Los datos huérfanos no afectarán el cálculo

### Estrategia 2: Regenerar Proyecciones

**Cuándo usar:** Escenarios 4, 5

**Cómo:**
- Ejecutar consolidación manual para los meses afectados
- O reintentar el pipeline (si el archivo fuente cambió, procesará de nuevo)

### Estrategia 3: Limpiar Datos Huérfanos

**Cuándo usar:** Escenarios 2, 3 (opcional)

**Cómo:**
```bash
# Identificar eventos huérfanos (no referenciados en current/manifest.json)
current_manifest = catalog.read_current_manifest(dataset_id)
current_version = current_manifest["current_version"]

# Listar todos los eventos
all_events = catalog.list_events_for_month(dataset_id, year, month)

# Eliminar eventos no referenciados
for event_version in all_events:
    if event_version != current_version:
        # Verificar si está referenciado
        # Si no, eliminar
        aws s3 rm s3://bucket/datasets/<dataset_id>/events/<event_version>/ --recursive
```

---

## Mejoras Futuras

### 1. Heartbeat para Locks

**Problema:** Si el proceso se reinicia, el lock queda bloqueado hasta TTL.

**Solución:** Implementar heartbeat:
- El proceso actualiza el lock periódicamente (cada 5 minutos)
- Si el heartbeat se detiene, el lock expira más rápido

### 2. Verificación de Consistencia

**Problema:** No hay forma automática de detectar proyecciones inconsistentes.

**Solución:** Agregar verificación de consistencia:
- Comparar proyecciones con eventos
- Regenerar automáticamente si hay inconsistencias

**Nota:** La consolidación resiliente (WAL + manifests) ya está implementada y resuelve el problema de proyecciones inconsistentes ante reinicios.

---

## Resumen de Garantías

| Escenario | Datos Publicados | Proyecciones | Lock | Acción |
|-----------|------------------|--------------|------|--------|
| 1. Antes de escribir | ❌ | ❌ | ✅ | Reintentar |
| 2. Después de escribir | ❌ (huérfanos) | ❌ | ⚠️ (TTL) | Esperar TTL + Reintentar |
| 3. Durante CAS | ❌ (huérfanos) | ❌ | ⚠️ (TTL) | Esperar TTL + Reintentar |
| 4. Después de CAS | ✅ | ✅ (recuperables) | ⚠️ (TTL) | Esperar TTL + Reintentar (auto-recupera) |
| 5. Durante consolidación | ✅ | ✅ (recuperables) | ⚠️ (TTL) | Esperar TTL + Reintentar (auto-recupera) |
| 6. Después de consolidación | ✅ | ✅ | ⚠️ (TTL) | Esperar TTL (opcional: notificar) |

---

## Conclusión

El pipeline es **resiliente ante reinicios**:

✅ **Datos seguros**: El CAS previene publicaciones parciales
✅ **Idempotencia**: Reintentar es seguro
✅ **Locks automáticos**: TTL previene deadlocks permanentes
✅ **Proyecciones resilientes**: WAL + manifests garantizan atomicidad e idempotencia

**Recomendación:** 
- Para producción, considera implementar heartbeat para locks
- Monitorea proyecciones inconsistentes (aunque ahora se recuperan automáticamente)
- Implementa verificación de consistencia periódica (mejora futura)

**Ver también:** [Consolidación Resiliente](./CONSOLIDATION_RESILIENCE.md) para más detalles sobre la implementación de WAL y manifests.


