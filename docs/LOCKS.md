# Sistema de Locks Distribuidos

## Resumen

El sistema utiliza **locks distribuidos** basados en **DynamoDB** para prevenir ejecuciones concurrentes del pipeline y consolidaciones simultáneas de las mismas proyecciones.

## Propósito

Los locks previenen **ejecuciones concurrentes del pipeline**: Si dos pipelines para el mismo dataset se ejecutan simultáneamente, solo uno procede.

**Nota**: La consolidación no requiere un lock separado porque solo ocurre dentro del pipeline, que ya está protegido por el lock del pipeline.

## Configuración

### Tabla DynamoDB

Se requiere una tabla DynamoDB con la siguiente estructura:

- **Partition Key**: `lock_key` (String)
- **Atributos**:
  - `owner_id` (String): Identificador único del proceso que adquirió el lock
  - `expires_at` (Number): Timestamp Unix de expiración del lock
  - `acquired_at` (Number): Timestamp Unix de adquisición del lock

### Configuración en AppConfig

```python
app_config = AppConfig(
    dynamodb_lock_table="ingestor-locks",  # Nombre de la tabla DynamoDB
    # ... otros campos
)
```

Si `dynamodb_lock_table` es `None`, los locks están deshabilitados y el pipeline se ejecuta sin protección contra concurrencia.

## Funcionamiento

### 1. Lock del Pipeline

**Cuándo se adquiere**: Al inicio de `run_pipeline()`

**Lock Key**: `pipeline:{dataset_id}`

**Ejemplo**: `pipeline:bcra_infomondia_series`

**Comportamiento**:
- Si el lock está disponible → se adquiere y el pipeline continúa
- Si el lock está ocupado → el pipeline se salta con un warning
- El lock se libera automáticamente al finalizar (éxito o error)

**Código**:
```python
lock_key = f"pipeline:{config.dataset_id}"
lock_acquired = False
if lock_manager:
    lock_acquired = lock_manager.acquire(lock_key, run_id)
    if not lock_acquired:
        logger.warning("Pipeline already running for %s, skipping execution", config.dataset_id)
        return run

try:
    # Ejecutar pipeline...
finally:
    if lock_manager and lock_acquired:
        lock_manager.release(lock_key, run_id)
```


## Mecanismo de Adquisición

### Condición Atómica

Los locks se adquieren usando **conditional writes** de DynamoDB:

```python
ConditionExpression="attribute_not_exists(lock_key) OR expires_at < :now"
```

Esto significa:
- El lock se adquiere si **no existe** el item
- O si el lock **ya expiró** (permite recuperación automática)

### TTL (Time-To-Live)

Por defecto, los locks expiran después de **1 hora** (3600 segundos).

**Ventajas**:
- Previene deadlocks si un proceso falla sin liberar el lock
- Permite recuperación automática después de un timeout

**Configuración**:
```python
DynamoDBLock(table_name="locks", ttl_seconds=3600)
```

## Liberación de Locks

### Liberación Explícita

Los locks se liberan explícitamente usando `try/finally`:

```python
try:
    # Trabajo protegido por lock
    ...
finally:
    lock_manager.release(lock_key, owner_id)
```

### Verificación de Propiedad

La liberación solo funciona si el `owner_id` coincide:

```python
ConditionExpression="owner_id = :owner"
```

Esto previene que un proceso libere un lock adquirido por otro proceso.

## Casos de Uso

### Caso 1: Pipeline Concurrente

**Escenario**: Dos pipelines para `bcra_infomondia_series` se ejecutan simultáneamente.

**Resultado**:
- Pipeline A adquiere el lock → continúa
- Pipeline B intenta adquirir → falla → se salta

**Log**:
```
Pipeline A: "Acquired lock for pipeline:bcra_infomondia_series (owner: run-123)"
Pipeline B: "Lock already acquired for pipeline:bcra_infomondia_series"
Pipeline B: "Pipeline already running for bcra_infomondia_series, skipping execution"
```

### Caso 2: Fallo del Proceso

**Escenario**: Un pipeline falla sin liberar el lock.

**Resultado**:
- El lock expira después de 1 hora (TTL)
- Otros pipelines pueden adquirir el lock después de la expiración

**Log**:
```
Pipeline A: "Acquired lock for pipeline:bcra_infomondia_series (owner: run-123)"
Pipeline A: [FALLA SIN LIBERAR LOCK]
[1 hora después]
Pipeline B: "Acquired lock for pipeline:bcra_infomondia_series (owner: run-456)"
```

## Deshabilitar Locks

Para deshabilitar los locks, simplemente no configures `dynamodb_lock_table`:

```python
app_config = AppConfig(
    dynamodb_lock_table=None,  # Locks deshabilitados
    # ... otros campos
)
```

**Comportamiento**:
- El pipeline se ejecuta sin protección contra concurrencia
- Las consolidaciones se ejecutan sin locks
- Útil para desarrollo local o entornos sin DynamoDB

## Estructura de la Tabla DynamoDB

### Esquema

```json
{
  "lock_key": "pipeline:bcra_infomondia_series",
  "owner_id": "run-1701234567",
  "expires_at": 1701238167,
  "acquired_at": 1701234567
}
```

### Ejemplo de Item

```json
{
  "lock_key": "consolidate:bcra_infomondia_series:2024:11",
  "owner_id": "run-1701234567",
  "expires_at": 1701238167,
  "acquired_at": 1701234567
}
```

## Consideraciones

### Performance

- **Adquisición de lock**: ~10-50ms (operación DynamoDB)
- **Impacto mínimo**: Solo se adquiere al inicio del pipeline y por cada mes en consolidación

### Costos

- **DynamoDB**: ~$0.25 por millón de escrituras, ~$0.25 por millón de lecturas
- **Impacto**: Mínimo (2-3 operaciones por pipeline)

### Monitoreo

Los locks se registran en los logs:
- `INFO`: Lock adquirido/liberado exitosamente
- `WARNING`: Lock ya adquirido (concurrencia detectada)

## Implementación

### Clase DynamoDBLock

```python
class DynamoDBLock:
    def acquire(self, lock_key: str, owner_id: str) -> bool:
        """Adquiere un lock. Retorna True si exitoso, False si ya está ocupado."""
        
    def release(self, lock_key: str, owner_id: str) -> bool:
        """Libera un lock. Retorna True si exitoso, False si no existe o owner no coincide."""
        
    def is_locked(self, lock_key: str) -> bool:
        """Verifica si un lock está actualmente activo."""
```

### Uso en el Pipeline

```python
# Inicialización
lock_manager = DynamoDBLock(
    table_name=app_config.dynamodb_lock_table,
    ttl_seconds=3600
)

# Adquisición
if lock_manager.acquire(lock_key, owner_id):
    try:
        # Trabajo protegido
        ...
    finally:
        lock_manager.release(lock_key, owner_id)
```

## Preguntas Frecuentes


### ¿Qué pasa si un pipeline falla a mitad de consolidación?

El lock del pipeline se libera en el `finally`, pero si el proceso muere, el lock expira después de 1 hora (TTL). La consolidación está protegida por el lock del pipeline, por lo que no puede haber consolidaciones concurrentes.

### ¿Puedo cambiar el TTL?

Sí, al inicializar `DynamoDBLock`:

```python
lock_manager = DynamoDBLock(
    table_name="locks",
    ttl_seconds=7200  # 2 horas
)
```

### ¿Los locks afectan el rendimiento?

Mínimamente. Solo se adquieren al inicio del pipeline y por cada mes en consolidación. El overhead es de ~10-50ms por operación DynamoDB.

