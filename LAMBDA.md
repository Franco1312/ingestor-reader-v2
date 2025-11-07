# AWS Lambda Deployment

Este proyecto está listo para ejecutarse en AWS Lambda de forma autónoma.

## Handler Lambda

El punto de entrada único es: `ingestor_reader.app.lambda_handler.handler`

## Configuración de Lambda

### Variables de Entorno Requeridas

- `S3_BUCKET`: Nombre del bucket S3 donde se almacenan los datasets
- `AWS_REGION`: Región de AWS (opcional, por defecto: us-east-1)

### Variables de Entorno Opcionales

- `SNS_TOPIC_ARN`: ARN del topic SNS para notificaciones (opcional)
- `VERIFY_SSL`: Verificar certificados SSL (true/false, por defecto: true)
- `ENV`: Entorno (local, staging, production) - solo si no se usa S3_BUCKET

### Permisos IAM Requeridos

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:HeadObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name/*",
        "arn:aws:s3:::your-bucket-name"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sns:Publish"
      ],
      "Resource": "arn:aws:sns:*:*:your-topic-name"
    }
  ]
}
```

## Formato del Event

```json
{
  "dataset_id": "bcra_infomondia_series",
  "full_reload": false
}
```

### Parámetros

- `dataset_id` (requerido): ID del dataset a procesar
- `full_reload` (opcional): Si es `true`, procesa aunque el archivo fuente no haya cambiado

## Formato de Respuesta

### Éxito (200)

```json
{
  "statusCode": 200,
  "body": {
    "dataset_id": "bcra_infomondia_series",
    "run_id": "abc123...",
    "version_ts": "2025-11-07T12-30-45",
    "status": "completed"
  }
}
```

### Error (400/404/500)

```json
{
  "statusCode": 400,
  "body": {
    "error": "dataset_id is required"
  }
}
```

## Ejemplo de Invocación

### Desde AWS CLI

```bash
aws lambda invoke \
  --function-name ingestor-reader \
  --payload '{"dataset_id": "bcra_infomondia_series"}' \
  response.json
```

### Desde Python (boto3)

```python
import boto3
import json

lambda_client = boto3.client('lambda')

response = lambda_client.invoke(
    FunctionName='ingestor-reader',
    Payload=json.dumps({
        'dataset_id': 'bcra_infomondia_series',
        'full_reload': False
    })
)

result = json.loads(response['Payload'].read())
print(result)
```

### Desde EventBridge (Schedule)

```json
{
  "dataset_id": "bcra_infomondia_series"
}
```

## Características

- ✅ **Stateless**: No usa almacenamiento local, todo en S3
- ✅ **Variables de entorno**: Toda la configuración por variables
- ✅ **Logs simples**: Formato optimizado para CloudWatch
- ✅ **Mismo código**: Funciona igual localmente y en Lambda
- ✅ **Sin hardcodeos**: Sin rutas ni valores fijos

## Deployment

### Crear Deployment Package

```bash
# Instalar dependencias
pip install -r requirements.txt -t .

# Crear ZIP (excluir archivos innecesarios)
zip -r lambda-deployment.zip . \
  -x "*.pyc" \
  -x "__pycache__/*" \
  -x "*.git*" \
  -x "tests/*" \
  -x "docs/*" \
  -x "scripts/*" \
  -x "*.csv" \
  -x "*.lock"
```

### Crear/Actualizar Lambda Function

```bash
aws lambda create-function \
  --function-name ingestor-reader \
  --runtime python3.12 \
  --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
  --handler ingestor_reader.app.lambda_handler.handler \
  --zip-file fileb://lambda-deployment.zip \
  --timeout 900 \
  --memory-size 1024 \
  --environment Variables="{
    S3_BUCKET=your-bucket-name,
    AWS_REGION=us-east-1,
    SNS_TOPIC_ARN=arn:aws:sns:us-east-1:ACCOUNT:datasets-updated
  }"
```

## Testing Local

El mismo código funciona localmente usando el CLI:

```bash
export S3_BUCKET=your-bucket-name
export AWS_REGION=us-east-1
python -m ingestor_reader.app.main bcra_infomondia_series
```

