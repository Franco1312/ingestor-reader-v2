# Docker Deployment para AWS Lambda

Este proyecto está listo para ejecutarse en AWS Lambda usando una imagen Docker almacenada en ECR.

## Dockerfile

El `Dockerfile` está optimizado para AWS Lambda:

- Usa la imagen base oficial de AWS Lambda Python 3.12
- Copia solo los archivos necesarios (código y configs)
- Instala dependencias desde `pyproject.toml`
- Configura el handler Lambda correctamente

## Construcción de la Imagen

### Localmente

```bash
# Construir imagen
docker build -t ingestor-reader:latest .

# Probar localmente con Lambda Runtime Interface Emulator
docker run -p 9000:8080 \
  -e S3_BUCKET=your-bucket \
  -e AWS_REGION=us-east-1 \
  -e ENV=local \
  ingestor-reader:latest

# En otra terminal, invocar la función
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"dataset_id": "bcra_infomondia_series"}'
```

### Para ECR

```bash
# Autenticarse en ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com

# Construir imagen
docker build -t ingestor-reader:latest .

# Tag para ECR
docker tag ingestor-reader:latest \
  <account-id>.dkr.ecr.us-east-1.amazonaws.com/ingestor-reader:latest

# Push a ECR
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ingestor-reader:latest
```

## Configuración de Lambda Function

### Crear Function desde ECR

```bash
aws lambda create-function \
  --function-name ingestor-reader \
  --package-type Image \
  --code ImageUri=<account-id>.dkr.ecr.us-east-1.amazonaws.com/ingestor-reader:latest \
  --role arn:aws:iam::<account-id>:role/lambda-execution-role \
  --timeout 900 \
  --memory-size 1024 \
  --environment Variables="{
    S3_BUCKET=your-bucket-name,
    AWS_REGION=us-east-1,
    ENV=production,
    DYNAMODB_TABLE=etl-locks,
    SNS_TOPIC_ARN=arn:aws:sns:us-east-1:<account-id>:datasets-updated
  }"
```

### Actualizar Function

```bash
# Reconstruir y push
docker build -t ingestor-reader:latest .
docker tag ingestor-reader:latest \
  <account-id>.dkr.ecr.us-east-1.amazonaws.com/ingestor-reader:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ingestor-reader:latest

# Actualizar Lambda
aws lambda update-function-code \
  --function-name ingestor-reader \
  --image-uri <account-id>.dkr.ecr.us-east-1.amazonaws.com/ingestor-reader:latest
```

## Estructura del Dockerfile

```
FROM public.ecr.aws/lambda/python:3.12
  ↓
Copia código y configs
  ↓
Instala dependencias desde pyproject.toml
  ↓
Configura handler: ingestor_reader.app.lambda_handler.handler
```

## Archivos Incluidos

- `ingestor_reader/`: Todo el código del proyecto
- `config/`: Archivos de configuración (appconfig y datasets)
- `pyproject.toml`: Dependencias del proyecto

## Archivos Excluidos (.dockerignore)

- `__pycache__/`, `*.pyc`: Archivos compilados
- `venv/`, `env/`: Entornos virtuales
- `tests/`, `docs/`, `scripts/`: No necesarios en runtime
- `.git/`, `.env`: Archivos de desarrollo
- `*.csv`, `*.lock`: Archivos temporales

## Verificación

### Probar Build

```bash
docker build -t ingestor-reader:test .
```

### Probar Ejecución Local

```bash
# Con Lambda Runtime Interface Emulator (RIE)
docker run -p 9000:8080 \
  -e S3_BUCKET=test-bucket \
  -e ENV=local \
  ingestor-reader:test

# Invocar
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"dataset_id": "bcra_infomondia_series"}'
```

## Notas

- La imagen base de AWS Lambda ya incluye boto3 y el runtime
- El handler está configurado en el CMD del Dockerfile
- Todas las dependencias se instalan desde `pyproject.toml`
- Los archivos de config deben estar en el package (se copian en el Dockerfile)

