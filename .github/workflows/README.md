# CI/CD Pipeline

Este pipeline despliega automáticamente la función Lambda en cada push a `main`.

## Configuración Requerida

Configura los siguientes secrets en GitHub (Settings > Secrets and variables > Actions):

### Secrets Requeridos

- `AWS_ROLE_ARN`: ARN del rol IAM para GitHub Actions (con permisos para ECR y Lambda)
  - Ejemplo: `arn:aws:iam::123456789012:role/github-actions-role`

### Secrets Opcionales (con valores por defecto)

- `AWS_REGION`: Región de AWS (por defecto: `us-east-1`)
- `ECR_REPOSITORY`: Nombre del repositorio ECR (por defecto: `ingestor-reader`)
- `LAMBDA_FUNCTION_NAME`: Nombre de la función Lambda (por defecto: `ingestor-reader`)

## Permisos IAM Requeridos

El rol IAM usado por GitHub Actions necesita:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:UpdateFunctionCode",
        "lambda:GetFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:ingestor-reader"
    }
  ]
}
```

## Flujo del Pipeline

1. **Checkout**: Obtiene el código
2. **Configure AWS**: Configura credenciales usando OIDC
3. **Login ECR**: Autentica en Amazon ECR
4. **Build**: Construye la imagen Docker
5. **Tag**: Taggea con commit SHA y `:latest`
6. **Push**: Empuja ambas tags a ECR
7. **Update Lambda**: Actualiza la función Lambda con la nueva imagen

## Ejecución Manual

El pipeline se ejecuta automáticamente en cada push a `main`. También puedes ejecutarlo manualmente desde la pestaña "Actions" en GitHub.

