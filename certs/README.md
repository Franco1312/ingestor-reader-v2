# Certificados SSL Personalizados

Este directorio contiene certificados SSL personalizados para verificar conexiones HTTPS.

## Cómo agregar un certificado

### Opción 1: Descargar el certificado del sitio

```bash
# Descargar el certificado del sitio
openssl s_client -showcerts -connect www.bcra.gob.ar:443 </dev/null 2>/dev/null | \
  openssl x509 -outform PEM > certs/bcra.pem

# O descargar la cadena completa de certificados
openssl s_client -showcerts -connect www.bcra.gob.ar:443 </dev/null 2>/dev/null | \
  sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > certs/bcra-chain.pem
```

### Opción 2: Combinar con el bundle de certificados de Python

```bash
# Obtener el bundle de certificados de Python
python3 -c "import certifi; print(certifi.where())" > /tmp/certifi_path.txt
CERTIFI_PATH=$(cat /tmp/certifi_path.txt)

# Combinar el certificado personalizado con el bundle
cat "$CERTIFI_PATH" certs/bcra.pem > certs/cacert.pem
```

### Opción 3: Usar el bundle de certificados del sistema

Si prefieres usar el bundle del sistema, puedes copiarlo:

```bash
# En Linux
cp /etc/ssl/certs/ca-certificates.crt certs/cacert.pem

# O en macOS
cp /etc/ssl/cert.pem certs/cacert.pem
```

## Configuración

El código buscará automáticamente el certificado en:
1. `/var/task/certs/cacert.pem` (en Lambda)
2. Variable de entorno `SSL_CERT_FILE`

Si encuentra un certificado personalizado y `verify_ssl=true`, lo usará para verificar las conexiones SSL.

## Nota

Si no se encuentra un certificado personalizado, se usará el bundle de certificados del sistema (por defecto de Python/certifi).

