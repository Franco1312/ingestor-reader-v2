FROM public.ecr.aws/lambda/python:3.12

# Copy project files
COPY ingestor_reader/ ${LAMBDA_TASK_ROOT}/ingestor_reader/
COPY config/ ${LAMBDA_TASK_ROOT}/config/
COPY pyproject.toml ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Set handler (Lambda runtime will call this function)
CMD ["ingestor_reader.app.lambda_handler.handler"]

