FROM python:3.12-slim

# Force logs to appear immediately in GCP console
ENV PYTHONUNBUFFERED True

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run the pipeline
ENTRYPOINT ["python", "main.py"]