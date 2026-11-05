# Written by: Claude. Origin: Claude chat, project "Transcripts to HubSpot". Date written: 2026-11-05.
# Image for the audit loader job (HubSpot -> BigQuery). Separate from the pipeline's image.
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY audit_loader.py .
CMD ["python", "audit_loader.py"]
