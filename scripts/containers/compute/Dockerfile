FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY compute_kpis.py .

ENTRYPOINT ["python", "compute_kpis.py"]