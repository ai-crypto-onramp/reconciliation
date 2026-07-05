FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN pip install --no-cache-dir -e .
EXPOSE 8080
CMD ["uvicorn", "reconciliation.app:app", "--host", "0.0.0.0", "--port", "8080"]
