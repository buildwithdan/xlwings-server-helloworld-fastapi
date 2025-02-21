FROM python:3.11-slim

# Ensure logs are shown immediately
ENV PYTHONUNBUFFERED=1

# Install build tools
RUN apt-get update && apt-get install -y gcc

COPY requirements.txt .
RUN pip install -r requirements.txt

# Watchfiles from uvicorn[standard] breaks reload inside Docker
RUN pip uninstall watchfiles -y

ENV XLWINGS_LICENSE_KEY="noncommercial"

COPY ./app /app

EXPOSE 8000

CMD ["gunicorn", "app.main:cors_app", \
     "--bind", "0.0.0.0:8000", \
     "--access-logfile", "-", \
     "--workers", "2", \
     "--worker-class", "uvicorn.workers.UvicornWorker"]


# building
# docker buildx build --platform linux/amd64 -t buildwithdan/xlwings-googlesheets .
