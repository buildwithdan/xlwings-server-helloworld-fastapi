FROM python:3.11-slim

# Ensure logs are shown immediately
ENV PYTHONUNBUFFERED=1

# Install required packages and prerequisites
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    apt-transport-https \
    gnupg2

# Install unixODBC packages
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 17 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

COPY requirements.txt .
RUN pip install -r requirements.txt

# Watchfiles from uvicorn[standard] breaks reload inside Docker
RUN pip uninstall watchfiles -y

ENV XLWINGS_LICENSE_KEY="noncommercial"

COPY ./app /app

EXPOSE 7999

CMD ["gunicorn", "app.main:cors_app", \
     "--bind", "0.0.0.0:7999", \
     "--access-logfile", "-", \
     "--workers", "4", \
     "--worker-class", "uvicorn.workers.UvicornWorker"]