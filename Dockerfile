FROM python:3.9-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    curl \
    gzip \
    lsb-release \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Instalar cliente PostgreSQL 16
RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    curl -sL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && apt-get install -y postgresql-client-16 && \
    rm -rf /var/lib/apt/lists/*

# Instalar rclone
RUN curl -O https://downloads.rclone.org/rclone-current-linux-amd64.deb && \
    dpkg -i rclone-current-linux-amd64.deb && \
    rm rclone-current-linux-amd64.deb

# Copiar todo o projeto
WORKDIR /app
COPY . .

# Instalar dependências Python
RUN pip install psycopg2-binary==2.9.9

# Comando de execução
CMD ["python", "main.py"]