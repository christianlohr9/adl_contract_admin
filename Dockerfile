# Verwende ein offizielles Python-Image als Basis
FROM python:3.12-slim

# Setze das Arbeitsverzeichnis im Container
WORKDIR /adl_contract_admin

# Systemabhängigkeiten installieren (R + benötigte Pakete für rpy2)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    r-base \
    r-base-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setze die Umgebungsvariablen für R und Pythonpath
ENV R_HOME=/usr/lib/R
ENV PATH="${R_HOME}/bin:${PATH}"
ENV PYTHONPATH="/app"

# Kopiere die requirements.txt Datei in das Arbeitsverzeichnis
COPY requirements.txt .

# Installiere die Python-Abhängigkeiten
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den gesamten Anwendungscode in das Arbeitsverzeichnis
COPY . .

# Setze die Umgebungsvariable für den Port, auf dem die App laufen soll
ENV PORT=5000

# Exponiere den Port, auf dem die App laufen soll
EXPOSE $PORT

# Starte die Anwendung
CMD ["python", "app/main.py"]
