# config.py

import os

# Render-Datenbank-Konfiguration
db_config = {
    "host": os.getenv("DB_HOST", "aws-0-eu-central-1.pooler.supabase.com"),  # Externer Hostname!
    "port": int(os.getenv("DB_PORT", 6543)),  # Standardport für PostgreSQL
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres.dvgannvtzcvqnpdxrvno"),
    "password": os.getenv("DB_PASSWORD", "w5b^b4XmNfAo7v")  # ⚠️ Niemals direkt im Code speichern!
}

# Andere Konstanten
START_YEAR = 2020
DEFAULT_SEASON = 2024
LEAGUE_ID = 60206
DEFAULT_TEAM = "New York Jets"
DEFAULT_WEEK = 0
