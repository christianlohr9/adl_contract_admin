# config.py

import os

# Render-Datenbank-Konfiguration
db_config = {
    "host": os.getenv("DB_HOST", "dpg-cuke2v5umphs73bcnhu0-a.frankfurt-postgres.render.com"),  # Externer Hostname!
    "port": int(os.getenv("DB_PORT", 5432)),  # Standardport für PostgreSQL
    "dbname": os.getenv("DB_NAME", "adl_data"),
    "user": os.getenv("DB_USER", "adl_data_user"),
    "password": os.getenv("DB_PASSWORD", "6YpiBm5BJCR6ABi9IELdCN9MqXLcGvIF")  # ⚠️ Niemals direkt im Code speichern!
}

# Andere Konstanten
START_YEAR = 2020
DEFAULT_SEASON = 2024
LEAGUE_ID = 60206
DEFAULT_TEAM = "New York Jets"
DEFAULT_WEEK = 0
