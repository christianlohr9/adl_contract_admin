# config.py

import os

# Absoluter Pfad zur externen Render-Datenbank
db_config = {
    "host": os.getenv("DB_HOST", "dpg-cuke2v5umphs73bcnhu0-a.a.render.com"),      # Hier wird der Wert von DB_HOST erwartet
    "port": int(os.getenv("DB_PORT", 5432)),        # Standardwert: 5432
    "dbname": os.getenv("DB_NAME", "adl_data"),     # Standardwert: "adl_data"
    "user": os.getenv("DB_USER", "adl_data_user"),           # Dein Benutzername
    "password": os.getenv("DB_PASSWORD", "6YpiBm5BJCR6ABi9IELdCN9MqXLcGvIF")  # Dein Passwort
}

# Andere Konstanten
START_YEAR = 2020
DEFAULT_SEASON = 2024
LEAGUE_ID = 60206
DEFAULT_TEAM = "New York Jets"
DEFAULT_WEEK = 0
