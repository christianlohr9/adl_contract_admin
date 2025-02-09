# config.py

import os

# Absoluter Pfad zur Datenbank
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),      # Standardwert: "localhost"
    "port": int(os.getenv("DB_PORT", 5432)),        # Standardwert: 5432
    "dbname": os.getenv("DB_NAME", "adl_data"),     # Standardwert: "taipy_db"
    "user": os.getenv("DB_USER", "postgres"),           # Standardwert: "postgres"
    "password": os.getenv("DB_PASSWORD", "jD5rb#qojM4Ki%") # Standardwert: "password"
}

# Andere Konstanten
START_YEAR = 2020
DEFAULT_SEASON = 2024
LEAGUE_ID = 60206
DEFAULT_TEAM = "New York Jets"
DEFAULT_WEEK = 0