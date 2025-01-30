import os
import logging
import pandas as pd
import polars as pl
import psycopg2
from config.config import db_config
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
import rpy2.rinterface_lib as rinterface_lib
from datetime import datetime
import numpy as np
from services.ffscrapr import *

def create_connection():
    """Verbindet sich mit der PostgreSQL-Datenbank anhand der Konfiguration in db_config."""
    return psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"]
    )

def load_table_from_db(table_name: str):
    # Verbindung zur DB herstellen
    conn = create_connection()
    
    # SQL-Abfrage erstellen
    query = f"SELECT * FROM {table_name}"
    
    # Cursor erstellen und SQL-Abfrage ausführen
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Alle Zeilen abholen
    rows = cursor.fetchall()
    
    # Spaltennamen extrahieren
    columns = [desc[0] for desc in cursor.description]
    
    # Einen polars DataFrame aus den Ergebnissen erstellen
    contracts_df = pl.DataFrame(rows, schema=columns)
    
    # Verbindung und Cursor schließen
    cursor.close()
    conn.close()
    
    return contracts_df

print(load_table_from_db("contracts"))