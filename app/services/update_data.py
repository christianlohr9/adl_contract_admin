import logging
from datetime import datetime
from services.database_service import create_connection, load_franchises, load_rosters, calculate_and_save_contracts, load_playerscores

# Importiere Konfigurationsvariablen
from config.config import START_YEAR, DEFAULT_SEASON, LEAGUE_ID

def update_database(start_year: int, end_year: int, league_id: int):
    """
    Zentrale Funktion, um alle Datenbanktabellen zu aktualisieren (Franchises, Roster, Verträge, etc.).
    """
    logging.info(f"Starting database update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Erstelle die Datenbankverbindung
        conn = create_connection()

        # Schritt 1: Aktualisiere die Tabelle 'franchises'
        logging.info("Updating franchises table...")
        load_franchises(start_year, end_year, league_id, conn)

        # Schritt 2: Aktualisiere die Tabelle 'rosters'
        logging.info("Updating rosters table...")
        load_rosters(start_year, end_year, league_id, conn)

        # Schritt 3: Aktualisiere die Tabelle 'contracts'
        logging.info("Updating contracts table...")
        calculate_and_save_contracts(start_year, end_year, conn)

        # Schritt 4: Aktualisiere die Tabelle 'playerscores' (falls notwendig)
        logging.info("Updating playerscores table...")
        load_playerscores(conn)

        # Änderungen speichern
        conn.commit()

        logging.info(f"Database update completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        logging.error(f"Error during database update: {e}")
    finally:
        if conn:
            conn.close()  # Schließe die Verbindung

if __name__ == "__main__":
    start_year = START_YEAR
    end_year = DEFAULT_SEASON
    league_id = LEAGUE_ID

    # Logging konfigurieren
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("update.log"), logging.StreamHandler()],
    )

    # Datenbank aktualisieren
    update_database(start_year, end_year, league_id)