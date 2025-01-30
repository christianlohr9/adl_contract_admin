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
import sys
sys.path.append('./adl_contract_admin')


def create_connection():
    """Verbindet sich mit der PostgreSQL-Datenbank anhand der Konfiguration in db_config."""
    return psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"]
    )

def load_table_from_db(table: str, db_config: dict) -> pl.DataFrame:
    """
    Lädt die angegebene Tabelle aus einer PostgreSQL-Datenbank und konvertiert sie in ein Polars DataFrame.
    
    Parameters:
    table (str): Der Name der Tabelle in der PostgreSQL-Datenbank.
    db_config (dict): Konfigurationsdaten für die PostgreSQL-Datenbank.
                      Beispiel: {"host": "localhost", "port": 5432, "dbname": "taipy_db", "user": "user", "password": "password"}
    
    Returns:
    pl.DataFrame: Polars DataFrame mit den Daten aus der angegebenen Tabelle.
    
    Raises:
    ValueError: Wenn der Tabellenname nicht angegeben ist.
    psycopg2.Error: Wenn es ein Problem beim Verbinden mit der Datenbank oder beim Ausführen der Abfrage gibt.
    """
    if not table:
        raise ValueError("Tabellenname muss angegeben werden.")
    
    # Verbindung zur DB herstellen
    conn = db_config
    
    try:
        # SQL-Abfrage erstellen
        query = f"SELECT * FROM {table}"
        
        # Cursor erstellen und SQL-Abfrage ausführen
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Alle Zeilen abholen
        rows = cursor.fetchall()
        
        # Spaltennamen extrahieren
        columns = [desc[0] for desc in cursor.description]
        
        # Einen Polars DataFrame aus den Ergebnissen erstellen
        contracts_df = pl.DataFrame(rows, schema=columns)
        
        if contracts_df.height == 0:
            logging.warning(f"Die Tabelle '{table}' ist leer.")
        else:
            logging.info(f"Erfolgreich Tabelle '{table}' mit {contracts_df.height} Zeilen geladen.")
        
        return contracts_df
    
    except psycopg2.Error as e:
        logging.error(f"Fehler bei der Datenbankabfrage für Tabelle '{table}': {e}")
        raise psycopg2.Error(f"Fehler bei der Datenbankabfrage: {e}")
    
    finally:
        # Verbindung und Cursor schließen
        if conn:
            conn.close()
            logging.info("Datenbankverbindung geschlossen.")

def delete_table_from_db(table_name: str, db_config: dict) -> None:
    """
    Deletes the specified table from the PostgreSQL database.
    
    Parameters:
    table_name (str): The name of the table to be deleted.
    db_config (dict): Configuration for the PostgreSQL database.
                      Example: {"host": "localhost", "port": 5432, "dbname": "taipy_db", "user": "user", "password": "password"}
    
    Returns:
    None
    
    Raises:
    psycopg2.Error: If there is an issue connecting to the database or executing the query.
    """
    conn_db = None
    try:
        # Verbindung zur PostgreSQL-Datenbank herstellen
        conn_db = create_connection()
        cursor = conn_db.cursor()
        logging.info(f"Connected to PostgreSQL database at {db_config['host']}")

        # Überprüfen, ob die Tabelle existiert
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'public' AND tablename = %s
            );
        """, (table_name,))
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logging.warning(f"Table '{table_name}' does not exist in the database.")
            return

        # SQL-Befehl zum Löschen der Tabelle
        query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(query)
        conn_db.commit()
        logging.info(f"Table '{table_name}' has been deleted from the database.")

    except psycopg2.Error as e:
        logging.error(f"Database error while deleting table '{table_name}': {e}")
        raise psycopg2.Error(f"Database error: {e}")
    
    finally:
        # Verbindung zur PostgreSQL-Datenbank schließen
        if conn_db:
            cursor.close()
            conn_db.close()
            logging.info("Database connection closed.")

def calculate_and_save_contracts(start_year: int, end_year: int, db_config: dict):
    """
    Save contracts data for multiple years to a PostgreSQL database.
    
    Parameters:
    start_year (int): The starting year for processing contracts data.
    end_year (int): The ending year for processing contracts data.
    db_config (dict): Configuration for the PostgreSQL database.
                      Example: {"host": "localhost", "port": 5432, "dbname": "taipy_db", "user": "user", "password": "password"}
    
    Returns:
    None
    """
    try:
        conn_db = create_connection()
        cursor = conn_db.cursor()

        # Überprüfen, ob die Tabelle existiert
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'public' AND tablename = 'contracts'
            );
        """)
        table_exists = cursor.fetchone()[0]

        for year in range(start_year, end_year + 1):
            if table_exists:
                cursor.execute("SELECT COUNT(*) FROM contracts WHERE season = %s", (year,))
                result = cursor.fetchone()
                if result[0] > 0:
                    logging.info(f"Contracts for year {year} already present in database. Skipping.")
                    continue

            # Process contracts data for the year
            contracts = (
                load_table_from_db("playerscores", db_config)
                .group_by(["player_id", "season"])
                .agg(
                    player_name=pl.col("player_name").first(),
                    pos=pl.col("pos").first(),
                    team=pl.col("team").first(),
                    num_games=pl.col("points").count(),
                    tot_pts=pl.col("points").sum(),
                    avg_pts=pl.col("points").mean()
                )
                .with_columns(is_robust=pl.col("num_games") >= 5)
            )

            contracts = (
                contracts
                .with_columns(
                    tot_pts_rank=pl.struct("tot_pts").rank("max", descending=True).over(["pos", "season"]),
                    avg_pts_rank=pl.struct("avg_pts").rank("max", descending=True).over(["pos", "season"])
                )
                .join(calculate_floor_pts_rank(), on="pos")
            )

            contracts = (
                contracts
                .join(load_table_from_db("roster", db_config), on=["player_id", "season"], how="left")
                .drop([col for col in contracts.columns if col.endswith("_right")])
                .join(
                    load_table_from_db("franchises", db_config).select(
                        ["franchise_id", "season", "salaryCapAmount", "conference", "division", "logo"]
                    ),
                    on=["franchise_id", "season"],
                    how="left"
                )
                .drop([col for col in contracts.columns if col.endswith("_right")])
                .with_columns(
                    salary_rank=pl.struct("salary").rank("ordinal", descending=True).over(["pos", "season", "conference"])
                )
            )

            # Save the contracts data to the database
            contracts_pandas = contracts.to_pandas()
            if not table_exists:
                contracts_pandas.to_sql("contracts", conn_db, if_exists="replace", index=False, method='multi')
                table_exists = True
            else:
                contracts_pandas.to_sql("contracts", conn_db, if_exists="append", index=False, method='multi')
            logging.info(f"Contracts data for year {year} written to PostgreSQL database.")

        conn_db.commit()
        cursor.close()
        conn_db.close()
    except Exception as e:
        logging.error(f"Error in calculate_and_save_contracts: {e}")
        raise

def load_franchises(start_year: int, end_year: int, league_id: int, db_config: dict):
    """
    Save franchise data for multiple years to a PostgreSQL database.
    
    Parameters:
    start_year (int): The starting year for processing data.
    end_year (int): The ending year for processing data.
    league_id (int): The league ID for connecting to the data source.
    db_config (dict): Configuration for the PostgreSQL database.
                      Example: {"host": "localhost", "port": 5432, "dbname": "taipy_db", "user": "user", "password": "password"}
    
    Returns:
    None
    """
    try:
        conn_db = create_connection()
        cursor = conn_db.cursor()

        # Überprüfen, ob die Tabelle existiert
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'public' AND tablename = 'franchises'
            );
        """)
        table_exists = cursor.fetchone()[0]

        for year in range(start_year, end_year + 1):
            if table_exists:
                cursor.execute("SELECT COUNT(*) FROM franchises WHERE season = %s", (year,))
                result = cursor.fetchone()
                if result[0] > 0:
                    logging.info(f"Franchises for year {year} already present in database. Skipping.")
                    continue

            # Process and save franchise data
            conn = ff_connect(year, league_id)
            franchise_df = ffscrapr.ff_franchises(conn)
            franchise_df = pandas2ri.rpy2py(franchise_df)
            franchise_df = franchise_df.map(lambda x: np.nan if isinstance(x, rinterface_lib.sexp.NACharacterType) else x)
            franchise_df = pl.from_pandas(franchise_df)
            franchise_df = franchise_df.with_columns(
                season=pl.lit(year),
                timestamp=pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

            franchise_pandas = franchise_df.to_pandas()
            if not table_exists:
                franchise_pandas.to_sql("franchises", conn_db, if_exists="replace", index=False, method='multi')
                table_exists = True
            else:
                franchise_pandas.to_sql("franchises", conn_db, if_exists="append", index=False, method='multi')
            logging.info(f"Franchise data for year {year} written to PostgreSQL database.")

        conn_db.commit()
        cursor.close()
        conn_db.close()
    except Exception as e:
        logging.error(f"Error in load_franchises: {e}")
        raise

def load_rosters(start_year: int, end_year: int, league_id: int, db_config: dict):
    """
    Save roster data for multiple years to a PostgreSQL database.
    
    Parameters:
    start_year (int): The starting year for processing roster data.
    end_year (int): The ending year for processing roster data.
    league_id (int): The league ID for connecting to the data source.
    db_config (dict): Configuration for the PostgreSQL database.
                      Example: {"host": "localhost", "port": 5432, "dbname": "taipy_db", "user": "user", "password": "password"}
    
    Returns:
    None
    """
    try:
        # Verbindung zur PostgreSQL-Datenbank herstellen
        conn_db = create_connection()
        cursor = conn_db.cursor()

        # Überprüfen, ob die Tabelle existiert
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE schemaname = 'public' AND tablename = 'roster'
            );
        """)
        table_exists = cursor.fetchone()[0]

        for year in range(start_year, end_year + 1):
            if table_exists:
                cursor.execute(sql.SQL("SELECT COUNT(*) FROM roster WHERE season = %s"), (year,))
                result = cursor.fetchone()
                if result[0] > 0:
                    logging.info(f"Roster for year {year} already present in database. Skipping.")
                    continue

            # Process roster data for the year
            conn = ff_connect(year, league_id)
            roster_df = ffscrapr.ff_rosters(conn)
            roster_df = pandas2ri.rpy2py(roster_df)
            roster_df = roster_df.map(lambda x: np.nan if isinstance(x, rinterface_lib.sexp.NACharacterType) else x)
            roster_df = pl.from_pandas(roster_df)
            roster_df = (
                roster_df
                .with_columns(
                    player_id=pl.col("player_id").cast(pl.Int32),
                    season=pl.lit(year),
                    timestamp=pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )
            )

            # Save the roster data to the database
            roster_df_pandas = roster_df.to_pandas()
            if not table_exists:
                roster_df_pandas.to_sql("roster", conn_db, if_exists="replace", index=False, method='multi')
                table_exists = True
            else:
                roster_df_pandas.to_sql("roster", conn_db, if_exists="append", index=False, method='multi')
            logging.info(f"Roster data for year {year} written to PostgreSQL database.")

        conn_db.commit()
        cursor.close()
        conn_db.close()
    except Exception as e:
        logging.error(f"Error in load_rosters: {e}")
        raise

def load_playerscores(mfl_id: int = 60206, past_seasons: list = [2024, 2023, 2022, 2021, 2020], max_week: int = 17, save_label: str = 'MFL'):
    """
    Load or scrape player scores data.
    
    Parameters:
    mfl_id (int): The MFL league ID.
    past_seasons (list): List of seasons to process.
    max_week (int): Maximum week number to consider.
    save_label (str): Label for saving the CSV file.
    
    Returns:
    pd.DataFrame: DataFrame containing player scores data.
    """
    try:
        load_data = True
        if load_data and os.path.isfile(f'{save_label}_PlayerScores.csv'):
            logging.info('Loading playerscore data from disk')
            playerscores_df = pd.read_csv(f'{save_label}_PlayerScores.csv')
            return playerscores_df

        logging.info('Scraping roster and playerscore data from MFL')
        playerscores_df = {}

        for season in past_seasons:
            mfl = ffscrapr.mfl_connect(season=season, league_id=mfl_id, rate_limit_number=1, rate_limit_seconds=6)
            playerscores_df_r = ffscrapr.ff_playerscores(mfl, season=season, week=[i + 1 for i in range(max_week)])

            with (ro.default_converter + pandas2ri.converter).context():
                playerscores_df[season] = ro.conversion.get_conversion().rpy2py(playerscores_df_r)

            # Cleanup data
            playerscores_df[season]['season'] = playerscores_df[season]['season'].astype(int)
            playerscores_df[season]['week'] = playerscores_df[season]['week'].astype(int)
            playerscores_df[season]['player_id'] = playerscores_df[season]['player_id'].astype(int)
            playerscores_df[season]['points'] = playerscores_df[season]['points'].astype(float)
            playerscores_df[season] = playerscores_df[season].drop('is_available', axis=1)

        # Merge seasons into a single DataFrame
        playerscores_df = pd.concat(playerscores_df, ignore_index=True)
        playerscores_df = playerscores_df.set_index(['player_id', 'season', 'week']).sort_index(level=[0, 1, 2], ascending=[True, True, True]).reset_index()

        # Save to CSV
        playerscores_df.to_csv(f'{save_label}_PlayerScores.csv', index=False)
        logging.info(f"Player scores data saved to {save_label}_PlayerScores.csv")

        return playerscores_df
    except Exception as e:
        logging.error(f"Error in load_playerscores: {e}")
        raise
