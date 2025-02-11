# data_processing.py

import polars as pl
from taipy.gui import Icon
from services.database_service import create_connection, load_table_from_db

def load_contracts() -> pl.DataFrame:
    db_config = create_connection()
    contracts_df = load_table_from_db("contracts",db_config)
    return contracts_df

def load_salaries() -> pl.DataFrame:
    db_config = create_connection()
    contracts_df = load_table_from_db("roster",db_config).select(["season", "salary", "pos"])
    return contracts_df

def filter_table(team: str, season: int) -> pl.DataFrame:
    """
    Filtert die Vertragsdaten basierend auf Team und Saison.
    """
    contracts_df = load_contracts() 
    contracts_df = (
        contracts_df
        .filter(
            (pl.col("franchise_name") == team) &
            (pl.col("season") == season) &
            (pl.col("contract_years") <= 1)
        )
        .select(["conference", "franchise_name", "player_id", "player_name", "pos", "salary", "contract_years"])
        .sort(by=pl.col("pos"))
        .to_pandas()
    )

    return contracts_df

def get_unique_teams() -> list:
    """
    Gibt eine Liste der einzigartigen Teams zurück.
    """
    contracts_df = load_contracts() 
    contracts_df = contracts_df.with_columns(pl.col("franchise_name").fill_null("Free Agent"))
    # Duplikate basierend auf 'franchise_name' entfernen und nach 'division' sortieren
    unique_contracts_df = contracts_df.unique(subset=["franchise_name"]).sort("division")

    # Teams-Liste erstellen
    # teams = [
    #     (row["franchise_name"], Icon(row["logo"], row["franchise_name"]))
    #     for row in unique_contracts_df.iter_rows(named=True)
    # ]
    teams = unique_contracts_df.select("franchise_name").to_series().to_list()

    return teams

def get_seasons() -> list:
    """
    Gibt eine Liste der verfügbaren Saisons zurück.
    """
    contracts_df = load_contracts() 
    seasons = contracts_df.select("season").unique().to_series().to_list()
    seasons = [int(season) for season in seasons]
    return sorted(seasons, reverse=True)

def get_weeks() -> list:
    """
    Gibt eine Liste der Wochen zurück.
    """
    weeks = [int(week) for week in range(0, 18)]
    return sorted(weeks)