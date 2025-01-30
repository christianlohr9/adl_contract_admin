import polars as pl
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri

def init_ffscrapr():
    # Can uncomment to install R packages as needed
    print('Installing R Packages for scraping MFL Data')
    utils = importr('utils')
    utils.install_packages('nflreadr')
    utils.install_packages('ffscrapr')

def ff_connect(season, league_id):
    ffscrapr = importr('ffscrapr')
    conn = ffscrapr.mfl_connect(season=season, league_id=league_id, rate_limit_number=1, rate_limit_seconds=6)
    return conn

def get_positions(conn):
    ffscrapr = importr('ffscrapr')
    positions = ffscrapr.ff_starter_positions(conn)
    positions = pandas2ri.rpy2py(positions)
    positions = pl.from_pandas(positions)
    positions = positions.select("pos").to_series()

    return positions

def get_starter(conn, position):
    ffscrapr = importr('ffscrapr')
    starter = ffscrapr.ff_starter_positions(conn)
    starter = pandas2ri.rpy2py(starter)
    starter = pl.from_pandas(starter)
    starter = starter.filter(pl.col("pos") == position).select("min").to_numpy()[0][0]

    return starter

