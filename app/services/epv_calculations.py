# epv_calculations.py

import polars as pl
from taipy.gui import navigate, notify
from services.data_processing import load_contracts, load_salaries

def calculate_new_salary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Berechnet das geglättete Gehalt für einen DataFrame.
    """
    def calculate_smoothed_salary(row):
        total_first = sum(row["salary"] * (row["growth_rate"] ** i) for i in range(int(row["prev_yrs"])))
        total_second = sum(row["eys"] * (row["growth_rate"] ** i) for i in range(int(row["prev_yrs"]), int(row["prev_yrs"] + row["ext_yrs"])))
        combined_total = total_first + total_second
        total_years = row["prev_yrs"] + row["ext_yrs"]
        denominator = sum(row["growth_rate"] ** i for i in range(int(total_years)))
        return combined_total / denominator

    df = (
        df
        .with_columns(growth_rate=pl.lit(1.1))
        .with_columns(
            new_sal=pl.struct(["salary", "growth_rate", "prev_yrs", "ext_yrs", "eys"])
            .apply(calculate_smoothed_salary)
        )
        .with_columns(new_sal=pl.col("new_sal").round(2))
    )
    return df

def calculate_epvs(state):
    """
    Aktualisiert den DataFrame basierend auf den angegebenen Filterkriterien und speichert ihn in `state.filtered_df`.

    :param state: Der aktuelle State der Taipy-Anwendung.
    """

    # Filtere Spieler und Teams basierend auf den Vertragsjahren
    player_filter = (
        pl.from_pandas(state.filtered_df)
        .filter(pl.col("contract_years") > 1)
        .select("player_id")
        .to_series()
        .to_list()
    )
    team_filter = (
        pl.from_pandas(state.filtered_df)
        .filter(pl.col("contract_years") > 1)
        .select("conference")
        .to_series()
        .to_list()
    )

    # Erster Transformationsschritt
    filtered_contracts_df = (
        load_contracts()
        .with_columns(YO5 = pl.when(pl.col("contractInfo").str.contains("5YO")).then(pl.lit(1)).otherwise(pl.lit(0)))
        .filter(
            (pl.col("player_id").is_in(player_filter))
            & ((pl.col("conference").is_in(team_filter)) | pl.col("conference").is_null())
            & (pl.col("season")<=state.selected_season)
        )
        .sort("player_id", "season", descending=[False, True])
        .with_columns(
            min_rank=pl.when(
                (pl.col("tot_pts_rank") <= pl.col("avg_pts_rank"))
                & (pl.col("tot_pts_rank") <= pl.col("floor_pts_rank"))
            )
            .then(pl.col("tot_pts_rank"))
            .when(
                (pl.col("avg_pts_rank") <= pl.col("tot_pts_rank"))
                & (pl.col("avg_pts_rank") <= pl.col("floor_pts_rank"))
            )
            .then(pl.col("avg_pts_rank"))
            .otherwise(pl.col("floor_pts_rank"))
        )
        .filter(pl.col("is_robust") == 1)
    )

    # Zweiter Transformationsschritt
    filtered_contracts_df = (
        filtered_contracts_df
        .sort(by=["player_id", "season"], descending=[False, True])
        .with_columns(
            pr1=pl.col("min_rank").shift(0).over("player_id"),
            pr2=pl.col("min_rank").shift(-1).over("player_id"),
            pr3=pl.col("min_rank").shift(-2).over("player_id")
            )
        .filter(pl.col("season") == pl.col("season").max().over("player_id"))
    )
    
    # Load Salaries
    salaries = load_salaries().filter(pl.col("season")==state.selected_season).with_columns(rank = pl.col("salary").rank(method="ordinal",descending=True).over("pos")).sort("pos","rank", descending=[False,False]).select(["pos","rank","salary"])
    salaries = salaries.with_columns(pl.col("rank").cast(pl.Int32))

    # Liste für neue Einträge
    special_rows = []

    # Spalten der Tabelle merken (Schema sicherstellen)
    table_columns = salaries.schema

    # Für jede Position (pos) die spezielle Berechnung durchführen
    for pos, group in salaries.group_by("pos", maintain_order=True):
        # Sortiere die Gehälter absteigend
        group_sorted = group.sort("salary", descending=True)
        
        # Sicherstellen, dass genügend Werte vorhanden sind
        if len(group_sorted) < 4:
            continue

        # Wichtige Werte extrahieren
        max_salary = group_sorted[0, "salary"]
        second_highest_salary = group_sorted[1, "salary"]
        third_highest_salary = group_sorted[2, "salary"]
        fourth_highest_salary = group_sorted[3, "salary"]

        # Formel 1: max(salary) + (2 * zweithöchste - dritte - vierte) / 3
        special_salary_1 = max_salary + (2 * second_highest_salary - third_highest_salary - fourth_highest_salary) / 3

        # Formel 2: 2 * Berechneter Wert aus Formel 1 - max(salary) ursprünglich
        special_salary_2 = 2 * special_salary_1 - max_salary

        # Zwei neue Einträge mit Standardwerten erstellen
        special_row_1 = {
            "pos": pos,
            "rank": 0,  # Beispielwert für den Rank
            "salary": special_salary_1,
        }

        special_row_2 = {
            "pos": pos,
            "rank": -1,  # Rank -1 für die zweite spezielle Zeile
            "salary": special_salary_2,
        }

        # Neue Zeilen der Liste hinzufügen
        special_rows.append(special_row_1)
        special_rows.append(special_row_2)

    # Neue Einträge in die Tabelle einfügen
    new_rows_df = pl.DataFrame(special_rows, schema=table_columns, infer_schema_length=1)
    salaries = salaries.vstack(new_rows_df, in_place=False)

    # Unterfunktion zur Berechnung einzelner Ränge
    def calculate_single_salary(pos, rank, table, multiplier=1.0):
        salary_values = table.filter(
            (table["pos"] == pos) & (table["rank"].is_in([rank * 2 - 3, rank * 2 - 2]))
        )["salary"]
        return multiplier * salary_values.mean() if len(salary_values) > 0 else None

    # Hauptfunktion zur Berechnung aller Ränge
    def calculate_salaries(row, end23_sal, jul1_sal):
        pos = row["pos"]
        ranks = [row["pr1"], row["pr2"], row["pr3"]]
        table = end23_sal if row.get("week", 0) == 0 else jul1_sal
        multiplier = 1.1 if row.get("week", 0) == 0 else 1.0
        # Für jeden Rank die Berechnung durchführen
        salaries = [
            calculate_single_salary(pos, rank, table, multiplier) for rank in ranks
        ]
        return salaries
    
    # Berechnung der Salaries und Hinzufügen zum DataFrame
    salary_columns = ["salary1", "salary2", "salary3"]
    salaries_list = [calculate_salaries(row, salaries, salaries) for row in filtered_contracts_df.to_dicts()]
    # Umwandlung der salaries_list in ein Dictionary
    salaries_dict = {
        salary_columns[i]: [row[i] for row in salaries_list]  # Extrahiere die i-te Spalte aus jeder Zeile
        for i in range(len(salary_columns))
    }
    salaries_df = pl.DataFrame(salaries_dict)
    main_df = (
        filtered_contracts_df
        .with_columns(salaries_df)
        .rename({"contract_years": "prev_yrs"})
        .drop("salary")
        .join(pl.from_pandas(state.filtered_df).select(["player_id","salary","contract_years"]).rename({"contract_years": "ext_yrs"}), on="player_id")
        .select(["player_name", "pos", "salary", "prev_yrs", "ext_yrs", "YO5", "salary1", "salary2", "salary3"])
        .with_columns(
            eys=(pl.max_horizontal(["salary", "salary1", "salary2", "salary3"]) * (1.15 - 0.05 * (pl.col("ext_yrs") - pl.col("YO5")))) # hier die 5th yr option rein
            )
    )
    main_df = calculate_new_salary(main_df).select(["player_name", "pos", "salary", "prev_yrs", "ext_yrs", "YO5", "new_sal"])

    # Speichere das Ergebnis in den State
    state.filtered_df = main_df.to_pandas()  # Konvertiere zurück in Pandas-DataFrame, falls Taipy Pandas erwartet
    navigate(state, "epv")
    notify(state, "success", f'Fuck this.')
    pass