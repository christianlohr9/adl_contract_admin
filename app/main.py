# main.py

from taipy.gui import Gui, Icon, navigate, notify
from pages.home import home_page
from pages.extension import extension_page
from pages.evp import ext_page
from services.data_processing import filter_table, get_unique_teams, get_seasons, get_weeks
from services.epv_calculations import calculate_epvs

# Initialisiere gefilterte DataFrame-Variable
filtered_df = None

# Abrufen der einzigartigen Teamnamen und Saisons
teams = get_unique_teams()
seasons = get_seasons()
weeks = get_weeks()

# Initiale Auswahl
selected_team = teams[0]
selected_season = seasons[0]
selected_weeks = weeks[0]

# Seiten- und Filterlogik
def filter_and_navigate(state):
    state.filtered_df = filter_table(state.selected_team[0], state.selected_season)
    state.filtered_df["week"] = state.selected_weeks
    navigate(state, "extension")
    notify(state, "success", f'Clicked on team: {state.selected_team[0]}')

def navigate_to_selection(state):
    navigate(state, "home")

# Tabelle editieren
def contract_years_on_edit(state, var_name, payload):
    index = payload["index"]
    col = payload["col"]
    value = payload["value"]
    old_value = state.filtered_df.loc[index, col]
    new_filtered_df = state.filtered_df.copy()
    new_filtered_df.loc[index, col] = value
    state.filtered_df = new_filtered_df
    notify(state, "I", f"Edited value from '{old_value}' to '{value}'. (index '{index}', column '{col}')")

# Seitenkonfiguration
pages = {
    "/": "<|menu|lov={page_names}|on_action=menu_action|>",
    "home": home_page,
    "extension": extension_page,
    "epv": ext_page,
}
page_names = [page for page in pages.keys() if page != "/"]

def menu_action(state, action, payload):
    page = payload["args"][0]
    navigate(state, page)

# Taipy GUI starten
gui = Gui(pages=pages)
gui.run(run_browser=True, use_reloader=True)