
home_page = """
<|toggle|theme|>
<|navbar|>
# Team und Saison Auswahl

<|layout|columns=1fr auto 1fr|
<|layout|direction=column|
**Wähle ein Team:**
<|{selected_team}|selector|lov={teams}|height=500px||>
<|button|label=Filtern|on_action=filter_and_navigate|>
|>

<|layout|columns=1fr auto
**Wähle eine Saison:**
<|{selected_season}|selector|lov={seasons}|height=250px|>

**Wähle eine Woche:**
<|{selected_weeks}|selector|lov={weeks}|height=250px|>
|>
|>

"""