
extension_page = """
<|navbar|>
# Gefilterte Verträge

<|button|label=Zurück|on_action=navigate_to_selection|>

**Verträge:**
<|{filtered_df}|table|filter=True|editable=false|editable[contract_years]=true|on_edit=contract_years_on_edit|height=400px|width=100%|>

<|button|label=EPVs berechnen|on_action=calculate_epvs|>
"""