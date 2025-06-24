import datetime
import os

from zipfile import ZipFile

import httpx
from pytz import country_names

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'YOUR API KEY'
}

server = 'https://app.welldatabase.com'
extractTo = "./phdwin_exports"

timeout = httpx.Timeout(5, read=None)

def run_export(county_id:int, export_format:str):
    with httpx.Client(timeout=timeout) as client:
        export_payload =    data = {
            'Filters': {
                'CountyIds': {'Included': [county_id]},
            },
            'ExportFormat': export_format,
        }
        export_response = client.post(f"{server}/api/v2/export", headers=headers,
                               json=export_payload, timeout=timeout)
        if export_response.status_code != httpx.codes.OK:
            export_response.raise_for_status()

        return export_response

def run_search():

    print("")
    print("")

    state_name = input('Enter State Name: ').strip() or "New Mexico"

    state_response = httpx.get(f"{server}/api/v2/apistate?name={state_name}",
                         headers=headers, timeout=timeout)
    state_matches = state_response.json()

    state_match_count = state_matches['total']

    print("")
    print("")
    print(f"Found {state_match_count} States(s) matching {state_name}:")

    for state in state_matches['data']:
        print(f"{state['id']} - {state['name']}")
    print(f"exit - Start Over")

    print("")
    state_id_str = input("Enter State Id").strip() or state_matches['data'][0]['id']

    if state_id_str == 'exit':
        run_search()

    state_id = int(state_id_str)
    selected_state = next(filter(lambda x: x['id'] == state_id, state_matches['data']))
    selected_state_name = selected_state['name']

    county_response = httpx.get(f"{server}/api/v2/apiState/{state_id}/counties",
                         headers=headers, timeout=timeout)
    county_matches = county_response.json()

    county_match_count = county_matches['total']
    print("")
    print("")
    print(f"Found {county_match_count} Counties(s) For State {selected_state_name}")

    for county in county_matches['data']:
        print(f"{county['id']} - {county['name']}")
    print(f"all - Export All")
    print(f"exit - Start Over")

    print("")
    county_id_str = input("Enter County Id: ").strip() or county_matches['data'][0]['id']

    if county_id_str == 'exit':
        run_search()

    selected_counties = []
    if county_id_str == 'all':
        print(f"Exporting All Counties in {selected_state_name}")
        for county in county_matches['data']:
            selected_counties.append(county)
    else:
        user_selection = next(filter(lambda x: x['id'] == int(county_id_str), county_matches['data']))
        selected_counties = [user_selection]

    for selected_county in selected_counties:

        print(f"Exporting {selected_county['name']}")

        export_response = run_export(selected_county['id'], "wdb")
        target_base_file_name = selected_county['name'].replace(',','_').replace(' ', '')
        file_name = os.path.join(extractTo, target_base_file_name + ".zip")
        file = open(file_name, "wb")
        file.write(export_response.content)
        file.close()

        # extract zip file
        with ZipFile(file_name, 'r') as zObject:
            items = zObject.namelist()
            if items.__len__() == 0:
                print(f"{file_name} is empty")

            #rename file to county name for convenience
            first_item = items[0]
            base_zipped_file_name, ext = os.path.splitext(first_item)
            zObject.extract(first_item, extractTo)
            os.rename(os.path.join(extractTo, first_item), os.path.join(extractTo, target_base_file_name + ext))


        # optionally, delete zip
        os.remove(file_name)

    #start over
    run_search()

#start app
run_search()
