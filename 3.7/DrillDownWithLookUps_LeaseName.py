import datetime
import re

import httpx

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your API Key'
}

modifiedSince = datetime.datetime.now() - datetime.timedelta(days=10)

def run_search():
    # default to texas
    stateId = 42 #int(input('Enter State Id: ').strip() or "42")
    district =  input('Enter District: ').strip() or '08'
    lease_number =  input('Enter Lease Number: ').strip() or '01770'

    lease_name = f"{district}-{lease_number}"
    response = httpx.get(f"https://app.welldatabase.com/api/v2/lease?name={lease_name}&stateId={stateId}",
                         headers=headers)
    leaseMatches = response.json()

    matchCount = leaseMatches['total']

    print("")
    print("")
    print(f"Found {matchCount} Leases(s) matching {lease_name}:")

    leaseIds = []
    for op in leaseMatches['data']:
        leaseIds.append(op['id'])

    if len(leaseIds) == 0:
        print(f"No Leases Found matching {lease_name}:")
        run_search()

    print(f"Found {len(leaseIds)} Lease(s) For {lease_name}")


    data = {
        'Filters': {
            'LeaseIds': {'Included': leaseIds},
        },
        'SortBy': 'DateCatalogued',
        'SortDirection': 'Descending',
        'PageSize': 10,
        'PageOffset': 0
    }

    response = httpx.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers, json=data)
    wellResults = response.json()
    matchCount = wellResults['total']

    print("")
    print("")
    print(f"Found {matchCount} Wells for Lease")

    for well in wellResults['data']:
        print(well['wellName'])

    print("Done...")
    run_search()

run_search()
