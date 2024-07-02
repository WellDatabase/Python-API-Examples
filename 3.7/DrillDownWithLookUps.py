import datetime
import re

import httpx

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'YOUR API KEY'
}

modifiedSince = datetime.datetime.now() - datetime.timedelta(days=10)

# working within texas
stateId = 42
operatorName = 'Comstock Energy'

response = httpx.get(f"https://app.welldatabase.com/api/v2/operator?name={operatorName}&stateId={stateId}",
                     headers=headers)
operatorMatches = response.json()

matchCount = operatorMatches['total']

print("")
print("")
print(f"Found {matchCount} Operator(s) matching {operatorName}")

for op in operatorMatches['data']:
    print(op['name'])

operatorId = operatorMatches['data'][0]['id']

response = httpx.get(f"https://app.welldatabase.com/api/v2/operator/{operatorId}/leases?stateId={stateId}",
                     headers=headers)
leaseMatches = response.json()

matchCount = leaseMatches['total']
print("")
print("")
print(f"Found {matchCount} Lease(s) For Operator {operatorId}")

for ls in leaseMatches['data']:
    leaseName = ls['name']
    parts = re.search("(..)-(.....)?\s*:\s*(.*)?", leaseName)

    if parts == None:
        # No distirict/lease id info found
        print(leaseName + " ( " + ("No District") + " | " + ("No Lease Assigned") + " | " + ("No Lease Name") + " )")
    else:
        print(leaseName + " ( " + (parts.group(1) or "No District") + " | " + (
                parts.group(2) or "No Lease Assigned") + " | " + (parts.group(3) or "No Lease Name") + " )")

leaseId = leaseMatches['data'][0]['id']

data = {
    'Filters': {
        'LeaseIds': {'Included': [leaseId]},
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 2,
    'Page': 1
}

response = httpx.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers, json=data)
wellResults = response.json()
matchCount = wellResults['total']

print("")
print("")
print(f"Found {matchCount} Wells for Lease {leaseId}")

for well in wellResults['data']:
    print(well['wellName'])
