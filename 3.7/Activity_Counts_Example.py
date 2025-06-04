import datetime
import httpx

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your API Key'
}


shale_name = input('Enter Play Name: ').strip()
date_since = input('Enter Min Date: ').strip() or '2025-01-01'

# county_name = f"{district}-{lease_number}"
response = httpx.get(f"https://app.welldatabase.com/api/v2/shaleplay?name={shale_name}",
                     headers=headers)

result = response.json()

if(len(result['data']) == 0):
    print("No Results Found")
    exit()

since = date_since
shalePlays = {
    'Included': [
        result['data'][0]['id']
    ]
}
rigFilters = {
    'Filters': {
        "ReportDate": {"Min": since},
        'HeaderFilters': {
            'ShalePlayIds': shalePlays
        }
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 0,
    'PageOffset': 0
}

response = httpx.post("https://app.welldatabase.com/api/v2/rig/search", headers=headers, json=rigFilters)
results = response.json()
totalRows = results['total']
print("Total Rigs: " + str(totalRows))

completionFilters = {
    'Filters': {
        "CompletionDate": {"Min": since},
        'HeaderFilters': {
            'ShalePlayIds': shalePlays
        }
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 0,
    'PageOffset': 0
}

response = httpx.post("https://app.welldatabase.com/api/v2/completions/search", headers=headers, json=completionFilters)
results = response.json()
totalRows = results['total']
print("Total Completions: " + str(totalRows))

wellFilters = {
    'Filters': {
        "FirstSpudDate": {"Min": since},
        'ShalePlayIds': shalePlays
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 0,
    'PageOffset': 0
}

response = httpx.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers, json=wellFilters)
results = response.json()
totalRows = results['total']
print("Total New Drills: " + str(totalRows))
