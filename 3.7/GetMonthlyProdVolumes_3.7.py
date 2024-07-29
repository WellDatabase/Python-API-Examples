import httpx

API_ENDPOINT = "https://app.welldatabase.com/api/v2/productionVolumes/search"

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your Api Key'
}

wellIds = [
    "3e70ef5a-9b8c-4153-8e6a-d3a962d6da57",
    "4a775d59-838e-452c-8205-8c58d8b466f1"
]
data = {
    'Filters': {
        'HeaderFilters': {
            'InfinityIds': wellIds
        }
    },
    'SortBy': '',
    'SortDirection': 'Descending',
    'PageSize': 2,
    'PageOffset': 0
}

r = httpx.post(url=API_ENDPOINT, headers=headers, json=data)
Parsed_data = r.json()
print(Parsed_data)
