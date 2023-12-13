import datetime
import httpx

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your API Key'
}

modifiedSince = datetime.datetime.now() - datetime.timedelta(days=10)


data = {
    'Filters': {
        'Api10': ["4232942588"],
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 2,
    'Page': 1
}

response = httpx.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers, json=data)
results = response.json()

print(results)