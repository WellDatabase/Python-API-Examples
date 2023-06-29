import datetime
import httpx

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Application',
    'Api-Key': 'Your Api Key'
}

modifiedSince = datetime.datetime.now() - datetime.timedelta(days=10)


data = {
    'Filters': {
        'DateLastModified': {
            'Min': modifiedSince.strftime("%Y-%m-%d")
        }
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 2,
    'Page': 1
}

response = httpx.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers, json=data)
results = response.json()

print(results)