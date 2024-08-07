import datetime
import requests

headers = {
    'Content-Type' : 'application/json',
    'User-Agent' : 'Sample Application',
    'Api-Key' : 'Your Api Key'
}

modifiedSince = datetime.datetime.now() - datetime.timedelta(days=100)

data = {
    'Filters': {
        'DateLastModified': {
            'Min': modifiedSince.strftime("%Y-%m-%d")
        }
    },
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 2,
    'PageOffset': 0
}

response = requests.post(url = "https://app.welldatabase.com/api/v2/wells/search", headers = headers, json = data)
results = response.json()

print results
