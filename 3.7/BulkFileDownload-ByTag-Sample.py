import datetime
import httpx
from zipfile import ZipFile
import os
import math

extract_to = "./raster-file-downloads"
page_size = 100

base_url = "https://app.welldatabase.com/api/v2"
api_endpoint = "%s/files/search"
api_key = "Your Api Key"

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': api_key
}

filters = {
    'Tags': ["Log Image"],
    'HeaderFilters': {
        'CountyIds': {
            'Included': [
                1953
            ]
        }
    }
}

searchPayload = {
    'Filters': filters,
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 1,
    'Page': 1
}

with httpx.Client() as client:
    # Get the total rows that match the search criteria
    response = client.post("%s/files/search" % base_url, headers=headers, json=searchPayload, timeout=None)
    result = response.json()

    totalFilesToDownload = result['total']
    print("Total Files To Download: " + format(totalFilesToDownload, ","))

    # Generate the Exports
    upperBound = totalFilesToDownload + page_size
    pages = math.ceil(totalFilesToDownload / page_size)

    print(datetime.datetime.now())

    for i in range(0, totalFilesToDownload, page_size):
        print("Retrieving Rows:  " + format(i, ",") + "-" + format(i + page_size, ","))

        searchRequest = {'Filters': filters, 'PageSize': page_size, 'PageOffSet': i}

        fileSearchResponse = client.post("%s/files/search" % base_url, headers=headers,
                                         json=searchRequest, timeout=None)

        fileResults = fileSearchResponse.json();

        ids = []
        for row in fileResults['data']:
            ids.append(row['id'])

        downloadRequest = {
            'Filters': {
                'Ids': ids
            }
        }

        fileDownloadResponse = client.post("%s/files/download" % base_url, headers=headers,
                                           json=downloadRequest, timeout=None)

        fileName = "./wdb-files_" + format(i, ",") + "_" + format(i + page_size, ",") + ".zip"
        file = open(fileName, "wb")
        file.write(fileDownloadResponse.content)
        file.close()

        # extract zip file
        with ZipFile(fileName, 'r') as zObject:
            zObject.extractall(extract_to, members=None, pwd=None)

        # optionally, delete zip
        os.remove(fileName)

print("Done")
print(datetime.datetime.now())
