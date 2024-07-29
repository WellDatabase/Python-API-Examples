import datetime
import httpx
from zipfile import ZipFile
import os
import glob
import math

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your API Key'
}

filters = {
    'FileName': "*.las",
    'HeaderFilters': {
        'CountyIds': {
            'Included': [
                2904
            ]
        }
    }
}

searchPayload = {
    'Filters': filters,
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 1,
    'PageOffset': 0
}

extractTo = "./file-downloads"
baseUrl = "https://app.welldatabase.com/api/v2"

with httpx.Client() as client:
    # Get the total rows that match the search criteria
    response = client.post("%s/files/search" % baseUrl, headers=headers, json=searchPayload, timeout=None)
    result = response.json()

    totalFilesToDownload = result['total']
    print("Total Files To Download: " + str(totalFilesToDownload))

    # Generate the Exports
    pageSize = 100
    upperBound = totalFilesToDownload + pageSize
    pages = math.ceil(totalFilesToDownload / pageSize)

    print(datetime.datetime.now())

    for i in range(0, pages, 1):
        print("Retrieving Rows:  " + str(i) + "-" + str(i + pageSize))

        searchRequest = {'Filters': filters, 'PageSize': pageSize, 'PageOffSet': i}

        fileSearchResponse = client.post("%s/files/search" % baseUrl, headers=headers,
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

        fileDownloadResponse = client.post("%s/files/download" % baseUrl, headers=headers,
                                           json=downloadRequest, timeout=None)

        fileName = "./wdb-files_" + str(i) + "_" + str(i + pageSize) + ".zip"
        file = open(fileName, "wb")
        file.write(fileDownloadResponse.content)
        file.close()

        # extract zip file
        with ZipFile(fileName, 'r') as zObject:
            zObject.extractall(extractTo, members=None, pwd=None)

        # optionally, delete zip
        os.remove(fileName)

print("Done")
print(datetime.datetime.now())
