import datetime
import httpx
from zipfile import ZipFile
import os
import glob
import math

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your Api Key'
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
    'Page': 1
}

extract_to = "./file-downloads"
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

        fileResults = fileSearchResponse.json()

        for row in fileResults['data']:
            file_id = row['id']
            print("Retrieving File:  " + str(file_id))
            downloadRequest = {
                'Filters': {
                    'Ids': [file_id]
                }
            }

            fileDownloadResponse = client.post("%s/files/download" % baseUrl, headers=headers,
                                               json=downloadRequest, timeout=None)

            file_name = "./wdb-files_" + str(i) + "_" + str(i + pageSize) + ".zip"

            with open(file_name, "wb") as file:
                file.write(fileDownloadResponse.content)

            # extract zip file
            with ZipFile(file_name, 'r') as zObject:
                zObject.extractall(extract_to, members=None, pwd=None)

            # optionally, delete zip
            os.remove(file_name)

print("Done")
print(datetime.datetime.now())
