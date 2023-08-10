import datetime
import httpx
from zipfile import ZipFile
import os
import math

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Application',
    'Api-Key': 'Your Api Key'
}

filters = {
    'Tag' : "Log Image"
}

searchPayload = {
    'Filters': filters,
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 1,
    'Page': 1
}

extractTo = "./raster-file-downloads"
baseUrl = "https://app.welldatabase.com/api/v2"

with httpx.Client() as client:
    # Get the total rows that match the search criteria
    response = client.post("%s/filetags/search" % baseUrl, headers=headers, json=searchPayload, timeout=None)
    result = response.json()

    totalFilesToDownload = result['total']
    print("Total Matches: " + str(totalFilesToDownload))

    # Generate the Exports
    pageSize = 100
    upperBound = totalFilesToDownload + pageSize
    pages = math.ceil(totalFilesToDownload / pageSize)

    print(datetime.datetime.now())

    ids = []
    for i in range(0, pages, 1):
        print("Retrieving Rows:  " + str(i) + "-" + str(i + pageSize))

        searchRequest = {'Filters': filters, 'PageSize': pageSize, 'PageOffSet': i}

        fileSearchResponse = client.post("%s/filetags/search" % baseUrl, headers=headers,
                                         json=searchRequest, timeout=None)

        fileResults = fileSearchResponse.json();


        for row in fileResults['data']:
            fileId = row['fileId']
            #temp fix for duplicate tags
            if fileId not in ids:
                ids.append(fileId)

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

        #reset ids
        ids = []

print("Done")
print(datetime.datetime.now())