import datetime
import httpx
from zipfile import ZipFile
import os
import glob
import pandas as pd

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Sample Python Application',
    'Api-Key': 'Your Api Key'
}


now = datetime.datetime.now()
modifiedSince = now - datetime.timedelta(days=10)

filters = {
        'DateLastModified': {
            'Min': modifiedSince.strftime("%Y-%m-%d"),
            'Max' : now.strftime("%Y-%m-%d") #use current run date/time to create afixed upper bound, this can  be used as the next min date to generate delta exports going forward.
        }
    }

searchPayload = {
    'Filters': filters,
    'SortBy': 'DateCatalogued',
    'SortDirection': 'Descending',
    'PageSize': 1,
    'Page': 1
}

# Get the total rows that match the search criteria
response = httpx.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers, json=searchPayload)
result = response.json()

totalRowsToExport = result['total']
print("Total Rows To Download: " + str(totalRowsToExport))

# Generate the Exports
pageSize = 10000
upperBound = totalRowsToExport + pageSize

extractTo = "./extracted"

with httpx.Client() as client:
    for i in range(0, upperBound, pageSize):
        print("Retrieving Rows:  " + str(i) + "-" + str(i + pageSize))

        exportPayload = {'Filters': filters, 'Settings': [{'Key': 'Skip', 'Value': i}, {'Key': 'Limit', 'Value': pageSize}]}

        response = client.post("https://app.welldatabase.com/api/v2/wells/export", headers=headers, json=exportPayload, timeout=None)

        fileName = "./wdb-export_" + str(i) + "_" + str(i + pageSize) + ".zip"
        file = open(fileName, "wb")
        file.write(response.content)
        file.close()

        # extract zip file
        with ZipFile(fileName, 'r') as zObject:
            zObject.extractall(extractTo, members=None, pwd=None)

        #optionally, delete zip
        os.remove(fileName)

#get all the csv files
csv_files = glob.glob(extractTo + '/*.csv')

print("Combining " + str(len(csv_files)) + " Files")

content = []

# combine the CSV files
for file in csv_files:
    df = pd.read_csv(file, engine='python', index_col=None)

    content.append(df)
    #optionally, remove csv
    os.remove(file)

df_csv_append = pd.concat(content, ignore_index=True)

#write combine csv file
df_csv_append.to_csv(extractTo + "/combined-exports.csv")

print("Done")
