import asyncio
import datetime
import httpx
from zipfile import ZipFile
import os
import glob
import pandas as pd

extractTo = "./extracted"
pageSize = 1000000
api_endpoint = "https://app.welldatabase.com/api/v2/wells/export"
api_key = "Your API Key"


class TaskQueue:
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.sem = asyncio.BoundedSemaphore(maxsize)
        self.tasks = set()

    def __len__(self):
        return len(self.tasks)

    def _task_done(self, task):
        self.sem.release()
        self.tasks.discard(task)

    async def put(self, coroutine):
        await self.sem.acquire()
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self._task_done)

    async def join(self):
        return await asyncio.gather(*self.tasks)


async def run_export(client, payload, headers, lower_bound, upper_bound):
    print("Retrieving Rows: " + format(lower_bound, ",") + " - " + format(upper_bound, ","))
    response = await client.post(api_endpoint, headers=headers,
                                 json=payload,
                                 timeout=None)

    if response.status_code != httpx.codes.OK:
        response.raise_for_status()

    fileName = "./wdb-export_" + str(lower_bound) + "_" + str(upper_bound) + ".zip"
    file = open(fileName, "wb")

    file.write(response.content)
    file.close()

    # extract zip file
    with ZipFile(fileName, 'r') as zObject:
        zObject.extractall(extractTo, members=None, pwd=None)

    # optionally, delete zip
    os.remove(fileName)


async def download():
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Sample Python Application',
        'Api-Key': api_key
    }

    now = datetime.datetime.now()
    modifiedSince = now - datetime.timedelta(days=10)

    filters = {
        'DateCatalogued': {
            'Min': modifiedSince.strftime("%Y-%m-%d"),
            'Max': now.strftime("%Y-%m-%d")
            # use current run date/time to create a fixed upper bound, this can be used as the next min date to generate delta exports going forward.
        }
    }

    searchPayload = {
        'Filters': filters,
        'SortBy': 'DateCatalogued',
        'SortDirection': 'Descending',
        'PageSize': 1,
        'Page': 1
    }

    async with httpx.AsyncClient() as client:
        # Get the total rows that match the search criteria
        response = await client.post("https://app.welldatabase.com/api/v2/wells/search", headers=headers,
                                     json=searchPayload)
        result = response.json()

        totalRowsToExport = result['total']
        print("Total Rows To Export: " + format(totalRowsToExport, ","))

        upperBound = totalRowsToExport + pageSize

        tasks = TaskQueue(4)

        for i in range(0, upperBound, pageSize):
            exportPayload = {'Filters': filters,
                             'Settings': [{'Key': 'Skip', 'Value': i}, {'Key': 'Limit', 'Value': pageSize}]}

            downloadTask = run_export(client, exportPayload, headers, i, i + pageSize)
            await tasks.put(downloadTask)

        await tasks.join();

    # get all the csv files
    csv_files = glob.glob(extractTo + '/*.csv')

    print("Combining " + format(len(csv_files), ",") + " Files")

    content = []

    # combine the CSV files
    for file in csv_files:
        df = pd.read_csv(file, engine='python', index_col=None)

        content.append(df)

        # optionally, remove csv
        # os.remove(file)

    df_csv_append = pd.concat(content, ignore_index=True)

    csv_path = extractTo + "/combined-exports.csv"

    print("Writing " + format(len(df_csv_append), ",") + " data rows to csv at " + csv_path)

    # write combine csv file
    df_csv_append.to_csv(csv_path)


print("Starting Exports - " + datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
asyncio.run(download())
print("Done - " + datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
