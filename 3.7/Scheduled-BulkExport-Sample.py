import datetime
import httpx
from zipfile import ZipFile
import os
import glob
import sched
import time

base_address = 'https://app.welldatabase.com/api/v2/'
export_format = 'mica'
exported_file_ext = '.xml'

extract_files_to_directory = "./extracted"

# get scheduler
scheduler = sched.scheduler(time.time, time.sleep)

run_every_hours = 24
seconds_per_hour = 3600
run_every_seconds = run_every_hours * seconds_per_hour

# should we run an export on start up?
run_on_start = True


def run_export():
    print('Running Export')

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Sample Python Application',
        'Api-Key': 'Your API Key'
    }

    now = datetime.datetime.now()
    wells_with_prod_since = datetime.datetime(year=now.year - 1, month=1, day=1)

    filters = {
        'FirstProdDate': {
            'Min': wells_with_prod_since.strftime("%Y-%m-%d")
        },
        #'LastProdDate': {
        #    'Min': wells_with_prod_since.strftime("%Y-%m-%d")
        #}
    }

    search_payload = {
        'Filters': filters,
        'SortBy': 'DateCatalogued',
        'SortDirection': 'Descending',
        'PageSize': 1,
        'PageOffset': 0
    }

    timeout = httpx.Timeout(5, read=None)
    with httpx.Client(timeout=timeout) as client:
        # Get the total rows that match the search criteria
        response = client.post(base_address + "wells/search", headers=headers, json=search_payload)
        result = response.json()

        totalRowsToExport = result['total']
        print("Total Rows To Download: " + str(totalRowsToExport))

        max_export_size = 1000000

        if totalRowsToExport > max_export_size:
            raise Exception("Maximum export size is " + str(max_export_size) + " please add filters and try again")

        file_timestamp = now.strftime("%Y-%m-%d_%I-%M-%S_%p")
        file_name = "./wdb-export_" + file_timestamp + ".zip"

        export_payload = {'Filters': filters,
                          'ExportFormat': export_format,
                          'Settings': [{'Key': 'VolumeType', 'Value': 'Lease'},
                                       {'Key': 'IncludeForecast', 'Value': 'true'}]}

        response = client.post(base_address + "wells/export", headers=headers,
                               json=export_payload, timeout=timeout)

        file = open(file_name, "wb")
        file.write(response.content)
        file.close()

        # extract zip file
        with ZipFile(file_name, 'r') as zObject:
            zObject.extractall(extract_files_to_directory, members=None, pwd=None)

        # optionally, delete zip
        os.remove(file_name)

        export_files = glob.glob(extract_files_to_directory + '/*' + exported_file_ext)

        if (len(export_files) == 0):
            raise Exception("Could not find any files with extension " + exported_file_ext)

        print('Export Complete, Export Written to file : ' + export_files[0])


def start_export_schedule():
    run_export()
    schedule_next_run()


def schedule_next_run():
    now = datetime.datetime.now()
    next_run = now + datetime.timedelta(seconds=run_every_seconds)
    print("Scheduled To Run " + next_run.strftime("%Y-%m-%d %H:%M:%S"))
    scheduler.enter(run_every_seconds, 1, start_export_schedule, ())


if run_on_start:
    start_export_schedule()
else:
    schedule_next_run()

# schedule next runs
scheduler.run()
