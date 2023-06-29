import requests
import json

#the well id you want here:
wellId = "3e70ef5a-9b8c-4153-8e6a-d3a962d6da57"
endpoint_url = "https://app.welldatabase.com/api/v2/wells/"+wellId
headers = {
    'Content-Type' : 'application/json',
    'User-Agent' : 'Your Application Name',
    'Api-Key' : 'YOUR Api Key'
}

response = requests.get(url = endpoint_url, headers = headers)
results = json.loads(response.content)
print results