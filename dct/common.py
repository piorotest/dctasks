import json
import time
import requests
requests.packages.urllib3.disable_warnings()


# import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1

BASE_URL = ''
API_KEY = ''

def set_headers():
    headers = {
        'content-type': "application/json", 
        'accept': "application/json", 
        'authorization': API_KEY
    }
    return headers

def DCT_GET(action):
    response = requests.get(BASE_URL + "/v2/" + action, headers=set_headers(), verify=False )
    if response.status_code in [ 200, 201 ]:
        return json.loads(response.content)
    else:
        return None


def DCT_POST(action, payload):
    url = BASE_URL + "/v2/" + action
    response = requests.post(url, headers=set_headers(), verify=False, json=payload  )
    if response.status_code in [ 200, 201 ]:
        return json.loads(response.content)
    else:
        print(f"Return code: {response.status_code}")
        return None


def DCT_DELETE(action):
    response = requests.delete(BASE_URL + "/v2/" + action, headers=set_headers(), verify=False )
    print(response.status_code)
    if response.status_code in [ 204 ]:
        return "OK"
    elif response.status_code in [ 404 ]:
        return "N/A"
    else:
        return None


def wait_for_job(job_id: str):
    jobrunning = True
    while(jobrunning):
        res = DCT_GET(f'jobs/{job_id}')
        print(res)
        if res["status"] in ['RUNNING','STARTED']:
            time.sleep(10)
        else:
            jobrunning = False

    return res


