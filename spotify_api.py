import json
import requests
import pandas as pd
from datetime import date


def access_token():
    tokenUri = "https://accounts.spotify.com/api/token"

    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    tokenRequestBody = {
        'grant_type': 'client_credentials',
        'client_id': '<your_client_id>',
        'client_secret': '<your_client_secret>'
    }

    response = requests.request('POST', url=tokenUri, headers=header, data=tokenRequestBody)

    access_token = response.json()['access_token']

    return access_token


# API call to get playlist.
url = "https://api.spotify.com/v1/playlists/37i9dQZEVXbLp5XoPON0wI/tracks?market=US"
header = {'Authorization': f'Bearer {access_token()}'}
response = requests.request('GET', url=url, headers=header)
item = response.json()

# storing each track in the playlist into an empty list.
item_list = []
rec = pd.DataFrame(item['items'])['track']
item_list.append(pd.DataFrame(rec))

# exporting data into a json file.
today = date.today()
file = "spotify_usa_50_"+str(today)+".json"
pd.concat(item_list).to_json(file, orient = 'records')