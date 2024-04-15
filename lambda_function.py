import json
import requests
import pandas as pd
from datetime import datetime, date
import boto3

bucket_name = 'spotify-playlist-api-bkt-vi'
region = 'us-east-1'
bucket_list = []

# File name to store JSON data
today = date.today()
json_file_name = "spotify_usa_50_"+str(today)+".json"

# initialize s3 client
s3_client = boto3.client('s3')

# function to get access token from spotify.
def access_token():
    tokenUri = "https://accounts.spotify.com/api/token"
    header = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    tokenRequestBody = {
        'grant_type': 'client_credentials',
        'client_id': 'aa8c5e4adb3849e4afdd41ba16fb1732',
        'client_secret': '448cc4b0498c477787b7ef4e8a3edc5f'
    }
    response = requests.request('POST', url=tokenUri, headers=header, data=tokenRequestBody)
    access_token = response.json()['access_token']
    return access_token

# function to get playlist data from Spotify
def fetch_spotify_data():
    # USA top 50 songs playlist url 
    url = "https://api.spotify.com/v1/playlists/37i9dQZEVXbLp5XoPON0wI/tracks?market=US"
    header = {
        'Authorization': f'Bearer {access_token()}'
    }
    response = requests.request('GET', url=url, headers=header)
    item = response.json()
    # storing each track in the playlist into an empty list.
    item_list = []
    rec = pd.DataFrame(item['items'])['track']
    item_list.append(pd.DataFrame(rec))
    return item_list

# function to write JSON data to a file.
def write_json_to_file(data, file_name):
    file_path = f"/tmp/{file_name}"  # Writing to /tmp directory
    with open(file_path, 'w') as json_file:
        pd.concat(data).to_json(file_path, orient = 'records')
    return file_path

# function to upload a file to an S3 bucket.
def upload_to_s3(file_name, bucket_name, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
        return False
    return True

# function to create s3 bucket if it does not exist. bucket is created in us-east-1 by default.
def create_s3_bucket(bucket_name):
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name
            )
        print('Bucket created successfully: ',response)
        return response
    except s3_client.exceptions.BucketAlreadyExists as e:
        print('Bucket already exists!!')


# lambda_handler to execute the lambda_function.
def lambda_handler(event, context):
    buckets = s3_client.list_buckets()
    for i in buckets['Buckets']:
        bucket = i['Name']
        bucket_list.append(bucket)
    if bucket_name in bucket_list:
        print('Bucket already exists!!')
    else:
        create_s3_bucket(bucket_name)

    # Write JSON data to file
    file_path = write_json_to_file(fetch_spotify_data() , json_file_name)

    # Upload the file
    uploaded = upload_to_s3(file_path, bucket_name)