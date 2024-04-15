import json
import requests
# import pandas as pd
from datetime import date
import boto3

bucket_name = 'spotify-playlist-vi'
region = 'us-east-1'

bucket_list = []

# File name to store JSON data
json_file_name = 'data.json'

s3_client = boto3.client('s3')

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



def write_json_to_file(data, file_name):
    """Write JSON data to a file.

    Args:
        data (dict): JSON data to write.
        file_name (str): Name of the file to write to.
    """
    file_path = f"/tmp/{file_name}"  # Writing to /tmp directory
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)
    return file_path

def upload_to_s3(file_name, bucket_name, object_name=None):
    """Upload a file to an S3 bucket.

    Args:
        file_name (str): Local path to the file.
        bucket_name (str): Name of the S3 bucket.
        object_name (str): S3 object name. If not specified then file_name is used.

    Returns:
        bool: True if file was uploaded, else False.
    """
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



def create_s3_bucket(bucket_name):
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-1'}
            )
        
        print('Bucket created successfully: ',response)
        return response
    
    except s3_client.exceptions.BucketAlreadyExists as e:
        print('Bucket already exists!!')
        # print(e.response)
        # return str(e)



def lambda_handler(event, context):

    buckets = s3_client.list_buckets()
    for i in buckets['Buckets']:
        bucket = i['Name']
        bucket_list.append(bucket)
        
    
    if bucket_name in bucket_list:
        print('Bucket already exists!!')
    else:
        create_s3_bucket(bucket_name)
        
    # Playlists
    url = "https://api.spotify.com/v1/playlists/37i9dQZEVXbLp5XoPON0wI/tracks?market=US"
    
    header = {
        'Authorization': f'Bearer {access_token()}'
    }
    
    response = requests.request('GET', url=url, headers=header)
    json_data = response.json()

    # Write JSON data to file
    file_path = write_json_to_file(json_data, json_file_name)

    # Upload the file
    uploaded = upload_to_s3(file_path, bucket_name)
    
    # item_list = []
    # rec = pd.DataFrame(item['items'])['track']
    # item_list.append(pd.DataFrame(rec))
    
    # today = date.today()
    # file = "spotify_usa_50_"+str(today)+".json"
    # pd.concat(item_list).to_json(file, orient = 'records')