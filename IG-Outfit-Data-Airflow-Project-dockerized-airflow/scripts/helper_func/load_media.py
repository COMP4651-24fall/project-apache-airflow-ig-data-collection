import pandas as pd
import os
import requests
import ast
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv('.env')

# MongoDB Connection
connection_string = os.getenv('MONGO_CONNECTION_STRING')
client = MongoClient(connection_string)
db = client.get_database()
print('Current Database:', db.name, 'Collections:', db.list_collection_names())

# Azure Blob Storage connection
blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))


def download_media(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {save_path}")
        return save_path
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None

def upload_to_azure_blob(container_name, file_path, blob_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_path} to Azure Blob Storage as {blob_name}")
    except Exception as e:
        print(f"Error uploading {file_path} to Azure Blob Storage: {e}")

def insert_post(shortcode, post_date, account_name, caption, like_count, is_outfit="unchecked", media_count=1, video_view_count=None, image_urls=None, reel_url=None, merged_same_outfit=False):
    posts_collection = db['IGposts']
    current_time = datetime.now()
    
    # Handle image URLs
    if image_urls:
        for url in image_urls:
            post = {
                'shortcode': shortcode,
                'post_date': post_date,
                'updatedAt': current_time,
                'account_name': account_name,
                'caption': caption,
                'like_count': like_count,
                'isOutfit': is_outfit,
                'media_count': media_count,
                'video_view_count': video_view_count,
                'imageURLs': [url],  # Keep as array but with single URL
                'reelURL': None,
                'merged_same_outfit': merged_same_outfit
            }
            posts_collection.update_one(
                {'shortcode': shortcode, 'imageURLs': [url]},  # filter criteria
                {
                    '$set': post,  # update operation
                    '$setOnInsert': {'createdAt': current_time}  # set createdAt only if inserting
                },
                upsert=True  # create if doesn't exist
            )
            print(f"Upserted image post with shortcode: {shortcode}, URL: {url}")
    
    # Handle reel URLs
    if reel_url:
        for url in reel_url:  # Assuming reel_url is a list
            post = {
                'shortcode': shortcode,
                'post_date': post_date,
                'createdAt': current_time,
                'updatedAt': current_time,
                'account_name': account_name,
                'caption': caption,
                'like_count': like_count,
                'isOutfit': is_outfit,
                'video_view_count': video_view_count,
                'imageURLs': None,
                'reelURL': [url],  # Keep as array but with single URL
                'merged_same_outfit': merged_same_outfit
            }
            posts_collection.insert_one(post)
            print(f"Inserted reel post with shortcode: {shortcode}, URL: {url}")
def sanitize_filename(filename):
    # Replace invalid characters with underscores
    return filename.replace(':', '_').replace('/', '_').replace('\\', '_')
