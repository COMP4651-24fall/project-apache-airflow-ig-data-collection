# Parameters
import sys
csv_file_path = sys.argv[1]  # Get account name from first argument
# %%
import pandas as pd
import os
import requests
import ast
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv('.env')

# Proxy set up 
os.environ["HTTP_PROXY"] = os.getenv('HTTP_PROXY')
os.environ["HTTPS_PROXY"] = os.getenv('HTTPS_PROXY')

# MongoDB Connection
connection_string = os.getenv('MONGO_CONNECTION_STRING')
client = MongoClient(connection_string)
db = client.get_database()
print('Current Database:', db.name, 'Collections:', db.list_collection_names())

# Azure Blob Storage connection
blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))


# %%
def sanitize_filename(filename):
    # Replace invalid characters with underscores
    return filename.replace(':', '_').replace('/', '_').replace('\\', '_')

def download_media(url, save_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {save_path}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

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
                'createdAt': current_time,
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
            posts_collection.insert_one(post)
            print(f"Inserted image post with shortcode: {shortcode}, URL: {url}")
    
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

def process_scraped_data(csv_file):
    container_name = 'outfit-data'
    base_path = 'ig-media'

    download_directory = "data/temp_images"
    os.makedirs(download_directory, exist_ok=True)

    # Read the CSV file using pandas
    print(f"reading csv from path: {csv_file}")
    data = pd.read_csv(csv_file)

    for index, row in data.iterrows():
        shortcode = row['shortcode']
        post_date = row['post_date']
        account_name = row['account_name']
        caption = row['caption']
        like_count = row['like_count']
        media_count = row['media_count']
        video_view_count = row['video_view_count']
        image_urls = ast.literal_eval(row['image_urls']) if pd.notna(row['image_urls']) else []
        reel_urls = ast.literal_eval(row['reel_urls']) if pd.notna(row['reel_urls']) else []

        uploaded_image_urls = []
        uploaded_reel_urls = []

        sanitized_post_date = sanitize_filename(post_date)

        # Download images
        print(f"image_urls = {image_urls}")
        for idx, url in enumerate(image_urls):
            file_name = f"{account_name}_{sanitized_post_date}_{shortcode}_image_{idx}.jpg"
            file_path = os.path.join(download_directory, file_name)
            download_media(url, file_path)
            upload_to_azure_blob(container_name, file_path, f"{base_path}/{account_name}/{file_name}")
            uploaded_image_url = f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net/{container_name}/{base_path}/{account_name}/{file_name}"
            uploaded_image_urls.append(uploaded_image_url)

        # Download reels
        for idx, url in enumerate(reel_urls):
            file_name = f"{account_name}_{sanitized_post_date}_{shortcode}_reel_{idx}.mp4"
            file_path = os.path.join(download_directory, file_name)
            download_media(url, file_path)
            upload_to_azure_blob(container_name, file_path, f"{base_path}/{account_name}/{file_name}")
            uploaded_reel_url = f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net/{container_name}/{base_path}/{account_name}/{file_name}"
            uploaded_reel_urls.append(uploaded_reel_url)

        # Insert post to db IGposts with the uploaded URLs, where each row contains only 1 image or reel
        insert_post(
            shortcode,
            post_date,
            account_name,
            caption,
            like_count,
            is_outfit="unchecked",
            video_view_count=video_view_count,
            image_urls=uploaded_image_urls,  # Use the uploaded image URLs
            reel_url=uploaded_reel_urls  # Use the uploaded reel URLs
        )
        # Cleanup: Optionally delete the downloaded files (Todo: this will cause error to airflow code, add a new task to do this delete action)
        # for idx in range(len(image_urls)):
        #     os.remove(os.path.join(download_directory, f"{account_name}_{sanitized_post_date}_{shortcode}_image_{idx}.jpg"))
        # for idx in range(len(reel_urls)):
        #     os.remove(os.path.join(download_directory, f"{account_name}_{sanitized_post_date}_{shortcode}_reel_{idx}.mp4"))


# %%
if __name__ == "__main__":
    process_scraped_data(csv_file_path)


