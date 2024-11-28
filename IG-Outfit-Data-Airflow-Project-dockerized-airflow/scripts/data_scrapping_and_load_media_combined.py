# %%
# Parameters
import argparse

# Create ArgumentParser
parser = argparse.ArgumentParser(description="Scrape data and load media for Instagram accounts.")

# Define the arguments you expect
parser.add_argument('--account_name', type=str, required=True, help="Instagram account name to scrape")
parser.add_argument('--http_proxy', type=str, required=True, help="HTTP proxy to use for this task")
parser.add_argument('--https_proxy', type=str, required=True, help="HTTPS proxy to use for this task")

# Parse the arguments
args = parser.parse_args()

# Access the values
account_name = args.account_name
http_proxy = args.http_proxy
https_proxy = args.https_proxy
print(f"http_proxy: {http_proxy}")
print(f"https_proxy: {https_proxy}")

# %%
import sys
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import instaloader
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
load_dotenv('.env')
# Import helper func
sys.path.append(os.getenv('BASE_HELPER_FUNC_PATH'))
import load_media
import proxy
import time


# MongoDB Connection
connection_string = os.getenv('MONGO_CONNECTION_STRING')
client = MongoClient(connection_string)
db = client.get_database()
print('Current Database:', db.name, 'Collections:', db.list_collection_names())


# %%
posts_collection = db['IGposts']
retry_collection = db['retry_shortcodes']

def post_exists(shortcode):
    return posts_collection.find_one({'shortcode': shortcode}) is not None
def is_retry_shortcode(shortcode):
    return retry_collection.find_one({'shortcode': shortcode}) is not None
def add_retry_shortcode(shortcode):
    """Adds a shortcode to the retry collection if not already present."""
    try:
        if not is_retry_shortcode(shortcode):
            retry_collection.insert_one({'shortcode': shortcode, 'timestamp': datetime.utcnow()})
            print(f"Added shortcode {shortcode} to retry collection.")
        else:
            print(f"Shortcode {shortcode} is already in the retry collection.")
    except Exception as e:
        print(f"Error adding shortcode {shortcode} to retry collection: {e}")

def delete_retry_shortcode(shortcode):
    """Deletes a shortcode from the retry collection."""
    try:
        result = retry_collection.delete_one({'shortcode': shortcode})
        if result.deleted_count > 0:
            print(f"Deleted shortcode {shortcode} from retry collection.")
        else:
            print(f"Shortcode {shortcode} not found in retry collection.")
    except Exception as e:
        print(f"Error deleting shortcode {shortcode} from retry collection: {e}")

def delete_all_files(directory_path):
    """Deletes all files in the specified directory."""
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f'Removed file: {file_path}')

def scrape_account(account_name, username=None, password=None, login_required=False):
    proxy.activate(http_proxy, https_proxy)
    L = instaloader.Instaloader()
    session = L.context._session
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    # Login to Instagram if required
    if login_required:
        try:
            L.load_session_from_file(username)  # Load session from file
            print(f"Loaded session for {username}")
        except FileNotFoundError:
            # If session file does not exist, log in and save session
            try:
                L.login(username, password)
                L.save_session_to_file()
                print(f"Logged in as {username} and saved session")
            except instaloader.exceptions.TwoFactorAuthRequiredException:
                code = input("Enter the two-factor authentication code: ")
                L.two_factor_login(code)
                L.save_session_to_file()
                print(f"Logged in as {username} with 2FA and saved session")
    else:
        print("Skipping login as per user choice.")
    Total_post_fetched=0
    # Fetch posts from the account
    try:
        try:    
            profile = instaloader.Profile.from_username(L.context, account_name)
            print(f"Fetched {account_name} profile")
            posts = profile.get_posts()
        except Exception as e:
            print("Error in fetching posts in the early stage", account_name, e)
            raise

        shortcode = ""
        for post in tqdm(posts, desc=f"Scraping {account_name}", unit="post"):
            try:
                shortcode = post.shortcode
                image_urls = []
                post_img_fetch_failed = False
                #reel_urls = []
                Total_post_fetched+=1
                if post_exists(shortcode) and not is_retry_shortcode(shortcode):
                    print(f"Post already exist or this is not a retry shortcode. Skipping shortcode {shortcode}")
                    continue
                else:
                    print(f"Start processing shortcode {shortcode}")

                if post.is_video:
                    # reel_urls.append(post.video_url)
                    continue
                else:
                    if post.mediacount > 1:
                        image_urls.extend([node.display_url for node in post.get_sidecar_nodes()])
                    else:
                        image_urls.append(post.url)

                # Introduce a delay of N seconds before processing each post
                Idle_time = 5
                print(f"Started delay for {Idle_time} seconds")
                time.sleep(Idle_time)
                print(f"Delay ended, continue processing.")

                # Download images and store in blob
                for idx, image_url in enumerate(image_urls):
                    sanitized_post_date = load_media.sanitize_filename(str(post.date_utc))

                    # Download image from ig and store in local storage
                    file_name = f"{account_name}_{sanitized_post_date}_{shortcode}_image_{idx}.jpg"
                    file_path = os.path.join(os.getenv("BASE_DATA_TEMP_IMG_PATH"), account_name, file_name)
                    # Ensure the directory exists
                    directory = os.path.dirname(file_path)  # Extract the directory part of the file path
                    os.makedirs(directory, exist_ok=True)  # Create the directory if it doesn't exist
                    proxy.activate(http_proxy, https_proxy)
                    saved_path = load_media.download_media(image_url, file_path)
                    proxy.deactivate()
                    
                    # Store image into azure blob
                    container_name = 'outfit-data'
                    base_path = 'ig-media'
                    uploaded_image_url = []
                    if saved_path:
                        load_media.upload_to_azure_blob(container_name, saved_path, f"{base_path}/{account_name}/{file_name}")
                        uploaded_image_url.append(f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net/{container_name}/{base_path}/{account_name}/{file_name}")
                    else:
                        post_img_fetch_failed= True
                    # Insert image link and post info into azure mongodb
                    load_media.insert_post(
                        shortcode, 
                        post.date_utc,
                        account_name, 
                        post.caption or "",
                        post.likes,
                        media_count = post.mediacount if not post.is_video else 1,
                        video_view_count = post.video_view_count if post.is_video else None,
                        image_urls = uploaded_image_url
                    )

                    Idle_time = 3
                    print(f"Started delay for {Idle_time} seconds")
                    time.sleep(Idle_time)
                    print(f"Delay ended, continue processing.")
                    
                delete_all_files(os.path.join(os.getenv("BASE_DATA_TEMP_IMG_PATH"),account_name))
                if not post_img_fetch_failed:
                    delete_retry_shortcode(shortcode)
            except Exception as e:
                print(f"Error processing post with shortcode {post.shortcode}: {e}")
                add_retry_shortcode(shortcode)
                continue  # Ensure the loop continues even after an error
    except Exception as e:
        print('Error fetching accounts: ' + account_name, e)
        print(f"Total of {Total_post_fetched} {account_name}'s post fetched")
        raise

# %%
# Example usage
scrape_account(account_name, os.getenv('IG_USERNAME'), os.getenv('IG_PASSWORD'), login_required=False)


