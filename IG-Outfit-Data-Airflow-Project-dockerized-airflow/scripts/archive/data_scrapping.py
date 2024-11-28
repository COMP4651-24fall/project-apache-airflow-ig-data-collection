# %%
# Parameters
import sys
account_name = sys.argv[1]  # Get account name from first argument


# %%
import os
import csv
from pymongo import MongoClient
from dotenv import load_dotenv
import instaloader
from datetime import datetime
from tqdm import tqdm
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
load_dotenv('.env')

# Proxy set up 
os.environ["HTTP_PROXY"] = os.getenv('HTTP_PROXY')
os.environ["HTTPS_PROXY"] = os.getenv('HTTPS_PROXY')

# MongoDB Connection
connection_string = os.getenv('MONGO_CONNECTION_STRING')
client = MongoClient(connection_string)
db = client.get_database()
print('Current Database:', db.name, 'Collections:', db.list_collection_names())
# path for stroging file
csv_file_path = os.getenv('IG_POST_CSV_PATH')

# %%
posts_collection = db['IGposts']

def post_exists(shortcode):
    return posts_collection.find_one({'shortcode': shortcode}) is not None

def scrape_account(account_name, username=None, password=None, login_required=False):
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

    # Fetch posts from the account
    try:
        profile = instaloader.Profile.from_username(L.context, account_name)
        print(f"Fetched {account_name} profile")
        posts = profile.get_posts()

        data = []

        for post in tqdm(posts, desc=f"Scraping {account_name}", unit="post"):
            shortcode = post.shortcode
            image_urls = []
            reel_urls = []

            if post_exists(shortcode):
                print(f"Post with shortcode {shortcode} already exists. Skipping.")
                continue

            if post.is_video:
                reel_urls.append(post.video_url)
            else:
                if post.mediacount > 1:
                    image_urls.extend([node.display_url for node in post.get_sidecar_nodes()])
                else:
                    image_urls.append(post.url)

            data.append({
                'shortcode': shortcode,
                'post_date': post.date_utc,
                'account_name': account_name,
                'caption': post.caption or "",
                'like_count': post.likes,
                'media_count': post.mediacount if not post.is_video else 1,
                'video_view_count': post.video_view_count if post.is_video else None,
                'image_urls': image_urls if not post.is_video else None,
                'reel_urls': reel_urls if post.is_video else None
            })

        # Convert data to DataFrame and save as CSV
        df = pd.DataFrame(data)
        temp_csv_file = f"{csv_file_path}/{account_name}_scraped_results.csv"
        df.to_csv(temp_csv_file, index=False)
        print(f"Data saved to {temp_csv_file}")

    except Exception as e:
        print('Error fetching accounts: ' + account_name, e)
        raise

# %%
# Example usage
scrape_account(account_name, os.getenv('IG_USERNAME'), os.getenv('IG_PASSWORD'), login_required=False)


