import requests
import snowflake.connector

# Function to establish Snowflake connection
def connect_to_snowflake():
    try:
        snowflake_account = "du18788.ap-southeast-1"
        snowflake_user = "YAMINIPATIL"
        snowflake_password = "Yamini@10"
        snowflake_database = "YoutubeAnalytics"
        snowflake_schema = "PUBLIC"
        
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema
        )
        return conn
    except Exception as e:
        print("Error connecting to Snowflake:", e)
        return None

# Function to create the table in Snowflake
def create_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_channel_stats (
            channelId STRING,
            channelTitle STRING,
            videoId STRING,
            PRIMARY KEY (videoId)
        )
        """)
        cursor.close()
        print("Table 'youtube_channel_stats' created successfully.")
    except Exception as e:
        print("Error creating table in Snowflake:", e)

# Function to fetch data from YouTube API
def fetch_youtube_data(api_url, api_key, channel_id):
    try:
        params = {
            "channelId": channel_id,
            "key": api_key,
            "part": "snippet,id",
            "order": "date",
            "maxResults": 100
        }
        response = requests.get(api_url, params=params)
        data = response.json()
        return data
    except Exception as e:
        print("Error fetching YouTube data:", e)
        return None

# Function to print and store data in Snowflake
def process_data(conn, data):
    try:
        cursor = conn.cursor()
        for item in data.get('items', []):
            channelId = item['snippet']['channelId']
            channelTitle = item['snippet']['channelTitle']
            videoId = item['id']['videoId']
            cursor.execute("SELECT COUNT(*) FROM youtube_channel_stats WHERE videoId = %s", (videoId,))
            result = cursor.fetchone()[0]
            if result == 0:
                cursor.execute("INSERT INTO youtube_channel_stats (channelId, channelTitle, videoId) VALUES (%s, %s, %s)", (channelId, channelTitle, videoId))
        conn.commit()
        cursor.close()
    except Exception as e:
        print("Error processing data:", e)

# Main function to execute the process
def process_youtube_channel_stats():
    api_url = "https://www.googleapis.com/youtube/v3/search"
    api_key = "AIzaSyCbdDN_qsxGKQNwuGc9gJ0-aMNqzzsuTDA"
    channel_id = "UCbr9s1iYnD4SRszBxzEPELg"
    conn = connect_to_snowflake()
    if conn:
        create_table(conn)
        data = fetch_youtube_data(api_url, api_key, channel_id)
        if data:
            process_data(conn, data)
        conn.close()

# if __name__ == "__main__":
#     process_youtube_channel_stats()

"""
above is the fetch channel id and title 
"""
def create_table_if_not_exists(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_video_stats(
                VideoId STRING,
                viewCount int,
                likeCount int,
                favoriteCount int,
                commentCount int
            )
        """)
        print("Table 'youtube_video_stats' created successfully.")
        cursor.close()  # Close the cursor after executing the query
    except Exception as e:
        print("Error creating table in Snowflake:", e)

def get_video_ids(channel_id, api_key):
    video_ids = []

    try:
        endpoint = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=id&maxResults=50"
        response = requests.get(endpoint)
        data = response.json()

        for item in data['items']:
            if item['id']['kind'] == 'youtube#video':
                video_ids.append(item['id']['videoId'])
#By checking if the kind attribute of an item is youtube#video, the code ensures that only video items are included in the list of video IDs returned by the function
        return video_ids

    except Exception as e:
        print("Error fetching video IDs:", e)
        return []
    
def get_video_statistics(video_id, api_key):
    try:
        endpoint = f"https://www.googleapis.com/youtube/v3/videos?id={video_id}&key={api_key}&part=statistics"
        response = requests.get(endpoint)
        data = response.json()

        if 'items' in data and len(data['items']) > 0:
            return data['items'][0]['statistics']
        else:
            return None

    except Exception as e:
        print("Error fetching statistics for video ID", video_id, ":", e)
        return None
# Function to insert statistics into Snowflake
def insert_statistics(cursor, video_id, statistics):
    try:
        # Check if the video ID already exists in the table
        cursor.execute("""
            SELECT COUNT(*) FROM youtube_video_stats WHERE VideoId = %s
        """, (video_id,))
        result = cursor.fetchone()[0]
        
        # If video ID does not exist, insert the statistics
        if result == 0:
            view_count = int(statistics.get('viewCount', 0))
            like_count = int(statistics.get('likeCount', 0))
            favorite_count = int(statistics.get('favoriteCount', 0))
            comment_count = int(statistics.get('commentCount', 0))

            cursor.execute("""
                INSERT INTO youtube_video_stats (VideoId, viewCount, likeCount, favoriteCount, commentCount) 
                VALUES (%s, %s, %s, %s, %s)
            """, (video_id, view_count, like_count, favorite_count, comment_count))
        else:
            print(f"video ID {video_id} already exist in the table.Skipping insertion.")
    except Exception as e:
        print("Error inserting statistics into Snowflake:", e)

def process_youtube_video_stats():
    api_key = "AIzaSyCbdDN_qsxGKQNwuGc9gJ0-aMNqzzsuTDA"
    channel_id = "UCbr9s1iYnD4SRszBxzEPELg"

    conn = connect_to_snowflake()
    if conn:
        create_table_if_not_exists(conn)
        cursor = conn.cursor()  # Get cursor after table creation
        video_ids = get_video_ids(channel_id, api_key)
        if video_ids:
            for video_id in video_ids:
                statistics = get_video_statistics(video_id, api_key)
                if statistics:
                    insert_statistics(cursor, video_id, statistics)
            cursor.close()  # Close the cursor after use
            conn.commit()
            conn.close()
  
    """
    above code is gives the videoId and theri like comments and views
    """

# # Function to create the table if it doesn't exist
def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_video_comments (
                video_id VARCHAR,
                comment_text VARCHAR,
                UNIQUE (video_id, comment_text)
            )
        """)
        print("Table 'youtube_video_comments' created successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print("Error:", e)
    finally:
        cursor.close()
   

# # Function to fetch video IDs for a given channel
def fetch_video_ids(api_key, channel_id):
    video_ids = []
    video_endpoint = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=snippet,id&maxResults=50"
    response = requests.get(video_endpoint)
    if response.status_code == 200:
        data = response.json()
        for item in data.get("items", []):
            if item["id"]["kind"] == "youtube#video":
                video_ids.append(item["id"]["videoId"])
    else:
        print("Failed to fetch video IDs")
    return video_ids

# # Function to extract and store comments for each video
def extract_comments(api_key, video_ids, conn):
    comment_endpoint = "https://www.googleapis.com/youtube/v3/commentThreads"
    for video_id in video_ids:
        params = {
            "key": api_key,
            "textFormat": "plainText",
            "part": "snippet",
            "videoId": video_id,
            "maxResults": 100
        }
        response = requests.get(comment_endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
            for item in data.get("items", []):
                comment_text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
                # Store in Snowflake table if not already present
                if not is_comment_exist(video_id, comment_text, conn):
                    store_in_snowflake(video_id, comment_text, conn)
        else:
            print(f"Failed to fetch comments for video ID: {video_id}")

def is_comment_exist(video_id, comment_text, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM youtube_video_comments WHERE video_id = %s AND comment_text = %s", (video_id, comment_text))
        count = cursor.fetchone()[0]
        return count > 0
    except snowflake.connector.errors.ProgrammingError as e:
        print("Error:", e)
    finally:
        cursor.close()


# # Function to store comments in Snowflake
def store_in_snowflake(video_id, comment_text, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO youtube_video_comments (video_id, comment_text) VALUES (%s, %s)", (video_id, comment_text))
        conn.commit()  # Commit the transaction
    except snowflake.connector.errors.ProgrammingError as e:
        print("Error:", e)
    finally:
        cursor.close()

# # Main function to process YouTube comments stats
def process_youtube_comments_stats():
    api_key = "AIzaSyCbdDN_qsxGKQNwuGc9gJ0-aMNqzzsuTDA"
    channel_id = "UCbr9s1iYnD4SRszBxzEPELg"
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if conn:
        # Create table if not exists
        create_table_if_not_exists(conn)
        # Fetch video IDs
        video_ids = fetch_video_ids(api_key, channel_id)
        # Extract and store comments
        extract_comments(api_key, video_ids, conn)
        # Close Snowflake connection
        conn.close()

if __name__ == "__main__":
    process_youtube_channel_stats()
    process_youtube_video_stats()
    process_youtube_comments_stats()
   
   