import requests
import snowflake.connector

# YouTube API endpoint to fetch video IDs
api_key = "AIzaSyCbdDN_qsxGKQNwuGc9gJ0-aMNqzzsuTDA"
channel_id = "UCbr9s1iYnD4SRszBxzEPELg"
video_endpoint = f"https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=snippet,id&maxResults=50"

# YouTube API endpoint to fetch comments for a video
comment_endpoint = "https://www.googleapis.com/youtube/v3/commentThreads"

# Snowflake connection details
snowflake_account = "du18788.ap-southeast-1"
snowflake_user = "YAMINIPATIL"
snowflake_password = "Yamini@10"
snowflake_database = "YoutubeData"
snowflake_schema = "PUBLIC"  

# Function to connect to Snowflake
def connect_to_snowflake():
    return snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema
    )

# Function to create the table if it doesn't exist
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

# Function to fetch video IDs for a given channel
def fetch_video_ids():
    video_ids = []
    response = requests.get(video_endpoint)
    if response.status_code == 200:
        data = response.json()
        for item in data.get("items", []):
            if item["id"]["kind"] == "youtube#video":
                video_ids.append(item["id"]["videoId"])
    else:
        print("Failed to fetch video IDs")
    return video_ids

# Function to extract and store comments for each video
def extract_comments(video_ids, conn):
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

# Function to check if a comment already exists in Snowflake
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
        
# Function to store comments in Snowflake
def store_in_snowflake(video_id, comment_text, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO youtube_video_comments (video_id, comment_text) VALUES (%s, %s)", (video_id, comment_text))
        conn.commit()  # Commit the transaction
    except snowflake.connector.errors.ProgrammingError as e:
        print("Error:", e)
    finally:
        cursor.close()

# Main function
def main():
    # Connect to Snowflake
    conn = connect_to_snowflake()
    print(conn, "connection successfully")

    # Create table if not exists
    create_table_if_not_exists(conn)

    # Fetch video IDs
    video_ids = fetch_video_ids()

    # Extract and store comments
    extract_comments(video_ids, conn)

    # Close Snowflake connection
    conn.close()

# Execute the main function
if __name__ == "__main__":
    main()
