import requests
import snowflake.connector

# YouTube API endpoint
api_key = "AIzaSyCbdDN_qsxGKQNwuGc9gJ0-aMNqzzsuTDA"
channel_id = "UCbr9s1iYnD4SRszBxzEPELg"

# Snowflake connection parameters
snowflake_account = "du18788.ap-southeast-1"
snowflake_user = "YAMINIPATIL"
snowflake_password = "Yamini@10"
snowflake_database = "YoutubeData"
snowflake_schema = "PUBLIC"

# Function to fetch all video IDs from a given channel
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

# Function to fetch statistics for a video
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


# Function to establish connection to Snowflake
def connect_to_snowflake():
    try:
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


# Function to create table in Snowflake if not exists
def create_table_if_not_exists(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_video_stats(
                VideoId STRING,
                viewCount int,
                likeCount int,
                favoriteCount int,
                commentCount int
            )
        """)
    except Exception as e:
        print("Error creating table in Snowflake:", e)


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


# Main function to fetch video statistics and store in Snowflake
def main():
    try:
        # Fetch video IDs from the channel
        video_ids = get_video_ids(channel_id, api_key)

        # Establish connection to Snowflake
        conn = connect_to_snowflake()
        if conn is None:
            return
        # Create a cursor object
        cur = conn.cursor()

        # Create table in Snowflake if not exists
        create_table_if_not_exists(cur)

        # Fetch statistics for each video and store in Snowflake
        for video_id in video_ids:
            statistics = get_video_statistics(video_id, api_key)
            if statistics:
                insert_statistics(cur, video_id, statistics)

        # Commit the transaction
        conn.commit()

        # Close cursor and connection
        cur.close()
        conn.close()

    except Exception as e:
        print("An error occurred:", e)
        
# Run the main function
if __name__ == "__main__":
    main()
