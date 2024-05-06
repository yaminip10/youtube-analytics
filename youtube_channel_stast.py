import requests
import snowflake.connector

# YouTube API endpoint
api_url = "https://www.googleapis.com/youtube/v3/search"
api_key = "AIzaSyCbdDN_qsxGKQNwuGc9gJ0-aMNqzzsuTDA"
channel_id = "UCbr9s1iYnD4SRszBxzEPELg"

# Snowflake connection parameters
snowflake_account = "du18788.ap-southeast-1"
snowflake_user = "YAMINIPATIL"
snowflake_password = "Yamini@10"
snowflake_database = "YoutubeData"
snowflake_schema = "PUBLIC"

# Establish Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema
)

# Function to create the table in Snowflake
def create_table(conn):
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

# Function to send request to YouTube API and extract data
def fetch_youtube_data():
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

# Function to print and store data in Snowflake
def process_data(data):
    cursor = conn.cursor()
    for item in data['items']:
        channelId = item['snippet']['channelId']
        channelTitle = item['snippet']['channelTitle']
        videoId = item['id']['videoId']

        # print("Channel ID:", channelId)
        # print("Channel Title:", channelTitle)
        # print("Video ID:", videoId)
        # print("--------------------")
        
        # Check if videoId already exists in the table
        cursor.execute("SELECT COUNT(*) FROM youtube_channel_stats WHERE videoId = %s", (videoId,))
        result = cursor.fetchone()[0]
        
        # If videoId does not exist, insert into Snowflake
        if result == 0:
            cursor.execute("INSERT INTO youtube_channel_stats (channelId, channelTitle, videoId) VALUES (%s, %s, %s)", (channelId, channelTitle, videoId))
    
    cursor.close()

# Main function to execute the process
def main():
    create_table(conn)
    data = fetch_youtube_data()
    process_data(data)
    conn.close()

if __name__ == "__main__":
    main()
