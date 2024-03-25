import streamlit as st
import pandas as pd
import mysql.connector
from googleapiclient.discovery import build

# Function to connect to MySQL database
def connect_to_db():
    conn = mysql.connector.connect(
        host="sql6.freesqldatabase.com",
        user="sql6694206",
        password="bCtXmq6LUs",
        database="sql6694206"
    )
    return conn

# Function to create tables in MySQL
def create_tables():
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INT AUTO_INCREMENT PRIMARY KEY,
            channel_id VARCHAR(255) UNIQUE,
            channel_name VARCHAR(255),
            subscription_count INT,
            channel_views INT,
            channel_description TEXT,
            playlist_id VARCHAR(255)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id VARCHAR(255) UNIQUE,
            channel_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT,
            tags TEXT,
            published_at DATETIME,
            view_count INT,
            like_count INT,
            dislike_count INT,
            favorite_count INT,
            comment_count INT,
            duration TIME,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(50)
        )
    ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS comments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(255),
    comment_id VARCHAR(255),
    comment_text TEXT,
    comment_author VARCHAR(255),
    comment_published_at DATETIME,
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
)

    ''')

    conn.commit()
    conn.close()

# Function to insert data into MySQL database
def insert_into_db(channel_data, video_data):
    conn = connect_to_db()
    cur = conn.cursor()

    # Insert channel data
    cur.execute('''
        INSERT IGNORE INTO channels (channel_id, channel_name, subscription_count, channel_views, channel_description, playlist_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (
        channel_data['Channel_Id'],
        channel_data['Channel_Name'],
        channel_data['Subscription_Count'],
        channel_data['Channel_Views'],
        channel_data['Channel_Description'],
        channel_data['Playlist_Id']
    ))

    # Insert video data
    for video_id, video_info in video_data.items():
        cur.execute('''
            INSERT IGNORE INTO videos (video_id, channel_id, video_name, video_description, tags, published_at, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            video_id,
            channel_data['Channel_Id'],
            video_info['Video_Name'],
            video_info['Video_Description'],
            ','.join(video_info['Tags']),
            video_info['PublishedAt'],
            video_info['View_Count'],
            video_info['Like_Count'],
            video_info['Dislike_Count'],
            video_info['Favorite_Count'],
            video_info['Comment_Count'],
            video_info['Duration'],
            video_info['Thumbnail'],
            video_info['Caption_Status']
        ))

        # Insert comments for the video
        comments = video_info.get('Comments', {})  # Check if 'Comments' key exists
        for comment_id, comment_info in comments.items():
            cur.execute('''
                INSERT IGNORE INTO comments (video_id, comment_id, comment_text, comment_author, comment_published_at)
                VALUES (%s, %s, %s, %s, %s)
            ''', (
                video_id,
                comment_id,
                comment_info['Comment_Text'],
                comment_info['Comment_Author'],
                comment_info['Comment_PublishedAt']
            ))

    conn.commit()
    conn.close()





# Function to fetch data from MySQL database
def fetch_from_db(channel_id):
    conn = connect_to_db()
    cur = conn.cursor(dictionary=True)

    # Fetch channel data
    cur.execute('''
        SELECT * FROM channels WHERE channel_id = %s
    ''', (channel_id,))
    channel_data = cur.fetchone()

    # Fetch video data
    cur.execute('''
        SELECT * FROM videos WHERE channel_id = %s
    ''', (channel_id,))
    videos = cur.fetchall()

    video_data = {}
    for video in videos:
        video_id = video['video_id']

        # Fetch comments for each video
        cur.execute('''
            SELECT * FROM comments WHERE video_id = %s
        ''', (video_id,))
        comments = cur.fetchall()

        comment_data = {}
        for comment in comments:
            comment_data[comment['comment_id']] = {
                'Comment_Id': comment['comment_id'],
                'Comment_Text': comment['comment_text'],
                'Comment_Author': comment['comment_author'],
                'Comment_PublishedAt': comment['comment_published_at']
            }

        # Calculate total comment count
        total_comments = len(comments)

        video_data[video_id] = {
            'Video_Id': video['video_id'],
            'Video_Name': video['video_name'],
            'Video_Description': video['video_description'],
            'Tags': video['tags'].split(','),
            'PublishedAt': video['published_at'],
            'View_Count': video['view_count'],
            'Like_Count': video['like_count'],
            'Dislike_Count': video['dislike_count'],
            'Favorite_Count': video['favorite_count'],
            'Comment_Count': total_comments,
            'Duration': video['duration'],
            'Thumbnail': video['thumbnail'],
            'Caption_Status': video['caption_status'],
            'Comments': comment_data
        }

    conn.close()
    return channel_data, video_data


def fetch_youtube_data(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    channel_response = youtube.channels().list(part='snippet,statistics,contentDetails', id=channel_id).execute()
    video_response = youtube.playlistItems().list(part='snippet', playlistId=channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'], maxResults=10).execute()

    channel_data = {
        'Channel_Id': channel_id,
        'Channel_Name': channel_response['items'][0]['snippet']['title'],
        'Subscription_Count': int(channel_response['items'][0]['statistics']['subscriberCount']),
        'Channel_Views': int(channel_response['items'][0]['statistics']['viewCount']), # Updated to 'viewCount'
        'Channel_Description': channel_response['items'][0]['snippet']['description'], # Updated to 'description'
        'Playlist_Id': channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }

    video_data = {}
    for item in video_response['items']:
        snippet = item.get('snippet', {})
        statistics = snippet.get('statistics', {})
        
        video_id = snippet.get('resourceId', {}).get('videoId', '')
        likes = int(statistics.get('likeCount', 0))
        dislikes = int(statistics.get('dislikeCount', 0))
        comments = int(statistics.get('commentCount', 0))

        # Extract comments for the video
        comments_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            textFormat="plainText"
        ).execute()

        comment_data = {}
        for comment in comments_response['items']:
            comment_snippet = comment['snippet']['topLevelComment']['snippet']
            comment_id = comment['id']
            comment_text = comment_snippet['textDisplay']
            comment_author = comment_snippet['authorDisplayName']
            comment_published_at = comment_snippet['publishedAt']
            
            comment_data[comment_id] = {
                'Comment_Text': comment_text,
                'Comment_Author': comment_author,
                'Comment_PublishedAt': comment_published_at
            }

        video_data[video_id] = {
            'Video_Name': snippet.get('title', ''),
            'Video_Description': snippet.get('description', ''),
            'Tags': snippet.get('tags', []),
            'PublishedAt': snippet.get('publishedAt', ''),
            'View_Count': int(statistics.get('viewCount', 0)), # Update view count retrieval
            'Like_Count': likes,
            'Dislike_Count': dislikes,
            'Favorite_Count': int(statistics.get('favoriteCount', 0)),
            'Comment_Count': comments,
            'Duration': snippet.get('duration', ''),
            'Thumbnail': snippet.get('thumbnails', {}).get('default', {}).get('url', ''),
            'Caption_Status': snippet.get('localized', {}).get('defaultAudioLanguage', ''),
            'Comments': comment_data  # Add comment data to video data
        }

    return channel_data, video_data


# Function to display data in Streamlit app
def display_data(channel_data, video_data):
    if not channel_data:
        st.write('No data available for the provided channel ID.')
    else:
        st.subheader('Channel Information')
        st.write(pd.DataFrame([channel_data], columns=channel_data.keys()))

        st.subheader('Video Information')
        video_df = pd.DataFrame(video_data.values(), columns=video_data[next(iter(video_data))].keys())
        st.write(video_df)

        st.subheader('All Comments')
        all_comments = []
        for video_id, video_info in video_data.items():
            comments = video_info['Comments']
            for comment_info in comments.values():
                all_comments.append({
                    'Video_ID': video_id,
                    'Comment_ID': comment_info['Comment_Id'],
                    'Comment_Text': comment_info['Comment_Text'],
                    'Comment_Author': comment_info['Comment_Author'],
                    'Comment_PublishedAt': comment_info['Comment_PublishedAt']
                })

        if all_comments:
            comments_df = pd.DataFrame(all_comments)
            st.write(comments_df)
        else:
            st.write("No comments available for any video.")


# Defining functions to execute SQL queries and return results as pandas DataFrames
def execute_query(query):
    conn = connect_to_db()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Main Streamlit app
def main():
    st.title('YouTube Data Harvesting and Warehousing')
    st.sidebar.header('Input YouTube Channel ID')

    channel_id = st.sidebar.text_input('Enter Channel ID')

    if st.sidebar.button('Fetch Data'):
        api_key = 'AIzaSyCxG8VPxu5t_FhKoZfF_g3KqiV35z9Bq0U'  # Replace with your YouTube API key
        channel_data, video_data = fetch_youtube_data(api_key, channel_id)
        create_tables()
        insert_into_db(channel_data, video_data)
        st.success('Data fetched and stored successfully.')

    if st.sidebar.button('Retrieve Data'):
        channel_data, video_data = fetch_from_db(channel_id)
        display_data(channel_data, video_data)
    
    # Add buttons to display query results
    st.sidebar.subheader('SQL Query Outputs')
    
    if st.sidebar.button('Names of all videos and their corresponding channels'):
        query1 = "SELECT v.video_name, c.channel_name FROM videos v JOIN channels c ON v.channel_id = c.channel_id;"
        df1 = execute_query(query1)
        st.write(df1)

    if st.sidebar.button('Channels with the most number of videos and their count'):
        query2 = "SELECT c.channel_name, COUNT(*) AS video_count FROM channels c JOIN videos v ON c.channel_id = v.channel_id GROUP BY c.channel_name ORDER BY video_count DESC LIMIT 1;"
        df2 = execute_query(query2)
        st.write(df2)

    if st.sidebar.button('Top 10 most viewed videos and their respective channels'):
        query3 = "SELECT v.video_name, c.channel_name, v.view_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.view_count DESC LIMIT 10;"
        df3 = execute_query(query3)
        st.write(df3)

    if st.sidebar.button('Number of comments on each video and their corresponding video names'):
        query4 = "SELECT v.video_name, COUNT(*) AS comment_count FROM videos v JOIN comments c ON v.video_id = c.video_id GROUP BY v.video_name;"
        df4 = execute_query(query4)
        st.write(df4)

    if st.sidebar.button('Videos with the highest number of likes and their corresponding channel names'):
        query5 = "SELECT v.video_name, c.channel_name, v.like_count FROM videos v JOIN channels c ON v.channel_id = c.channel_id ORDER BY v.like_count DESC LIMIT 1;"
        df5 = execute_query(query5)
        st.write(df5)

    if st.sidebar.button('Total number of likes and dislikes for each video and their corresponding video names'):
        query6 = "SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes FROM videos v GROUP BY v.video_name;"
        df6 = execute_query(query6)
        st.write(df6)

    if st.sidebar.button('Total number of views for each channel and their corresponding channel names'):
        query7 = "SELECT c.channel_name, SUM(v.view_count) AS total_views FROM channels c JOIN videos v ON c.channel_id = v.channel_id GROUP BY c.channel_name;"
        df7 = execute_query(query7)
        st.write(df7)

    if st.sidebar.button('Names of channels that published videos in 2022'):
        query8 = "SELECT DISTINCT c.channel_name FROM channels c JOIN videos v ON c.channel_id = v.channel_id WHERE YEAR(v.published_at) = 2022;"
        df8 = execute_query(query8)
        st.write(df8)

    if st.sidebar.button('Average duration of all videos in each channel and their corresponding channel names'):
        query9 = "SELECT c.channel_name, AVG(TIME_TO_SEC(v.duration)) AS avg_duration_seconds FROM videos v JOIN channels c ON v.channel_id = c.channel_id GROUP BY c.channel_name;"
        df9 = execute_query(query9)
        st.write(df9)

    if st.sidebar.button('Videos with the highest number of comments and their corresponding channel names'):
        query10 = '''SELECT v.video_name, ch.channel_name, COUNT(*) AS comment_count 
                    FROM videos v 
                    JOIN comments c ON v.video_id = c.video_id 
                    JOIN channels ch ON v.channel_id = ch.channel_id 
                    GROUP BY v.video_name, ch.channel_name
                    ORDER BY comment_count DESC
                    LIMIT 1;

                    '''
        df10 = execute_query(query10)
        st.write(df10)

if __name__ == '__main__':
    main()
