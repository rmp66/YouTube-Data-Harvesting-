import mysql.connector
import pymongo
from pymongo import MongoClient
import json
import re
from datetime import datetime
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd


# Remove non-ascii characters from Youtube Data Harvest
def remove_non_ascii(text):
    if isinstance(text, str):
        return re.sub(r'[^\x00-\x7f]', '', text)
    return text


# You tube data harvestiing
api_key = 'AIzaSyA7nX0_rpeuPcmEuJnXsr76nzudF1UUcaU'
from googleapiclient.discovery import build

main_data = []


def channels(channel_ids):
    for channel_id in channel_ids:
        api_key = 'AIzaSyA7nX0_rpeuPcmEuJnXsr76nzudF1UUcaU'
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics,status,topicDetails",
            id=channel_id
        )
        channel_response = request.execute()
        channel_data = {
            'channel_id': channel_response['items'][0]['id'],
            'channel_name': remove_non_ascii(channel_response['items'][0]['snippet']['title']),
            'channel_type': channel_response['items'][0]['topicDetails']['topicCategories'][0],
            # 'subscription_count': channel_response['items'][0]['statistics']['subscriberCount'],
            'channel_views': channel_response['items'][0]['statistics']['viewCount'],
            'channel_description': remove_non_ascii(channel_response['items'][0]['snippet']['description']),
            'channel_status': channel_response['items'][0]['status']['privacyStatus'],
            'playlist_id': channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
        main_data.append(channel_data)
        # Get play list data:
        # Get two video IDs from given Channel ID:
        uploads_playlist_id = channel_data['playlist_id']
        # Channels list contentdetails has Playlist uploads.
        # This is The ID of the playlist that contains the channel's uploaded videos.

        playlist_response = youtube.playlistItems().list(
            part='snippet',
            playlistId=uploads_playlist_id,
            maxResults=2  # change to get more video_ids and related comments
        ).execute()
        # PlaylistItems snippet resourceId has the related videoIds
        # PlayList can not be filtered by channel id
        playlist_data = {
            'playlist_id': playlist_response['items'][0]['id'],
            'channel_id': playlist_response['items'][0]['snippet']['channelId'],
            'playlist_name': remove_non_ascii(playlist_response['items'][0]['snippet']['title']),
        }

        main_data.append(playlist_data)

        video_ids = [item['snippet']['resourceId']['videoId'] for item in playlist_response['items']]

        # Video data harvesting code - for each video id above get first data:

        for video_id in video_ids:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            video_response = request.execute()
            video_data = {
                'video_id': video_response['items'][0]['id'],
                'playlist_id': playlist_response['items'][0]['id'],
                'video_name': remove_non_ascii(video_response['items'][0]['snippet']['title']),
                'video_description': remove_non_ascii(video_response['items'][0]['snippet']['description']),
                # 'tags': video_response['items'][0]['etag'],
                'published_at': video_response['items'][0]['snippet']['publishedAt'],
                'view_count': video_response['items'][0]['statistics']['viewCount'],
                'like_count': video_response['items'][0]['statistics']['likeCount'],
                'favorite_count': video_response['items'][0]['statistics']['favoriteCount'],
                'comment_count': video_response['items'][0]['statistics']['favoriteCount'],
                'duration': video_response['items'][0]['contentDetails']['duration'],
                'thumbnail': video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                'caption_status': video_response['items'][0]['contentDetails']['caption']
            }

            # main_data.append(json_video_data)
            main_data.append(video_data)

            # Comment data harvesting code:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=2  # change to get more comments
            )
            comment_response = request.execute()
            # comments_data = []
            for item in comment_response['items']:
                comment_data = {
                    'comment_id': item['id'],
                    'video_id': item['snippet']['videoId'],
                    # 'comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_text': remove_non_ascii(item['snippet']['topLevelComment']['snippet']['textDisplay']),
                    'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_publishedat': item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                # json_comment_data = json.dumps(comment_data)
                # main_data.append(json_comment_data)
                main_data.append(comment_data)

    # print(main_data)

    # Move harvested data to MongoDB data lake
    client = MongoClient('localhost', 27017)
    db = client['youtube_harvest']
    collection = db.harvested_data
    # db.harvested_data.insert_many(main_data)
    # db.harvested_data.delete_many({})
    #     for document in collection.find():
    #         print(document)
    # print(main_data)
    #     for document in collection.find({}, {"_id": 1, "comment_id": 1, "video_id": 1, "comment_text": 1, "comment_author": 1, "comment_publishedat": 1}):
    #         print(document)

    # Connect to mysql dB
    cnx = mysql.connector.connect(
        user='root',
        password='Password',
        host='127.0.0.1',
        database='harvested_data'
    )
    cursor = cnx.cursor()

    # channels fn needs to be called for execution
    #    Create Channel Table
    channel_table = """
    CREATE TABLE IF NOT EXISTS channel (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_type VARCHAR(255),
        channel_views INT,
        channel_description TEXT,
        channel_status VARCHAR(255)
    )
    """
    # cursor.execute(channel_table)
    channel_insert_tosql = """
    INSERT IGNORE INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    for document in collection.find():
        if 'channel_id' in document and 'channel_name' in document:
            channel_id = document['channel_id']
            channel_name = document['channel_name']
            channel_type = document['channel_type']
            channel_views = int(document['channel_views'])
            channel_description = document['channel_description']
            channel_status = document['channel_status']

            cursor.execute(channel_insert_tosql, (channel_id, channel_name, channel_type, channel_views,
                                                  channel_description, channel_status))
        else:
            continue

        # print(channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
    # cnx.commit()     # Commit changes
    # Create Playlist Table
    playlist_table = """
    CREATE TABLE IF NOT EXISTS playlist (
        playlist_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        playlist_name VARCHAR(255),
        FOREIGN KEY(channel_id) REFERENCES channel(channel_id)
    )
    """
    # cursor.execute(playlist_table)
    playlist_insert_tosql = """
    INSERT IGNORE INTO playlist (playlist_id, channel_id, playlist_name)
    VALUES (%s, %s, %s)
    """

    for document in collection.find():
        if 'playlist_id' in document and 'channel_id' in document and 'playlist_name' in document:  # there are other docs with both ids. So checking all items
            playlist_id = document['playlist_id']
            channel_id = document['channel_id']
            playlist_name = document['playlist_name']
            cursor.execute(playlist_insert_tosql, (playlist_id, channel_id, playlist_name))
        else:
            continue
        # print(playlist_id, channel_id, playlist_name)

    # cnx.commit()     # Commit changes

    # Create Video Table
    video_table = """
    CREATE TABLE IF NOT EXISTS video (
        video_id VARCHAR(255) PRIMARY KEY,
        playlist_id VARCHAR(255),
        video_name VARCHAR(255),
        video_description TEXT,
        published_date DATETIME, #Check format
        view_count INT,
        like_count INT,
        favorite_count INT,
        comment_count INT,
        duration INT,
        thumbnail VARCHAR(255),
        caption_status VARCHAR(255),
        FOREIGN KEY(playlist_id) REFERENCES playlist(playlist_id)
    )
    """
    # cursor.execute(video_table)
    video_insert_tosql = """
    INSERT IGNORE INTO video (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail, caption_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for document in collection.find():
        if 'playlist_id' in document and 'video_id' in document and 'published_at' in document:
            video_id = document['video_id']
            playlist_id = document['playlist_id']
            video_name = document['video_name']
            video_description = document['video_description']
            published_date = document['published_at']  # data label in document is published_at
            view_count = document['view_count']
            like_count = document['like_count']
            favorite_count = document['favorite_count']
            comment_count = document['comment_count']
            duration = document['duration']
            thumbnail = document['thumbnail']
            caption_status = document['caption_status']

            cursor.execute(video_insert_tosql, (
            video_id, playlist_id, video_name, video_description, published_date, view_count, like_count,
            favorite_count, comment_count, duration, thumbnail, caption_status))
        else:
            continue
        # print(video_id, playlist_id, video_name, video_description, published_date, view_count, favorite_count, comment_count, duration, thumbnail, caption_status)

    # cnx.commit()     # Commit changes
    # Create Comment Table
    comment_table = """
    CREATE TABLE IF NOT EXISTS comment (
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_publishedat DATETIME,
        FOREIGN KEY(video_id) REFERENCES video(video_id)
    )
    """
    cursor.execute(comment_table)
    comment_insert_tosql = """
    INSERT IGNORE INTO comment (comment_id, video_id, comment_text, comment_author, comment_publishedat)
    VALUES (%s, %s, %s, %s, %s)
    """
    for document in collection.find():
        if 'comment_id' in document and 'comment_publishedat' in document:
            comment_id = document['comment_id']
            video_id = document['video_id']
            comment_text = document['comment_text']
            comment_author = document['comment_author']
            comment_publishedat = document['comment_publishedat']
            # print(comment_id, video_id, comment_text, comment_author, comment_publishedat)
            cursor.execute(comment_insert_tosql,
                           (comment_id, video_id, comment_text, comment_author, comment_publishedat))
        else:
            continue

        print(comment_id, video_id, comment_text, comment_author, comment_publishedat)

        cnx.commit()     # Commit changes

    # Close the mysql connection
    cursor.close()
    cnx.close()


# Streamlit code:
# Display harvested data:
#Connect to mysql:
cnx = mysql.connector.connect(
        user='root',
        password='Password',
        host='127.0.0.1',
        database='harvested_data'
)
cursor = cnx.cursor()
# Function to run queries:
def streamlit_display(title, query, channel_id):
    subheader_text = title + " for " + channel_id
    st.subheader(subheader_text)
    #cursor.execute(query)
    cursor.execute(query, (channel_id,))
    result = cursor.fetchall()

    if result:
        #st.write("Data Fetched Successfully:")
        for row in result:
            st.write(row)  # Display each row
    else:
        st.write("No data found.")

st.title("YouTube Harvested Data Display - Palaniappan Kannan")
channel_ids_input = st.text_input("Enter Channel IDs (comma-separated without any space or quote):", "")

if channel_ids_input:
    channel_ids = channel_ids_input.split(',')  # Splitting the input to get individual channel IDs

    # for channel_id in channel_ids:
    #     channel_id = channel_id.strip()  # Removing any leading/trailing whitespace

    # Queries for each sql table based on channel_id
    for channel_id in channel_ids:
        channel_query = "SELECT * FROM channel WHERE channel_id = %s"
        playlist_query = "SELECT * FROM playlist WHERE channel_id = %s"
        video_query = """SELECT video.* FROM video
                         INNER JOIN playlist ON video.playlist_id = playlist.playlist_id
                         WHERE playlist.channel_id = %s"""
        comment_query = """SELECT comment.* FROM comment
                           INNER JOIN video ON comment.video_id = video.video_id
                           INNER JOIN playlist ON video.playlist_id = playlist.playlist_id
                           WHERE playlist.channel_id = %s"""

        # Displaying data for each table for the current channel_id
        streamlit_display("Channel Data", channel_query, channel_id)
        streamlit_display("Playlist Data", playlist_query, channel_id)
        streamlit_display("Video Data(2)", video_query, channel_id)
        streamlit_display("Comment Data(4)",comment_query, channel_id)
#
st.subheader(":red[Select your query to get data]")
st.text("Limited data fetched")
query = st.selectbox('Query',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2.	Which channels have the most number of videos, and how many videos do they have?',
    '3.	What are the top 10 most viewed videos and their respective channels?',
    '4.	How many comments were made on each video, and what are their corresponding video names?',
    '5.	Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7.	What is the total number of views for each channel, and what are their corresponding channel names?',
    '8.	What are the names of all the channels that have published videos in the year 2022?',
    '9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

output = None
if query == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute("""SELECT channel_name AS CHANNEL_NAME, video_name AS VIDEO_NAME FROM channel
                  JOIN playlist ON channel.channel_id = playlist.channel_id
                  JOIN video ON playlist.playlist_id = video.playlist_id""")
    # df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    # st.write(df)
    output = cursor.fetchall()
elif query == '2.	Which channels have the most number of videos, and how many videos do they have?':
    cursor.execute("""SELECT channel_name AS Channel_Name, count(video.video_id) AS Number_of_Videos FROM channel
                  JOIN playlist ON channel.channel_id = playlist.channel_id
                  JOIN video ON playlist.playlist_id = video.playlist_id
                  GROUP BY channel.channel_name
                  ORDER BY Number_of_Videos DESC""")
    output = cursor.fetchall()
    # df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    # st.write(df)
    #st.dataframe(df, height=750)
    #'3.	What are the top 10 most viewed videos and their respective channels?'
elif query == '3.	What are the top 10 most viewed videos and their respective channels?':
    cursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Name, view_count AS View_Count FROM channel
                  JOIN playlist ON channel.channel_id = playlist.channel_id
                  JOIN video ON playlist.playlist_id = video.playlist_id
                  ORDER BY view_count DESC LIMIT 10""")
    output = cursor.fetchall()
    # df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    # st.write(df)
elif query == '4.	How many comments were made on each video, and what are their corresponding video names?':
    cursor.execute("""SELECT video_name AS Video_Name, count(comment.comment_id) AS Number_Of_Comments FROM video
                   JOIN comment ON video.video_id = comment.video_id 
                   Group BY video.video_name""")
    output = cursor.fetchall()
elif query == '5.	Which videos have the highest number of likes, and what are their corresponding channel names?':
    cursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Name, like_count AS Like_Count FROM channel
                   JOIN playlist ON channel.channel_id = playlist.channel_id 
                   JOIN video ON playlist.playlist_id = video.playlist_id
                   ORDER BY like_count DESC LIMIT 10""")
    output = cursor.fetchall()
elif query == '6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    cursor.execute("""SELECT video_name AS Video_Name, like_count AS Number_Of_Likes FROM video
                   ORDER BY like_count DESC""")
    output = cursor.fetchall()
elif query == '7.	What is the total number of views for each channel, and what are their corresponding channel names?':
    cursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Channel_views FROM channel
                   ORDER BY channel_views DESC""")
    output = cursor.fetchall()
elif query == '8.	What are the names of all the channels that have published videos in the year 2022?':
    cursor.execute("""SELECT channel_name AS Channel_Name, count(video.video_id) AS Number_of_Videos FROM channel
                    JOIN playlist ON channel.channel_id = playlist.channel_id 
                    JOIN video ON playlist.playlist_id = video.playlist_id
                    WHERE video.published_date LIKE '2022%'
                    GROUP BY channel.channel_name""")
    output = cursor.fetchall()
elif query == '9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cursor.execute("""SELECT channel_name AS Channel_Name, avg(video.duration) AS Average_Duration_of_Videos_Secs FROM channel
                    JOIN playlist ON channel.channel_id = playlist.channel_id 
                    JOIN video ON playlist.playlist_id = video.playlist_id
                    GROUP BY channel.channel_name""")
    output = cursor.fetchall()
elif query == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cursor.execute("""SELECT channel_name AS Channel_Name, COUNT(comment.comment_id) AS Number_of_Comments FROM channel 
                      JOIN playlist ON channel.channel_id = playlist.channel_id 
                      JOIN video ON playlist.playlist_id = video.playlist_id
                      JOIN comment ON video.video_id = comment.video_id
                      GROUP BY channel.channel_name
                      ORDER BY COUNT(comment_id) DESC""")
    output = cursor.fetchall()


if output:
    df = pd.DataFrame(output, columns=cursor.column_names)
    st.write(df)
else:
    st.write("No data found for the selected query.")

# Close the mysql connection
cursor.close()
cnx.close()

