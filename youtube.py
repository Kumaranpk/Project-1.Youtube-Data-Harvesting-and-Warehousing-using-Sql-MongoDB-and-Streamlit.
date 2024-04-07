from googleapiclient.discovery import build
import streamlit as st
import pymongo
import mysql.connector as mysql
import pandas as pd
from datetime import datetime
from isodate import parse_duration


#connecting Api
def main():
    Api_id = "AIzaSyCFWj4M5fuqTExL9mlTwMEuLApO8rEMT7I"
    Api_service_name = "youtube"
    Api_version = "v3"
    youtube = build(Api_service_name,Api_version,developerKey=Api_id)
    return youtube

youtube = main()

#getting channels information

def get_channels(channel_Id):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_Id
    )
    response = request.execute()

    for i in response["items"]:
        channel_info = dict(channel_name= i["snippet"]["title"],
                        channel_id= i["id"],
                        subscribers_count= i["statistics"]["subscriberCount"],
                        channel_views= i["statistics"]["viewCount"],
                        channel_descirption= i["snippet"]["description"],
                        total_videos= i['statistics']['videoCount'])
    return channel_info

#getting video_ids

def get_video_ids(channel_id):
        videos_id = []
        request = youtube.channels().list(
                part = "contentDetails",
                id = channel_id)
        response = request.execute()

        upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None

        while True:
                request1 = youtube.playlistItems().list(
                                                part = 'snippet',
                                                playlistId = upload_id,
                                                maxResults = 50,
                                                pageToken = next_page_token)
                response1 = request1.execute()


                for i in range(len(response1['items'])):
                        videos_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token = response1.get('nextPageToken')
                
                if next_page_token is None:
                        break

        return(videos_id)

#get video information

def get_video_info(videos_id):
    video_infos = []
    for i in videos_id:
        request2 = youtube.videos().list(
                    part = "snippet,contentDetails,statistics",
                    id = i)
        response2 = request2.execute()

        for item in response2['items']:
            video_inf = dict(video_id = item['id'],
                    video_name = item['snippet']['title'],
                    channel_name = item['snippet']['channelTitle'],
                    video_description = item['snippet'].get('description'),
                    tags = item['snippet'].get('tags'),
                    published_at = item['snippet']['publishedAt'],
                    view_count = item['statistics'].get('viewCount'),
                    like_count = item['statistics'].get('likeCount'),
                    favorite_count = item['statistics'].get('favoriteCount'),
                    comment_count = item['statistics'].get('commentCount'),
                    duration = item['contentDetails']['duration'],
                    thumbnail = item['snippet']['thumbnails']['default']['url'],
                    caption_status = item['contentDetails']['caption'])
            video_infos.append(video_inf)

    return video_infos

#get comment information
def get_comments(video_ids):
    comments_info = []
    try:
        for id in video_ids:    
            request = youtube.commentThreads().list(
                                    part = "snippet",
                                    videoId = id,
                                    maxResults = 50
                                    )
            response = request.execute()


            for i in response['items']:
                data = dict(comment_id = i['snippet']['topLevelComment'].get('id'),
                            video_id = i['snippet']['videoId'],
                            comment_text = i['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                            comment_author = i['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
                            comment_publishedAt = i['snippet']['topLevelComment']['snippet'].get('publishedAt'))
                comments_info.append(data)

    except:
        pass

    return comments_info

#connecting mongoDB with python
client = pymongo.MongoClient("mongodb+srv://bkumaranpk:mongo64@cluster0.daua4in.mongodb.net/")

#create database
db = client['youtube_proj']

#create collection for youtube channel details

def youtube_channel_details(channel_id):
    ch_details = get_channels(channel_id)
    video_ids = get_video_ids(channel_id)
    video_details = get_video_info(video_ids)
    comment_details = get_comments(video_ids)

    collec1 = db['channel_details']
    collec1.insert_one({'channel_info':ch_details,'video_info':video_details,
                        'comment_info':comment_details})
    
    return 'channel details are loaded successfully'

#create channel table in mysql

def channel_table():    
    #connect sql with python and create database
    mydb = mysql.connect(
        host = "127.0.0.1",
        user = "root",
        password = "sqlroot",
        database = "capstone_1"
    )

    cursor = mydb.cursor(buffered=True)


    #query to drop the table if exists previously

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    #query to create channel_table

    create_query = '''create table if not exists channels(channel_name varchar(255),
                                                                channel_id varchar(255),
                                                                subscribers_count int,
                                                                channel_views int,
                                                                channel_descirption text,
                                                                total_videos int)'''

    cursor.execute(create_query)
    mydb.commit()


    #collect data from mongoDB & convert into DataFrame

    channel_list = []
    db = client['youtube_proj']
    collec1 = db['channel_details']
    for channel_data in collec1.find({},{'_id':0,'channel_info':1}):
        channel_list.append(channel_data['channel_info'])

    df1 = pd.DataFrame(channel_list)


    #insert channel_details DataFrame into sql-channel_table
    for i in channel_list:
        query = '''insert into channels(channel_name,
                                            channel_id,
                                            subscribers_count,
                                            channel_views,
                                            channel_descirption,
                                            total_videos)  values(%s,%s,%s,%s,%s,%s)'''

        values = (i['channel_name'],
                i['channel_id'],
                i['subscribers_count'],
                i['channel_views'],
                i['channel_descirption'],
                i['total_videos']
                )
        #try:
        cursor.execute(query,values)
        mydb.commit()
        #except:
            #print('Channel details are already inserted')


    return "channel table created successfully"

#create video table in mysql

def video_table():    
    #connect sql with python and create database
    mydb = mysql.connect(
        host = "127.0.0.1",
        user = "root",
        passwd = "sqlroot",
        database = "capstone_1"
    )

    cursor = mydb.cursor(buffered=True)


    #query to drop the table if exists previously

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    #query to create video_table

    create_query = '''create table if not exists videos(video_id VARCHAR(255),
                                                                video_name VARCHAR(255),
                                                                channel_name VARCHAR(255),
                                                                video_description TEXT,
                                                                tags TEXT,
                                                                published_at DATETIME,
                                                                view_count INT,
                                                                like_count INT,
                                                                favorite_count INT,
                                                                comment_count INT,
                                                                duration TIME,
                                                                thumbnail VARCHAR(255),
                                                                caption_status VARCHAR(255))'''

    cursor.execute(create_query)
    mydb.commit()


    #collect data from mongoDB & convert into DataFrame

    video_list = []
    db = client['youtube_proj']
    collec1 = db['channel_details']
    for video_data in collec1.find({},{'_id':0,'video_info':1}):
        for i in range(len(video_data['video_info'])):
            video_list.append(video_data['video_info'][i])

    df2 = pd.DataFrame(video_list)


    #insert video_details DataFrame into sql-video_table

    for index,row in df2.iterrows():

                # Convert duration string to seconds
        duration_seconds = int(parse_duration(row['duration']).total_seconds())
        minutes, seconds = divmod(duration_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        duration = ("%d:%02d:%02d" % (hours, minutes, seconds))

        # Convert published_at string to datetime object
        published_at_datetime = datetime.strptime(row['published_at'], '%Y-%m-%dT%H:%M:%SZ')

        # Check if tags is a string, and convert it to a list
        tags = row['tags'] if isinstance(row['tags'], list) else [row['tags']]

        # Filter out None values from the tags list
        tags = [tag for tag in tags if tag is not None]

        query = '''insert into videos(video_id,
                                            video_name,
                                            channel_name,
                                            video_description,
                                            tags,
                                            published_at,
                                            view_count,
                                            like_count,
                                            favorite_count,
                                            comment_count,
                                            duration,
                                            thumbnail,
                                            caption_status)
                                            
                                            values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (row['video_id'],
                    "".join(row['video_name']),
                    "".join(row['channel_name']),
                    "".join(row['video_description']),
                    ", ".join(tags),
                    published_at_datetime,
                    row['view_count'],
                    row['like_count'],
                    row['favorite_count'],
                    row['comment_count'],
                    duration,
                    "".join(row['thumbnail']),
                    "".join(row['caption_status'])
                    )

        cursor.execute(query,values)
        mydb.commit()

    return "video table created successfully"

#create comments table in mysql

def comments_table():    
    #connect sql with python and create database
    mydb = mysql.connect(
        host = "127.0.0.1",
        user = "root",
        passwd = "sqlroot",
        database = "capstone_1"
    )

    cursor = mydb.cursor(buffered=True)


    #query to drop the table if exists previously

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    #query to create comments_table
    try:
        create_query = '''create table if not exists comments(comment_id varchar(255),
                                                                    video_id varchar(255),
                                                                    comment_text text,
                                                                    comment_author varchar(255),
                                                                    comment_publishedAt DATETIME)'''
    except: pass
    cursor.execute(create_query)
    mydb.commit()


    #collect data from mongoDB & convert into DataFrame

    comment_list = []
    db = client['youtube_proj']
    collec1 = db['channel_details']
    for comment_data in collec1.find({},{'_id':0,'comment_info':1}):
        for i in range(len(comment_data['comment_info'])):
            comment_list.append(comment_data['comment_info'][i])

    df3 = pd.DataFrame(comment_list)


    #insert c0mment_details DataFrame into sql-comments_table
    from datetime import datetime

    for i in comment_list:

        # Convert published_at string to datetime object
        published_at_datetime = datetime.strptime(i['comment_publishedAt'], '%Y-%m-%dT%H:%M:%SZ')

        query = '''insert into comments(comment_id,
                                                video_id,
                                                comment_text,
                                                comment_author,
                                                comment_publishedAt)  values(%s,%s,%s,%s,%s)'''

        values = (i['comment_id'],
                i['video_id'],
                i['comment_text'],
                i['comment_author'],
                published_at_datetime
                )
    
        cursor.execute(query,values)
        mydb.commit()
    
    return "comments table created successfully"

def tables():
    channel_table()
    video_table()
    comments_table()

    return "tables are created successfully"

def st_channel_table():
    channel_list = []
    db = client['youtube_proj']
    collec1 = db['channel_details']
    for channel_data in collec1.find({},{'_id':0,'channel_info':1}):
        channel_list.append(channel_data['channel_info'])

    df1 = st.dataframe(channel_list)

    return df1

def st_video_table():
    video_list = []
    db = client['youtube_proj']
    collec1 = db['channel_details']
    for video_data in collec1.find({},{'_id':0,'video_info':1}):
        for i in range(len(video_data['video_info'])):
            video_list.append(video_data['video_info'][i])
    df2 = st.dataframe(video_list)

    return df2

def st_comments_table():
    comment_list = []
    db = client['youtube_proj']
    collec1 = db['channel_details']
    for comment_data in collec1.find({},{'_id':0,'comment_info':1}):
        for i in range(len(comment_data['comment_info'])):
            comment_list.append(comment_data['comment_info'][i])

    df3 = st.dataframe(comment_list)

    return df3


#streamlit code

st.set_page_config(layout= "wide")

st.title(":rainbow[YOUTUBE DATA HARVESTING AND WAREHOUSING USING MONGODB,SQL AND STREAMLIT]")

tab1, tab2, tab3 = st.tabs([":blue[Home]", ":blue[Queries]", ":blue[About]"])

with tab3:
   st.header("Introduction", divider='blue')
   st.text('''The project YOUTUBE DATA HARVESTING AND WAREHOUSING USING MONGODB,SQL AND STREAMLIT is aims to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app. \n Overall, this project involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.''')
   
   st.header("Skills Takeaway", divider='blue')
   st.text("1.Python scripting")
   st.text("2.API integration")
   st.text("3.Data Collection")
   st.text("4.Data management using MongoDB and MySql")
   st.text("5.streamlit")

   st.header("Features", divider='blue')
   st.text("The application have the following features: \n 1.Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API. \n 2.Option to store the data in a MongoDB database as a data lake. \n 3.Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button. \n 4.Option to select a channel name and migrate its data from the data lake to a SQL database as tables. \n 5.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.")


with tab1:
   st.header("Analyzing Data")
   channel_Id = st.text_input("Enter the channel ID")

   if st.button("Collect and Store data"):
      ch_ids = []
      db = client['youtube_proj']
      collec1 = db['channel_details']
      for ch_data in collec1.find({},{'_id':0,'channel_info':1}):
         ch_ids.append(ch_data['channel_info']['channel_id'])

      if channel_Id in ch_ids:
         st.success("channel details of the given channel id is already exists")

      else:
         insert = youtube_channel_details(channel_Id)
         st.success(insert)

   if st.button("Migrate to MySql"):
      Table = tables()
      st.success(Table)

   show_table = st.radio("Select the Table to View", ("channels Table", "Videos Table", "Comments Table"))

   if show_table=="channels Table":
      st_channel_table()

   elif show_table=="Videos Table":
      st_video_table()

   elif show_table=="Comments Table":
      st_comments_table()
with tab2:
    #Sql connection-streamlit

    from numpy import average

    mydb = mysql.connect(
            host = "127.0.0.1",
            user = "root",
            passwd = "sqlroot",
            database = "capstone_1"
        )

    cursor = mydb.cursor()

    Question = st.selectbox("select your question",("1.  What are the names of all the videos and their corresponding channels?",
                                                    "2.  Which channels have the most number of videos, and how many videos do they have?",
                                                    "3.  What are the top 10 most viewed videos and their respective channels?",
                                                    "4.  How many comments were made on each video, and what are their corresponding video names?",
                                                    "5.  Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6.  What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                    "7.  What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8.  What are the names of all the channels that have published videos in the year 2022?",
                                                    "9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

    if Question=="1.  What are the names of all the videos and their corresponding channels?":
        query1 = '''select channel_name as 'channel name', video_name as 'video name' from videos'''
        cursor.execute(query1)    
        t1 = cursor.fetchall()
        d1 = pd.DataFrame(t1, columns = ['channel name','video name'])
        st.write(d1)

    elif Question=="2.  Which channels have the most number of videos, and how many videos do they have?":
        query2 = '''select channel_name as 'channel name', total_videos as 'No of videos' from channels
                    order by total_videos desc'''
        cursor.execute(query2)
        t2 = cursor.fetchall()
        d2 = pd.DataFrame(t2, columns = ['channel name','No of videos'])
        st.write(d2)

    elif Question=="3.  What are the top 10 most viewed videos and their respective channels?":
        query3 = '''select channel_name as 'channel name', video_name as 'video name', view_count as 'views' from videos
                    where view_count is not null order by view_count limit 10'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        d3 = pd.DataFrame(t3, columns = ['channel name','video name','views'])
        st.write(d3)

    elif Question=="4.  How many comments were made on each video, and what are their corresponding video names?":
        query4 = '''select video_name as 'video name', comment_count as 'No of comments' from videos
                    where comment_count is not null'''
        cursor.execute(query4)
        t4 = cursor.fetchall()
        d4 = pd.DataFrame(t4, columns = ['video name', 'No of comments'])
        st.write(d4)

    elif Question=="5.  Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = '''select channel_name as 'channel name', video_name as 'video name', like_count as 'No of likes' from videos
                    where like_count is not null order by like_count desc '''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        d5 = pd.DataFrame(t5, columns = ['channel name','video name','No of likes'])
        st.write(d5)

    elif Question=="6.  What is the total number of likes for each video, and what are their corresponding video names?":
        query6 = '''select video_name as 'video name', like_count as 'No of likes' from videos'''
        cursor.execute(query6)
        t6 = cursor.fetchall()
        d6 = pd.DataFrame(t6, columns = ['video name','No of likes'])
        st.write(d6)

    elif Question=="7.  What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = '''select channel_name as 'channel name', channel_views as 'total views' from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7 = cursor.fetchall()
        d7 = pd.DataFrame(t7, columns = ['channel name','total views'])
        st.write(d7)

    elif Question=="8.  What are the names of all the channels that have published videos in the year 2022?":
        query8 = '''select channel_name as 'channel name', video_name as 'video name', published_at as 'published date'
                    from videos where extract(year from published_at) = 2022'''
        cursor.execute(query8)
        t8 = cursor.fetchall()
        d8 = pd.DataFrame(t8, columns = ['channel name','video name', 'published date'])
        st.write(d8)

    elif Question=="9.  What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = '''select channel_name as 'channel name', AVG(duration) as 'avg duration'
                    from videos group by channel_name'''
        cursor.execute(query9)
        t9 = cursor.fetchall()
        d9 = pd.DataFrame(t9, columns = ['channel name','avg duration'])

        t9a = []
        for index,row in d9.iterrows():
            chan_name = row['channel name']
            avg_duration = row['avg duration']
            avg_duration_str = str(avg_duration)
            t9a.append(dict(Channel_Name = chan_name, Average_Duration = avg_duration_str))
        d9a = pd.DataFrame(t9a)
        st.write(d9a)

    elif Question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = '''select channel_name as 'channel name', video_name as 'video name', comment_count as 'No of comments'
                    from videos where comment_count is not null order by comment_count desc'''
        cursor.execute(query10)
        t10 = cursor.fetchall()
        d10 = pd.DataFrame(t10, columns = ['channel name','video name', 'No of comments'])
        st.write(d10)