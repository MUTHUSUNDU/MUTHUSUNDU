from googleapiclient.discovery import build
import google_auth_oauthlib.flow
import pymongo
import pandas as pd
import re
import  mysql.connector as sql
import json
import streamlit as st

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Api key connection
def Api_connection():
    Api_Id = "AIzaSyDLGSKNJsRJaINpmVXcmqk2tcPNXDk6CZE"
    
    api_service_name = "youtube"
    
    api_version = "v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    
    return youtube

youtube = Api_connection()

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
#get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
                    part = "snippet, ContentDetails, Statistics",
                    id = channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data
#------------------------------------------------------------------------------------------------------------------------------------------------------------------

#get vidoe ids
def get_videos_ids(channel_id):
    video_ids=[]

    response=youtube.channels().list(id =channel_id,
                                        part='ContentDetails').execute()
         
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_ID,
            maxResults=50, 
            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken') 
        
        if next_page_token is None:
            break
    return video_ids
#------------------------------------------------------------------------------------------------------------------------------------------------------------------


def get_video_info(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:
            request=youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id=video_id
            ) 
            
            #try:
            response=request.execute()
        # except Exception as e:
                #print(f"Error fetching video info for {video_id}: {e}")
            # continue  # Skip this video and proceed to the next one

            for item in  response["items"]: #response.get('items', []):
                data=dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Descrption=item['snippet'].get('description'),
                        Published_Date=item['snippet']['publishedAt'],
                                         
                        Duration=item['contentDetails']['duration'], #parse_duration(item['contentDetails']['duration']),
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favourite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item.get('caption')
                        )
                
                video_data.append(data)
    return video_data

#------------------------------------------------------------------------------------------------------------------------------------------------------------------

#get comment information
# print(Video_Ids)
def get_comment_info(Video_Ids):
    Comment_data=[]
        
    try:
        
        for video_id in Video_Ids:
            
            request=youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
            )
            #print(request)

            response=request.execute()
           
            for item in response['items']:
               
                #print(item)
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        
                Comment_data.append(data)
    except:
        pass
        
    return Comment_data

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
#get playlist details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet, contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount']
                    )
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
#upload to  mongodb
client=pymongo.MongoClient("localhost:27017")
db=client.youtube_test1

def channel_details(channel_id):
    channel_details=get_channel_info(channel_id)
    playlist_details=get_playlist_details(channel_id)
    Video_Ids=get_videos_ids(channel_id)
    video_details=get_video_info(Video_Ids)
    comment_details=get_comment_info(Video_Ids)

    Collection1=db.Channel_Details
    Collection1.insert_one({"channel_information":channel_details, "playlist_information":playlist_details, "video_information":video_details, "comment_information":comment_details})

    return "upload completed successfully"

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
#table creation for channels,playlists,videos,comments

def channels_table():

    mydb=sql.connect(host="127.0.0.1",
                        user="muthus",
                        password="Oneness@1",
                        auth_plugin='mysql_native_password',
                        database= "youtube_test1")

    cursor=mydb.cursor()
    def create_or_truncate_table():
                #cursor = mydb.cursor()

                # Check if the table exists
                cursor.execute("SHOW TABLES LIKE 'channels'")
                result = cursor.fetchone()

                if result:
                        # Table exists, truncate it
                        truncate_query = "TRUNCATE TABLE channels"
                        cursor.execute(truncate_query)
                        mydb.commit()
                else:
                        try:
                            create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                                Channel_Id varchar(80) primary key,
                                                                                Subscribers bigint,
                                                                                Views bigint,
                                                                                Total_Videos int,
                                                                                Channel_Description text,
                                                                                Playlist_Id varchar(80))'''
                            cursor.execute(create_query)
                            mydb.commit()

                        except:
                            print("Channels table already created")

    create_or_truncate_table()
    

    ch_list=[]

    db=client["youtube_test1"]
    collection1=db['Channel_Details']

    for ch_data in collection1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers ,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            print("Channel values are already inserted")

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
def playlist_table():
    mydb=sql.connect(host="127.0.0.1",
                        user="muthus",
                        password="Oneness@1",
                        auth_plugin='mysql_native_password',
                        database= "youtube_test1")

    cursor=mydb.cursor()

    def create_or_truncate_table():
                #cursor = mydb.cursor()

                # Check if the table exists
                cursor.execute("SHOW TABLES LIKE 'playlists'")
                result = cursor.fetchone()

                if result:
                        # Table exists, truncate it
                        truncate_query = "TRUNCATE TABLE playlists"
                        cursor.execute(truncate_query)
                        mydb.commit()
                else:

                        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                                            Title varchar(500) ,
                                                                            Channel_Id varchar(500),
                                                                            Channel_Name varchar(500),
                                                                            PublishedAt timestamp,
                                                                            Video_Count int)'''
                        cursor.execute(create_query)
                        mydb.commit()

    create_or_truncate_table()

    pl_list=[]

    db=client["youtube_test1"]
    collection1=db['Channel_Details']

    for pl_data in collection1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=pd.DataFrame(pl_list)
     # Convert Published_Date to SQL date format
    df1['PublishedAt'] = pd.to_datetime(df1['PublishedAt']).dt.strftime('%Y-%m-%d %H:%M:%S')

    for index, row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''

        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count'])
                


        cursor.execute(insert_query, values)
        mydb.commit()
#------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
def videos_table():
    mydb=sql.connect(host="127.0.0.1",
                        user="muthus",
                        password="Oneness@1",
                        auth_plugin='mysql_native_password',
                        database= "youtube_test1")

    cursor=mydb.cursor()

    def create_or_truncate_table():
                #cursor = mydb.cursor()

                # Check if the table exists
                cursor.execute("SHOW TABLES LIKE 'videos'")
                result = cursor.fetchone()

                if result:
                        # Table exists, truncate it
                        truncate_query = "TRUNCATE TABLE videos"
                        cursor.execute(truncate_query)
                        mydb.commit()
                else:
                    
                    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                                    Channel_Id varchar(100),
                                                                    Video_Id varchar(30) primary key,
                                                                    Title varchar(150),
                                                                    Tags text,
                                                                    Thumbnail varchar(5000),
                                                                    Descrption text,
                                                                    Published_Date timestamp,
                                                                    Duration time,
                                                                    Views bigint,
                                                                    Likes bigint,
                                                                    Comments int,
                                                                    Favourite_Count int,
                                                                    Definition varchar(30),
                                                                    Caption_Status varchar(50))'''
                    cursor.execute(create_query)
                    mydb.commit()
        
    create_or_truncate_table()

    vi_list=[]

    def convert_duration(duration_str):
        # Remove the 'PT' prefix
        duration_str = duration_str[2:]
        
        # Split the duration string into hours, minutes, and seconds parts
        parts = duration_str.split('H')
        
        # Initialize hours, minutes, and seconds to 0
        hours = minutes = seconds = 0
        
        # If the duration contains hours
        if len(parts) > 1:
            hours = int(parts[0])
            duration_str = parts[1]
        
        # Split the remaining string into minutes and seconds
        parts = duration_str.split('M')
        
        # If the duration contains minutes
        if len(parts) > 1:
            minutes = int(parts[0])
            duration_str = parts[1]
        
        # Remove the 'S' suffix and convert the remaining string to seconds
        if duration_str[:-1]:
            seconds_str = int(duration_str[:-1])
        else:
            seconds = 0
        
        # Return the formatted duration string
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


    db=client["youtube_test1"]
    collection1=db['Channel_Details']

    for vi_data in collection1.find({},{"_id":0, "video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data["video_information"][i])


    df2=pd.DataFrame(vi_list)
    # df2.columns

    # Apply the conversion function to the duration column
    df2['Duration'] = df2['Duration'].apply(convert_duration)

    # Convert Published_Date to SQL date format
    df2['Published_Date'] = pd.to_datetime(df2['Published_Date']).dt.strftime('%Y-%m-%d %H:%M:%S')


    # Print the updated DataFrame
    #print(df2)

    for index, row in df2.iterrows():
    
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Descrption,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favourite_Count,
                                            Definition,
                                            Caption_Status )
                                                
                                                
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

       
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                json.dumps(row['Tags'] if row['Tags'] is not None else {}),  # Convert dictionary to JSON string
                row['Thumbnail'],
                row['Descrption'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favourite_Count'],
                row['Definition'],
                row['Caption_Status'])
                        
       # try:
        cursor.execute(insert_query, values)
        mydb.commit()
       # except TypeError as e:
            #print("Error inserting data:", e)
            #print("Problematic value:", row)


#------------------------------------------------------------------------------------------------------------------------------------------------------------------
#comment table creation in sql
def comments_table():
        mydb=sql.connect(host="127.0.0.1",
                        user="muthus",
                        password="Oneness@1",
                        auth_plugin='mysql_native_password',
                        database= "youtube_test1")

        cursor=mydb.cursor()

 

        def create_or_truncate_table():
                #cursor = mydb.cursor()

                # Check if the table exists
                cursor.execute("SHOW TABLES LIKE 'comments'")
                result = cursor.fetchone()

                if result:
                        # Table exists, truncate it
                        truncate_query = "TRUNCATE TABLE comments"
                        cursor.execute(truncate_query)
                        mydb.commit()
                else:
                        # Table doesn't exist, create it
                        create_query = '''
                        CREATE TABLE comments (
                                Comment_Id VARCHAR(500) PRIMARY KEY,
                                Video_Id VARCHAR(50),
                                Comment_Text TEXT,
                                Comment_Author VARCHAR(150),
                                Comment_Published TIMESTAMP
                        )
        '''
                        cursor.execute(create_query)
                        mydb.commit()

# Call the function to create or truncate the table
        create_or_truncate_table()




        com_list=[]

        db=client["youtube_test1"]
        collection1=db['Channel_Details']
        

        for com_data in collection1.find({},{"_id":0, "comment_information":1}):
                for i in range(len(com_data['comment_information'])):
                        com_list.append(com_data["comment_information"][i])

        df3=pd.DataFrame(com_list)
        #print(df3)
        # Convert Published_Date to SQL date format
        df3['Comment_Published'] = pd.to_datetime(df3['Comment_Published']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        for index, row in df3.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text ,
                                                Comment_Author,
                                                Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
                
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )
      
                cursor.execute(insert_query, values)
                mydb.commit()

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"
#------------------------------------------------------------------------------------------------------------------------------------------------------------------

def show_channels_table():
    ch_list=[]

    db=client["youtube_test1"]
    collection1=db['Channel_Details']

    for ch_data in collection1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)

    return df

#------------------------------------------------------------------------------------------------------------------------------------------------------------------

def show_playlists_table():
    pl_list=[]

    db=client["youtube_test1"]
    collection1=db['Channel_Details']

    for pl_data in collection1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=st.dataframe(pl_list)

    return df1

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
def show_vidoes_list():
    vi_list=[] 
    db=client["youtube_test1"]
    collection1=db['Channel_Details']

    for vi_data in collection1.find({},{"_id":0, "video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data["video_information"][i])


    df2=st.dataframe(vi_list)

    return df2

#------------------------------------------------------------------------------------------------------------------------------------------------------------------
def show_comments_table():
        com_list=[]

        db=client["youtube_test1"]
        collection1=db['Channel_Details']


        for com_data in collection1.find({},{"_id":0, "comment_information":1}):
                for i in range(len(com_data['comment_information'])):
                        com_list.append(com_data["comment_information"][i])

        df3=st.dataframe(com_list)

        return df3
#------------------------------------------------------------------------------------------------------------------------------------------------------------------


#streamlit 

with st.sidebar:
    st.title(":red[Youtube Data Harvesting and Warehousing]")
    st.header("Technologies Explored")
    st.caption("API integration")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Data Management using Mongodb and SQL")

channel_id=st.text_input("Enter the Channel Id")


if st.button("Submit"):
# if channel_id and st.button("Submit"):
#     def embed_youtube_video(video_id):
#         st.write(f"## Video")
#         st.markdown(f'<iframe width="560" height="315" src="https://www.youtube.com/embed/{video_id}" frameborder="0" allowfullscreen></iframe>', unsafe_allow_html=True)
#     v_ids = get_videos_ids(channel_id)

#     # Display each video in the Streamlit app
#     for video_id in v_ids:
#         embed_youtube_video(video_id)
#         break # Exit the loop after embedding one video

    ch_ids=[]
    db=client["youtube_test1"]
    collection1=db["Channel_Details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]['Channel_Id'])

    if channel_id in ch_ids:
        st.success("Channel Details is already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("Select the table for view",("Channels","Playlists","Videos","Comments"))
    
if show_table=="Channels":
    show_channels_table()

elif show_table=="Playlists":
    show_playlists_table()

elif show_table=="Videos":
    show_vidoes_list()

elif show_table=="Comments":
    show_comments_table()

#------------------------------------------------------------------------------------------------------------------------------------------------------------------

#SQL connection
mydb=sql.connect(host="127.0.0.1",
                        user="muthus",
                        password="Oneness@1",
                        auth_plugin='mysql_native_password',
                        database= "youtube_test1")

cursor=mydb.cursor()

# Define the questions
Question = st.selectbox("Select your questions", (
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))



# # Define a function to format the questions
# def format_question(question, max_question_length=300):
#     # Adjust the width of the selectbox based on the length of the questions
#     return f"{question:<{max_question_length}}"

# # Create the selectbox with formatted questions
# Question = st.selectbox("Select your questions", questions, format_func=format_question)



if Question == '1. What are the names of all the videos and their corresponding channels?':
    query1='''select Title as videos , Channel_Name as channelname from videos order by Channel_Name'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    mydb.commit()
    df=pd.DataFrame(t1, columns=["Video_Title","Channel_Name"])
    st.write(df)


elif Question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2='''select Channel_Name as channelname,Total_Videos as No_Videos from Channels order by Total_Videos DESC'''
    cursor.execute(query2)
    t2=cursor.fetchall()
    mydb.commit()
    df1=pd.DataFrame(t2, columns=["Channel_Name", "No_Videos"])
    st.write(df1)


elif Question == '3. What are the top 10 most viewed videos and their respective channels?':
    query3='''select  Channel_Name as channelname,Title as videoname, Views as top_Views from videos order by Views DESC LIMIT 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    mydb.commit()
    df2=pd.DataFrame(t3, columns=["Channel_Name", "Video_Title","Views"])
    st.write(df2)

elif Question =='4. How many comments were made on each video, and what are their corresponding video names?':
        query4='''select Title as videoname, Comments as no_comments from videos order by Comments DESC '''
        cursor.execute(query4)
        t4=cursor.fetchall()
        mydb.commit()
        df3=pd.DataFrame(t4, columns=["Video_Title","No_Comments"])
        st.write(df3)

elif Question =='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5='''select  Channel_Name as channelname,Title as videoname, Likes as No_Likes from videos order by Likes DESC '''
    cursor.execute(query5)
    t5=cursor.fetchall()
    mydb.commit()
    df4=pd.DataFrame(t5, columns=["Channel_Name","Video_Title","No_Likes"])
    st.write(df4)

elif Question =='6. What is the total number of likes for each video, and what are their corresponding video names?':
    query6='''select  Title as videoname, Likes as No_Likes from videos order by Likes DESC limit 20'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    mydb.commit()
    df5=pd.DataFrame(t6, columns=["Video_Title","No_Likes"])
    st.write(df5)


elif Question =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7='''select  Channel_Name as channelname,Views as Total_No_of_Views from channels '''
    cursor.execute(query7)
    t7=cursor.fetchall()
    mydb.commit()
    df6=pd.DataFrame(t7, columns=["Channel_Name","Total_No_of_Views"])
    st.write(df6)

elif Question =='8. What are the names of all the channels that have published videos in the year 2022?':
    query8='''select  Channel_Name as channelname,Title as videoname, YEAR(Published_Date) AS year_2022 from videos WHERE YEAR(Published_Date) = 2022 '''
    cursor.execute(query8)
    t8=cursor.fetchall()
    mydb.commit()
    df7=pd.DataFrame(t8, columns=["Channel_Name","Video_Name","Year"])
    st.write(df7)

elif Question =='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9='''SELECT 
        Channel_Name AS channelname, 
        CONCAT(
            FLOOR(ROUND(AVG(TIME_TO_SEC(Duration)) / 60)), 
            ' minutes ', 
            MOD(ROUND(AVG(TIME_TO_SEC(Duration)) / 60), 60), 
            ' seconds'
        ) AS average_duration_string
    FROM 
        videos
    GROUP BY 
        Channel_Name;  '''
    cursor.execute(query9)
    t9=cursor.fetchall()
    mydb.commit()
    df8=pd.DataFrame(t9, columns=["Channel_Name","Video_Name",])
    st.write(df8)

elif Question =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10='''select Channel_Name AS channelname, Title as videoname, Comments as no_comment from
    videos order by Comments DESC limit 1'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    mydb.commit()
    df9=pd.DataFrame(t10, columns=["Channel_Name","Video_Name","No_Comments"])
    st.write(df9)
