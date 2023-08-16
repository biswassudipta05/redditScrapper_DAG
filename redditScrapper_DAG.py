# -*- coding: utf-8 -*-

# Coded by Sudipta Biswas
# biswassudipta05@gmail.com

# Script to Scrape the data from reddit and put it in Database using Airflow DAG

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sqlite3
import praw
import time
from datetime import datetime
from praw.models import MoreComments
import configparser
import traceback

def readSqliteTable():
    try:
        cursor = connection.cursor()
        # Fetch list of subreddits from dd_subreddits
        sqlite_select_query = """SELECT * from dd_subreddits;"""
        cursor.execute(sqlite_select_query)
        records = cursor.fetchall()
        cursor.close()
        return records       
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
        raise

def insert_into_table(tname,sql_data=None):
    if tname == 'dd_dates':
        sql_query_insert = """
                        INSERT INTO dd_dates(run_date,seq)
                        SELECT CURRENT_TIMESTAMP, count(*)+1
                        FROM dd_dates
                        WHERE date(run_date) = date(CURRENT_TIMESTAMP); 
                        """
    elif tname == 'ft_comments':
        sql_query_insert = """INSERT INTO ft_comments(com_id,date_key,post_id,sub_id,com_body,com_score) 
                            VALUES(?, ?, ?, ?, ?, ?);"""
                
    elif tname == 'ft_posts':
        sql_query_insert = """INSERT INTO ft_posts(post_id,date_key,sub_id,post_title,post_body,post_score,postRS,com_num) 
                            VALUES(?, ?, ?, ?, ?, ?, ?, ?);"""
    elif tname == 'ft_subRS':
        sql_query_insert = """INSERT INTO ft_subRS(date_key,sub_id,subRS) 
                            VALUES(?, ?, ?);"""

    cursor=connection.cursor()        
    try:
        if tname == 'dd_dates':
            cursor.execute(sql_query_insert)
        else:
            cursor.execute(sql_query_insert,sql_data)
        #print("Record inserted successfully")
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print(tname,sql_data)
    cursor.close()

def redditScrapper():
    print("Start Time =", datetime.now())

    try:
        global config_data, connection
        config_data = configparser.ConfigParser()

        config_data.read("meta_config.ini")
        api_secrets_config = config_data.get("config_files","api_secrets_config")
        script_config = config_data.get("config_files","script_config")

        config_data.read(api_secrets_config)
        api_secrets = config_data["api_secrets"]

        reddit = praw.Reddit(client_id = api_secrets.get("client_id"),
                            client_secret = api_secrets.get("client_secret"),
                            user_agent = api_secrets.get("user_agent"),
                            username = api_secrets.get("username"),
                            password = api_secrets.get("password"))

        config_data.read(script_config)

        try:
            connection = sqlite3.connect(config_data.get("database","db_name"))
        except sqlite3.Error as error:
            print("Error connecting to the database",error)
            raise

        posts_limit = config_data.getint("limits","posts_limit")+5  #Incase posts are NSFW then they will be skipped
        comments_limit = config_data.getint("limits","comments_limit")+2 #Incase comments are from automoderator

        sub_records = readSqliteTable()

        insert_into_table('dd_dates')
        cursor=connection.cursor()
        cursor.execute("SELECT MAX(date_key) FROM dd_dates")
        date_key = next(cursor, [None])[0]
        cursor.close()

        subs_inserted = 0

        for sub_rec in sub_records:
            submission = reddit.subreddit(sub_rec[1])

            subRS = 0
            posts_inserted = 0

            for post in submission.top(time_filter="day",limit=posts_limit):
                if post.over_18:
                    continue #If post is NSFW then skip it
                else:
                    post.comment_sort = "top" #Sort order
                    post.comment_limit = comments_limit #Incase top comment is by AutoModerator
                    top_com = post.comments.list()

                    sum = 0

                    comments_inserted = 0

                    for com in top_com:
                        if isinstance(com, MoreComments):
                            continue
                        if com.author == "AutoModerator":
                            continue
                        else:
                            com_data = (com.fullname, date_key, post.fullname, submission.fullname , com.body, com.score)
                            insert_into_table('ft_comments',com_data)
                            comments_inserted = comments_inserted + 1
                            sum = sum + com.score
                        if comments_inserted == 5:
                            break

                    if comments_inserted == 0: #Incase there are 0 comments for a Post for now
                        postRS = 0
                    else:
                        postRS = sum/comments_inserted
                    postRS = round(postRS,2)

                    post_data = (post.fullname, date_key, submission.fullname , post.title, post.selftext, post.score, postRS, post.num_comments)
                    insert_into_table('ft_posts',post_data)

                    posts_inserted = posts_inserted + 1

                    subRS = subRS + postRS

                    if posts_inserted == 10:
                            break

            if posts_inserted == 0: #Incase there are 0 post for a subreddit for now
                subRS = 0
            else:
                subRS = round((subRS/posts_inserted),2)
            print(submission.display_name,"subRS :",subRS)

            sub_data = (date_key, submission.fullname , subRS)
            insert_into_table('ft_subRS',sub_data)
            subs_inserted = subs_inserted + 1

            if subs_inserted == 25:
                print("API Cooldown Pause Start Time =", datetime.now())
                time.sleep(60*5) # Delay for 5 minutes for cooldown of Reddit API Rate limits
                print("API Cooldown Pause End Time =", datetime.now())

    except Exception:
        connection.rollback()
        traceback.print_exc()
        raise
    finally:
        connection.commit()
        connection.close()
        print("End Time =", datetime.now())


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'redditScrapper_DAG',
    default_args=default_args,
    schedule='0 8,16 * * *',  # Daily at 8th and 16th hour
    catchup=False,
) as dag:
    
    redditScrapper_task = PythonOperator(
        task_id='redditScrapper',
        python_callable=redditScrapper,
        dag=dag,
    )

    redditScrapper_task
