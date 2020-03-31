import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ Parses fields from a song file and inserts them into songs and artists tables """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = list( df[song_data_cols].values[0] )
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = list( df[artist_data_cols].values[0] )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ Parses fields from a log file and inserts them into time, user, and songplay tables """
    # open log file
    log_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_df = log_df.loc[ log_df.page == "NextSong" ]

    # convert timestamp column to datetime
    t = pd.to_datetime(log_df.ts, unit='ms')
    log_df.ts = pd.to_datetime(log_df.ts, unit='ms')
    
    # insert time data records
    time_data = list( (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday ) )
    column_labels = ['start_time', 'hour', 'week', 'month', 'year', 'day']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = log_df[user_cols]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in log_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)  
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Fetches JSON files and calls one of the two methods above depending on the file """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()