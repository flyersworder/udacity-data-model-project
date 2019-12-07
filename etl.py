import io
import os
import sys
import glob
import time
import psycopg2
import pandas as pd
import hashlib
from sql_queries import *

start_time = time.time()

def copy_df_to_table(cur, df, table, sql_stmt=None):
    """
    Description: This function copy a pandas dataframe by block to a pg table (rather than do it in rows).
    Therefore this method is much faster (about half the time).

    Arguments:
        cur: the cursor object. 
        df: a pandas data frame object.
        table: the table name (string).
        sql_stmt: a sql statement to be executed.

    Returns:
        None
    """
    s_buf = io.StringIO()
    # saving a data frame to a buffer (same as with a regular file):
    df.to_csv(s_buf, header=False, index=False, sep='\t')
    s_buf.seek(0)
    
    if sql_stmt is None:
        try:
            cur.copy_from(s_buf, table)
        except psycopg2.Error as e:
            print(f'Unable to copy data frame to the table {table}')
            print(e.pgerror)
    else:
        try:
            cur.copy_from(s_buf, table)
            cur.execute(sql_stmt)
            cur.execute(f"DELETE FROM {table}")
        except psycopg2.Error as e:
            print(e.pgerror)

def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used to populate the song and artist dim tables
    .

    Arguments:
        cur: the cursor object. 
        filepath: log data file path.

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, typ='series').to_frame().T

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].loc[0].values.tolist()
    
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print(e.pgerror)
    
    # insert artist record
    artist_data = df[[
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].loc[
        0].values.tolist()
    
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
            print(e.pgerror)

def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms').astype('datetime64[s]')
    
    # insert time data records
    time_data = [t.dt.time, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
    copy_df_to_table(cur, time_df, 'temp_time', time_table_insert)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    copy_df_to_table(cur, user_df, 'temp_user', user_table_insert)

    # insert songplay records
    file_id = hashlib.md5()
    file_id.update(str.encode(filepath))
    file_id = str(int(file_id.hexdigest(), 16))[0:11] # create a unique id for each log file
    songplay_list = []
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [file_id + str(index), pd.to_datetime(
            row.ts, unit='ms').replace(
            microsecond=0), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        songplay_list.append(songplay_data)
        
    copy_df_to_table(cur, pd.DataFrame(songplay_list), 'songplays')

def process_data(cur, conn, filepath, func):
    """
    Description: This function can be used to process the data: read all the file in the file path and populate 
    the tables using their functions accordingly.

    Arguments:
        cur: the cursor object.
        conn: the connection object.
        filepath: log data file path. 
        func: the function used to process the data.

    Returns:
        None
    """

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
        print('{}/{} files processed.'.format(i, num_files))

def main():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
            
        conn.close()
        
    except psycopg2.OperationalError as e:
        print(f'Unable to connect!\n{e}')
        sys.exit(1)

if __name__ == "__main__":
    main()
    
# track running time
print("--- %s seconds ---" % (time.time() - start_time))