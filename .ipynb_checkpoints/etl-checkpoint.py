import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, check_stl_load_error, check_event_staging, check_song_staging, test


def load_staging_tables(cur, conn):
    """
    - load query from copy_table_queries and execute
    
    - read file from s3 bucket and insert rows into staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - load query from insert_table_queries and execute
    
    - copy data from staging table and insert into analytic tables
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def load_error(cur, conn):
    """
    - check error status in stl table in redshift
    """
    cur.execute(check_stl_load_error)
    row = cur.fetchone()

    while row:
        print(row)
        row = cur.fetchone()


def check_event_table(cur, conn):
    """
    check data in event_staging table
    """
    cur.execute(check_event_staging)
    row = cur.fetchone()

    while row:
        print(row, '\n')
        row = cur.fetchone()


def check_song_table(cur, conn):
    """
    check data in song_staging table
    """
    cur.execute(check_song_staging)
    row = cur.fetchone()

    while row:
        print(row, '\n')
        row = cur.fetchone()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
#     load_error(cur,conn)
#     check_event_table(cur,conn)
#     check_song_table(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()
