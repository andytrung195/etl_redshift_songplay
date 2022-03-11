import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, check_stl_load_error,check_event_staging,check_song_staging,test


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def load_error(cur, conn):
    cur.execute(check_stl_load_error)
    row = cur.fetchone()
    
    while row:
        print(row)
        row = cur.fetchone()
        
def check_event_table(cur,conn):
    cur.execute(check_event_staging)
    row = cur.fetchone()
    
    while row:
        print(row,'\n')
        row = cur.fetchone()
        
def check_song_table(cur,conn):
    cur.execute(check_song_staging)
    row = cur.fetchone()
    
    while row:
        print(row,'\n')
        row = cur.fetchone()
        
def test_func(cur,conn):
    cur.execute(test)
    row = cur.fetchone()
    
    while row:
        print(row,'\n')
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
#     test_func(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()