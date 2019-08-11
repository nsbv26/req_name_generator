import psycopg2
from config import config
#import dbconnectCERN
import csv
import datetime
import os
from pathlib import Path

def connect():
    """ Connect to the PostgreSQL database server """
    params = config.config('cmis')
    conn = psycopg2.connect(**params)
    #return(conn)

    try:
        # read connection parameters
        print("in try1")
        #params = config.config(cmis)
        print("in try2")
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        #conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()


        #Empty table
        sql1 = """truncate prc_requisition_name;"""
        cur.execute(sql1)

        #open file
        data_folder  = Path("E:/Shared/prc_req_name/")
        file_to_open = data_folder / "req_name.csv"

        in_file = open(file_to_open, mode="r")
        csvReader = csv.reader(in_file)

        SQL = """
            COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','
            """
        def process_file(conn,table,file_object):
            cursor = conn.cursor()
            cursor.copy_expert(sql=SQL % table, file=file_object)
            conn.commit()
            cursor.close()


        #Load CMIS data
        process_file(conn, 'prc_requisition_name',in_file)



        in_file.close()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

if __name__ == '__main__':
    connect()
