import psycopg2
from config import config
import pandas as pd
import numpy as np
from datetime import date

today = date.today()
date = today.strftime("%m.%d.%Y")

days=45

def connect(db):
    params = config.config(db)
    conn = psycopg2.connect(**params)
    return(conn)

def getData(db,sql):
    conn = None

    try:

        # Open database connection
        conn = connect(db)

        ## Open and read the file as a single buffer
        sqlFile  = open('SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

##############################################################
# Gather ClosedStack asset data and solutioon to sku mapping #
##############################################################

##connect to the closedstack db and pull assets as a df
asp_assets = getData('closedstackasp','asset_demand.sql')

fed_assets = getData('closedstackfed','asset_demand.sql')

corp_assets = getData('cmis','corp_asset_demand.sql')

###################################
#Combine ASP, Fed, and Corp assets#
###################################

assets = [asp_assets,fed_assets,corp_assets]

assets = pd.concat(assets)

assets = assets.drop_duplicates(subset='asset_id',keep='first').reset_index(drop=True)

################################################################
#create requisition date and convert to naming standard (mm.yy)#
################################################################

assets['monthdue']=pd.to_datetime(assets['monthdue'])

assets['req_date']=assets['monthdue']-pd.to_timedelta(days,unit='D')

assets['monthyear']=assets['req_date'].apply(lambda x:x.strftime('%m.%y'))

###########################################################
#Connect to CMIS DB to pull solution and site alias tables#
###########################################################

solution_alias = getData('cmis','solution_alias.sql')

location_alias = getData('cmis','location_alias.sql')

####################################
#Merge asset data with alias tables#
####################################

assets = pd.merge(assets, solution_alias, on='solution')

assets = pd.merge(assets,location_alias, on='site')

## drop extra columns
assets = assets.drop(['monthdue','req_date','site','solution'], axis=1)

## reorder columns
assets = assets[['asset_id','monthyear','client','alias','site_alias']]

##########################
#Create requisition names#
##########################

assets['name'] = assets['monthyear'].str.cat(assets['client'],sep=" ")

assets['name'] = assets['name'].str.cat(assets['alias'],sep=" ")

assets['name'] = assets['name'].str.cat(assets['site_alias'],sep=" ")

## reduce columns to asset id and newly created req names
assets = assets.drop(['monthyear','client','alias','site_alias'], axis=1)

assets.to_csv(r'C:\Users\NB044705\OneDrive - Cerner Corporation\development\output\assets.csv',index=False)
