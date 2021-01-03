import petl as etl
import psycopg2 as db
from decouple import config
from faker import Faker

#ETL Script for 'public.notes' table. 

#Extract
#Define database connection paraemters for original database
ODBNAME = config('ORIGINAL_DBNAME')
ODBUSER = config('ORIGINAL_DBUSER')
ODBPASSWORD = config('ORIGINAL_DBPASSWORD')
ODBHOST = config('ORIGINAL_DBHOST')
ODBPORT = config('ORIGINAL_DBPORT')

oConnection = db.connect(database=ODBNAME, user=ODBUSER, password=ODBPASSWORD, 
host = ODBHOST, port = ODBPORT)

#Since we use target planter table to fill FK field in notes table, need target DB. 
#Define database connection paraemters for target database
TDBNAME = config('TARGET_DBNAME')
TDBUSER = config('TARGET_DBUSER')
TDBPASSWORD = config('TARGET_DBPASSWORD')
TDBHOST = config('TARGET_DBHOST')
TDBPORT = config('TARGET_DBPORT')

tConnection = db.connect(database=TDBNAME, user=TDBUSER, password=TDBPASSWORD, 
host = TDBHOST, port = TDBPORT)

table = etl.fromdb(oConnection, 'SELECT * FROM public.notes')

planterTable = etl.fromdb(tConnection, 'SELECT id FROM public.entity')

#Transform
#Join notes table with planterTable on 'planter_id'
joinTable = etl.join(table, planterTable, lkey='planter_id', rkey='id')

#Anonymize values
joinTable = etl.convert(joinTable, {
    'content': lambda row: None #Some transformation
})

#Load
etl.todb(joinTable, tConnection, 'notes', 'public')