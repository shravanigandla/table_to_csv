import mysql.connector
import logging
import pandas
import sys
import  datetime
import os
import configparser
import math


proj_folder_path=sys.argv[1] #Path where script located eg:/home/****/***/
csv_folder_path=sys.argv[2] #Path to store csv files


#Read configurations from file
config = configparser.ConfigParser()
config.read(proj_folder_path+'properties.config')
snowflake_config = {}


#mysql configuration
mysql_config = {}
mysql_config['host']=config['MYSQL_CONFIG']['host']
mysql_config['user']=config['MYSQL_CONFIG']['user']
mysql_config['password']=config['MYSQL_CONFIG']['password']
mysql_config['database']=config['MYSQL_CONFIG']['database']
logging_level =config['MYSQL_CONFIG']['logging_level']
tables=config['MYSQL_CONFIG']['tables']
if (tables) != '':
    table_list = (tables.split(','))
else:
    table_list =[]
tables=[]
table_dict={}
for i in table_list:
    table_dict[i.split(':')[0]]=i.split(':')[-1].strip('()').split('=')[-1].strip('[]').split(',')
table_list = list(table_dict.keys())

# Get mail config
to_mail = config['MAIL_CONFIG']['to_mail']
cc_mail = config['MAIL_CONFIG']['cc_mail']

#file's , directory's name configuration
table_dump_csv_log=proj_folder_path+config['DIR_FILE_CONFIG']['table_dump_csv_log']
csv_storage_dir=csv_folder_path+config['DIR_FILE_CONFIG']['csv_storage_dir']
max_records_csv=int(config['DIR_FILE_CONFIG']['max_records_csv'])

for logger_name in ['mysql.connector', 'botocore']:
    logger = logging.getLogger(logger_name)
    if (logging_level == 'INFO'):
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    ch = logging.FileHandler(table_dump_csv_log)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter(
        '%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)




def mysql_connection(mysql_config):
    try:
        #connect to mysql
        conn = mysql.connector.connect(
            user=mysql_config['user'],
            password=mysql_config['password'],
            host=mysql_config['host'],
            database=mysql_config['database']
        )
        logger.info("Mysql connection for database {} established successfully".format(mysql_config['database']))
        return conn
    except Exception as err:
        logger.info("Failed to establish Mysql connection for database {}".format(mysql_config['database']))
        logger.info("ERROR : {}".format(err))
        sys.exit()

def table_to_csv(mysql_conn,table,single_unique_key):

    #remove all files from directory which stores csv files
    if os.path.exists(csv_storage_dir):
        for file in os.listdir(csv_storage_dir):
            os.remove(csv_storage_dir+"/"+file)
    else:
        os.mkdir(csv_storage_dir)
    query = 'select count(*) as count,UNIX_TIMESTAMP() as timestamp from  {}'.format(table)
    try:
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute(query)
    except Exception as error:
        logger.info("Error occured at getting record count from table {}".format(table))
        logger.info("ERROR : {} ".format(error))
        sys.exit()
    mysql_res = mysql_cursor.fetchone()
    table_row_count = mysql_res[0]
    table_created_timestamp = mysql_res[1]

    if single_unique_key == 'exists':
    # select data from mysql
        query='select min({unique_id}) as min_value,max({unique_id}) as max_value from  {table}'.format(unique_id=json_data["unique_id"],table=table)
        try:
            mysql_cursor=mysql_conn.cursor()
            mysql_cursor.execute(query)
        except Exception as error:
            logger.info("Error occured at getting record count from table {}".format(table))
            logger.info("ERROR : {} ".format(error))
            sys.exit()
        mysql_res = mysql_cursor.fetchone()
        if table_row_count!=0:
            max_rows=max_records_csv
            table_min_value=mysql_res[0]
            table_max_value=mysql_res[1]
            table_row_count1 = (table_max_value+1)-table_min_value
            div_factor=math.ceil(table_row_count1/max_rows)
            list = [table_min_value]
            temp = table_min_value
            for i in range(div_factor):
                temp = temp + max_rows
                list.append(temp)
            st_time = datetime.datetime.now()
            for i in range(len(list) - 1):
                #print(list[i], list[i + 1])
                en_time = datetime.datetime.now()
                print(en_time - st_time)
                st_time = datetime.datetime.now()
                query = 'select * from  {table} where {unique_id}>={min_value} and {unique_id}<{max_value}'.format(table=table,unique_id=json_data["unique_id"],min_value=list[i],max_value=list[i+1])
                results = pandas.read_sql_query(query, mysql_conn)
                if results.empty == True:
                    continue
                print("processing . . . ")
                results = results.replace('\n', '', regex=True)
                for column in results:
                    results[column] = results[column].replace('\s+', ' ', regex=True)
                try:
                    # create and insert data to csv file
                    results.to_csv(csv_storage_dir+"/"+table+str(i)+".csv", index=False)
                    logger.info("csv file created successfully for table {}".format(table))
                except Exception as error:
                    logger.info("Error occured at creating csv file for table {}".format(table))
                    logger.info("ERROR : {} ".format(error))
                    sys.exit()
        else:
            logger.info("Table {} created successfully".format(table))

    else:
        query = 'select * from  {}'.format(table)
        results = pandas.read_sql_query(query, mysql_conn)
        results = results.replace('\n', '', regex=True)
        for column in results:
            results[column] = results[column].replace('\s+', ' ', regex=True)
        try:
            # create and insert data to csv file
            results.to_csv(csv_storage_dir + "/" + table + ".csv", index=False)
            logger.info("csv file created successfully for table {}".format(table))
        except Exception as error:
            logger.info("Error occured at creating csv file for table {}".format(table))
            logger.info("ERROR : {} ".format(error))
            sys.exit()

mysql_conn = mysql_connection(mysql_config)
for table in table_list:
    unique_key_list = table_dict[table]
    if len(unique_key_list)!=1:
        single_unique_key = 'exists'
    else:
        single_unique_key = 'not exists'
    table_to_csv(mysql_conn, table,single_unique_key)
