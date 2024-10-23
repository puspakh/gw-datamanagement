import schedule
import sys
import os
import time 
from datetime import datetime, timedelta
import fnmatch
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine
import logging
import configparser

# initialize logger 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) 

# Configure logging
log_filename = f'db_update_report_{datetime.now().strftime("%Y%m%d %H-%M-%S")}.log'
log_filename = log_filename.replace(':', '-')
log_file_path = os.path.join('D:\\repos\\pirnamoonsym\\updating_report\\', log_filename)

logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a StreamHandler to send logs to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter to format the log messages
formatter = logging.Formatter('%(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the StreamHandler to the logger
logger.addHandler(console_handler)

# function to read the database configuration file 
# in this configuration file 'config.ini, the data access is stored
# this config.ini file should be added to the '.gitignore' so it will not be exposed accidentally 
def read_db_config(filename='config.ini', section='database'):
    parser = configparser.ConfigParser()    # create a parser  
    parser.read(filename)   # Read the configuration file

    logger.debug("Available sections: %s", parser.sections())
    
    # get section, default to database
    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file")
    return db_config
# Print the loaded configuration for debugging
logger.info("Loaded config: %s", read_db_config())

db_config = read_db_config()

# context manager for database connection to optimize the connection in every query
# therefore, the function will not be close and open repeatedly 
# this can also minimalize the human error in the script to close the cursor and connection to the database  
class DatabaseConnection:
    def __init__(self, db_config):
        try:
            self.conn = psycopg2.connect(**db_config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            sys.exit(1)     # exit the script if an error in connection occurs

    def __enter__(self):
        return self.conn, self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

# function to rename data frame table (has nothing to do with database connection)
def preprocess(df):
    try:
        logger.info("Before renaming: %s" % df.columns)
        df.rename(columns={r'Datum/Zeit': 'date_time',
                        r'EC {ec} [mS/cm]': 'ec_mscm',
                        r'EC25 {ec_25} [mS/cm]': 'ec_25',
                        r'h {h} [mWs]': 'h_mws',
                        r'hlevel {h_level} [mNN]': 'h_level',
                        r'T {t} [°C]': 't_c',
                        r'Tintern {t_intern} [°C]': 't_intern',
                        r'Vbatt {v_batt} [V]': 'vbatt'}, inplace=True)
        logger.debug("After renaming: %s" % df.columns)
        df['Datum/Zeit'] = pd.to_datetime(df['Datum/Zeit'], dayfirst=True)
        logger.info("Dataframe pre-processing successful")
    except Exception as e:
        logger.error("An error occurred: %s" % e)

# to execute insertion of a dataframe without the id column to the database table
# in the function there is no conn.commit() because it is made to be used inside the context manager ('with DatabaseConnection:')  
def execute_values(cursor, df, table): 
    df_without_id = df.drop(columns=['id'], errors='ignore')
                
    tuples = [tuple(x) for x in df_without_id.reset_index(drop=True).to_numpy()] 
            
    cols = ','.join(list(df_without_id.columns))
                
    # SQL query to execute 
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 
    try: 
        extras.execute_values(cursor, query, tuples) 
        logger.info(f"the dataframe is inserted to {table}") 
    except (Exception, psycopg2.DatabaseError) as error: 
        logger.error("Error: %s" % error) 
        cursor.connection.rollback()
        raise 

# data query inside database table after the insertion of the data frame to the basic table
# function to insert unique table to datameasurement and append it  
def datameasurement_insert(source_db_table, target_dbtable_name, maxid_before):
    try:
        with DatabaseConnection(db_config) as (conn, cursor): 
            select_query = f'''SELECT date_time, ec_mscm, ec_25, h_mws, h_level, t_c, t_intern, vbatt, waterpoints_id, waterpoints_name, ref_level, sensor_depth 
                            FROM {source_db_table} WHERE id > {maxid_before};'''
            cursor.execute(select_query)
            filtered_data = cursor.fetchall()
            
            filtered_df = pd.DataFrame(filtered_data, columns=['date_time', 'ec_mscm', 'ec_25', 'h_mws', 'h_level', 't_c', 't_intern', 'vbatt', 'waterpoints_id', 'waterpoints_name', 'ref_level', 'sensor_depth'])
            if not filtered_df.empty: 
                insert_query = f'''INSERT INTO {target_dbtable_name} (date_time, ec_mscm, ec_25, h_mws, h_level, t_c, t_intern, vbatt, waterpoints_id, waterpoints_name, ref_level, sensor_depth) {select_query}'''
                cursor.execute(insert_query)
                logger.info(f"From {source_db_table} Data inserted into {target_dbtable_name} successfully.")
            else:
                logger.info(f"No data to insert from {source_db_table} to {target_dbtable_name}.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")        

# function to insert waterlevel table from before max id rows of datameasurement table 
def waterlevel_insert(source_db_table, target_dbtable_name, maxid_before):
    try:
        with DatabaseConnection(db_config) as (conn, cursor):            
            ## filter the DB table based on the condition
            select_data = f'''SELECT date_time, ref_level-sensor_depth+h_level AS water_level, waterpoints_id, waterpoints_name, ec_25, vbatt 
                            FROM {source_db_table} WHERE id > %s''' % maxid_before
            cursor.execute(select_data)
            filtered_data = cursor.fetchall()
            filtered_df = pd.DataFrame(filtered_data, columns=['date_time', 'water_level', 'waterpoints_id', 'waterpoints_name', 'ec_25', 'vbatt'])           
            ## print the contents of filtered_data
            logger.info(f"Filtered Data for {source_db_table}: \n{filtered_df}")
            ## if there is data to insert, perform the insertion
            if not filtered_df.empty:
                insert_query = f'''INSERT INTO {target_dbtable_name} (date_time, water_level, waterpoints_id, waterpoints_name, ec_25, vbatt) {select_data}'''
                cursor.execute(insert_query)
                logger.info(f"From {source_db_table} data inserted into {target_dbtable_name} successfully.")
            else:
                logger.info(f"No data to insert from {source_db_table} to {target_dbtable_name}.")         
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")

# Function to insert longwaterpointlayer table from the join of waterpoints and waterlevel tables
def longtable_join_insert(source_db_table, join_table, target_dbtable_name, maxid_before):
    try:
        with DatabaseConnection(db_config) as (conn, cursor):
            # Filter the DB table based on the condition
            join_query = f'''SELECT {join_table}.name, {join_table}.type, {join_table}.geom, {join_table}.x, {join_table}.y, {source_db_table}.date_time, {source_db_table}.water_level, {source_db_table}.waterpoints_id, {source_db_table}.ec_25, {source_db_table}.vbatt  
                        FROM {join_table} 
                        JOIN {source_db_table} ON {source_db_table}.waterpoints_id = {join_table}.id 
                        WHERE {source_db_table}.id > {maxid_before}'''
            cursor.execute(join_query)
            filtered_data = cursor.fetchall()
            filtered_df = pd.DataFrame(filtered_data, columns = ['waterpoints_name', 'waterpoints_type', 'geom', 'x', 'y', 'date_time', 'water_level', 'waterpoints_id', 'ec_25', 'vbatt'])
            
            # Print the contents of filtered_data
            logger.info(f"Filtered Data for {source_db_table} and {join_table}: \n{filtered_df}")
                        
            # If there is data to insert, perform the insertion
            if not filtered_df.empty:
                insert_query = f'''INSERT INTO {target_dbtable_name} (waterpoints_name, waterpoints_type, geom, x, y, date_time, water_level, waterpoints_id, ec_25, vbatt) {join_query}'''
                cursor.execute(insert_query)
                logger.info(f"From {source_db_table} data has been inserted into {target_dbtable_name} successfully.")
            else:
                logger.info(f"No data to insert from {source_db_table} to {join_table}.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error: {error}")

# Making database table list into variable db_table
db_tables = []
with DatabaseConnection(db_config) as (conn, cursor):
    query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE' 
            AND table_name NOT LIKE 'g%'
            AND table_name NOT LIKE 'static%'
            AND table_name NOT LIKE 'weather%'
            AND table_name NOT LIKE 'de%'
            ORDER BY table_name ASC;
            """
    cursor.execute(query)
    db_tables = [table[0] for table in cursor.fetchall()]
    if 'spatial_ref_sys' in db_tables:
        db_tables.remove('spatial_ref_sys')
    logger.info(f"table name inside database: \n{db_tables}")

def db_update():
    global logger  
    try:
        # locate the location of the source csv table after being downloaded from FTP Server
        desired_folder = 'D:\\repos\\pirnamoonsym\\FTPServer'
        original_folder = os.getcwd()
        
        original_folder = os.path.normpath(original_folder)
        desired_folder = os.path.normpath(desired_folder)
        
        if original_folder != desired_folder:
            os.chdir(desired_folder)
            logger.info("Changed working directory to: %s" % os.getcwd())
                    
        files_name = os.listdir()

        unique = ['ctd123', 'ctd124', 'ctd125', 'ctd126', 'ctd127']

        dataframes = {'ctd123': pd.DataFrame(),
                    'ctd124': pd.DataFrame(),
                    'ctd125': pd.DataFrame(),
                    'ctd126': pd.DataFrame(),
                    'ctd127': pd.DataFrame()}

        # concanate the downloaded csv file that has not been inserted into database table
        # in the form of single data frame for each unique value 
        for i in unique:
            try:
                matching_files = [file for file in files_name if file.endswith('.csv') and fnmatch.fnmatch(file, f'*{i}*.csv')]

                if matching_files:
                    for file in matching_files:
                        filename = os.path.join(desired_folder, file)
                        creation_time = datetime.fromtimestamp(os.path.getctime(filename)).strftime("%Y-%m-%d %H:%M:%S")
                        modification_time = datetime.fromtimestamp(os.path.getmtime(filename))
                        # the limit time depend on when you downloaded the ftp server data
                        # in this case, the interval is 1 day before because the schedule update will be everyday
                        lowerlimit_day = datetime.now() - timedelta(days =31)
                        #upperlimit_day = datetime.now() - timedelta(days=1)

                        if modification_time > lowerlimit_day:
                            print(f'{i} has files name of: {filename} with creation time: {creation_time} and modification time: {modification_time.strftime("%Y-%m-%d %H:%M:%S")}')
                            df = pd.read_csv(filename, sep=';|,|[|]', encoding='unicode_escape', engine='python')
                            dataframes[i] = pd.concat([dataframes[i], df], axis=0, ignore_index=True)
                            logger.info(f'{i} has files name of: {filename} with modification time: {modification_time.strftime("%Y-%m-%d %H:%M:%S")} has been concanated to {i}')
                        else:
                            logger.info(f'{i} has files name of: {filename} - Modification time is less than 48 hours ago and will not be read')
                else:
                    logger.info(f'{i} - No files found')
            except Exception as e:
                logger.error("An error occurred: %s" % e)

        ## Preprocessing for the basic well data frame in unique before inserted it into the database table 
        for i in unique:
            preprocess(dataframes[i])

        # to insert the maxid_before values as a limit for the data insertion in other table
        maxid_before_values = []
        
        with DatabaseConnection(db_config) as (conn, cursor):
            ## Loop through the tables and check the maximum id by unique variable. 
            for table in db_tables:
                try: 
                    query = f"SELECT MAX(id) FROM {table};"
                    cursor.execute(query)
                    maxid_before_values.append(cursor.fetchone()[0])
                    logger.info("Maximum ID for %s: %s" % (table, maxid_before_values[-1]))
                except Exception as e: 
                    logger.error("There is an error with selecting maximum id from table: %s" % table)
            print(maxid_before_values)

        # make last datetime data list
        last_datetime = []

        for table in db_tables[:9]:
            try:
                with DatabaseConnection(db_config) as (conn, cursor): 
                    query = f"SELECT date_time FROM {table} ORDER by date_time DESC LIMIT 1;"
                    cursor.execute(query)
                    result = cursor.fetchone()
                    
                    if result is not None:
                        last_datetime.append(result[0].strftime("%Y-%m-%d %H:%M:%S"))
                        logger.info("Max datetime for table %s : %s" % (table, result[0].strftime("%Y-%m-%d %H:%M:%S")))
                    else:
                        logger.info(f'No data found for table {table}')
            except (Exception, psycopg2.DatabaseError) as error: 
                logger.error("Error: %s" % error) 

        last_datetime = pd.to_datetime(last_datetime)
        
        # Looping of insertion execution
        with DatabaseConnection(db_config) as (conn, cursor):
            for i in unique:
                execute_values(cursor, dataframes[i], f'{i}')

        # Execution of the datameasurement table insertion 
        unique = ['ctd123', 'ctd124', 'ctd125', 'ctd126', 'ctd127']
        target_dbtable_name = db_tables[5]
        maxid_before_number = maxid_before_values[:5]
        
        for source_db_table, maxid_before in zip(unique, maxid_before_number):
            datameasurement_insert(source_db_table, target_dbtable_name, maxid_before)    

        # Execution of the waterlevel table insertion 
        source_db_table = db_tables[5]
        maxid_before_datameasurement = maxid_before_values[5]
        target_dbtable_name = db_tables[8]

        waterlevel_insert(source_db_table, target_dbtable_name, maxid_before_datameasurement)
            
        # pirna_gauge table updating before longwaterpointlayer join table
        river_avghr = pd.read_csv(r'D:\repos\pirnamoonsym\River_PegelOnline\riverpirna_avghour.csv')
        river_avghr['timestamp'] = pd.to_datetime(river_avghr['timestamp'])
        river_avghr['timestamp'] = river_avghr['timestamp'].values.astype(dtype='datetime64[ns]')
        river_avghr = river_avghr.sort_values('timestamp')
        river_avghr.rename(columns={'timestamp':'date_time', 'value' : 'river_level'}, inplace=True)
        
        
        if last_datetime[7] < river_avghr['date_time'].max():
            try:
                with DatabaseConnection(db_config) as (conn, cursor): 
                    cursor = conn.cursor()
                    river_avghr_filtered = river_avghr[(river_avghr['date_time'] > last_datetime[7]) & (river_avghr['date_time'] <= river_avghr['date_time'].max())]
                            
                    execute_values(cursor, river_avghr_filtered, db_tables[7])
                                    
                    logger.info(f"Pirna_gauge has been inserted from {last_datetime[7]} until {river_avghr['date_time'].max()}")
                                        
                    # then, if execute_sucess it will then insert the pirna_gauge data to the waterlevel table 
                    cursor.execute(f"INSERT INTO waterlevel (date_time, water_level, waterpoints_id, waterpoints_name) "
                                    f"SELECT date_time, river_level/100 + ref_level, waterpoints_id, waterpoints_name "
                                    f"FROM pirna_gauge WHERE pirna_gauge.id > {maxid_before_values[7]};")
                    logger.info("Data has been inserted from pirna_gauge table into waterlevel table in the database")
            except (Exception, psycopg2.DatabaseError) as error: 
                logger.error("Error: %s" % error) 
        else: 
            print("Database pirna_gauge last date time is not less than the river data frame")



        ### Execution of the longwaterpointlayer table insertion 
        join_table = db_tables[9]
        source_db_table = db_tables[8]
        maxid_before_longwaterpointlayer = maxid_before_values[8]
        target_dbtable_name = db_tables[6]

        longtable_join_insert(source_db_table, join_table, target_dbtable_name, maxid_before_longwaterpointlayer)

        # generating waterpoints id to use it for the cleaning up the table
        waterpointsid = []

        with DatabaseConnection(db_config) as (conn, cursor):
            query = '''SELECT id FROM waterpoints ORDER BY id;'''
            cursor.execute(query)
            waterpointsid = [id[0] for id in cursor.fetchall()]
            logger.info(f"The id for waterpoints data: \n{waterpointsid}")
        
        
        # check duplicates and if duplicate data exist, delete the duplicate 
        # using a context manager for the connection
        with DatabaseConnection(db_config) as (conn, cursor):
            for table in db_tables[:9]:
                for id in waterpointsid[:6]:
                    try:
                        ## detection of duplicate data: 
                        duplicate_query = f"SELECT date_time, COUNT(date_time) FROM {table} WHERE waterpoints_id = %s GROUP BY date_time HAVING COUNT(date_time)>1;"
                        cursor.execute(duplicate_query, (id,))
                        duplicate = cursor.fetchall()

                        ## delete the duplicate data if the duplication detection is not empty
                        if duplicate and duplicate[0][1] > 1:
                            logger.info(f"There is duplicate data in {table} with id {id}")
                            delete_query = f'''
                                            DELETE FROM {table} pd
                                            WHERE pd.waterpoints_id = %s
                                            AND pd.id IN (
                                                SELECT id
                                                FROM (
                                                SELECT id,
                                                        ROW_NUMBER() OVER (PARTITION BY date_time ORDER BY id) AS row_num
                                                FROM {table}
                                                WHERE waterpoints_id = %s
                                                ) subquery
                                                WHERE row_num > 1
                                            );'''
                            cursor.execute(delete_query, (id,id))
                            logger.info(f"Delete duplicate successful for {table} with id {id}")
                        else:
                            logger.info(f"There is no data duplication in {table} with id {id}")
                    except psycopg2.Error as e:
                        logger.error(f"Error: {e}")
                try:
                    # update the sequence: 
                    ## restart the sequence:
                    #cursor.execute(f'''ALTER TABLE {table} REPLICA IDENTITY FULL;''')
                    cursor.execute(f'''ALTER SEQUENCE {table}_id_seq RESTART WITH 1;''')
                    
                    ## update the id with the default sequence that already restart with 1
                    update_id = f'''WITH cte AS (
                        SELECT id, ROW_NUMBER() OVER (ORDER BY id) as new_id
                        FROM {table}
                        )
                        UPDATE {table}
                        SET id = cte.new_id
                        FROM cte
                        WHERE {table}.id = cte.id;'''
                    cursor.execute(update_id)
                    logger.info(f"The id in table {table} has been updated")
                    
                    # Get the count of the remaining row after deleting the duplicates 
                    # and set the max(id) as the start of the sequence  
                    resequence = f'''SELECT setval('{table}_id_seq', (SELECT MAX(id) FROM {table}));'''
                    cursor.execute(resequence)
                    logger.info(f"Sequence alteration successful for table {table}")
                except psycopg2.Error as e:
                        logger.error(f"Error: {e}")
        
        
        # checking the database maximum id after data frame insertion 
        maxid_after_values = []
        with DatabaseConnection(db_config) as (conn, cursor):
            ## Loop through the tables and check the maximum id by unique variable. 
            for table in db_tables:
                query = f"SELECT MAX(id), COUNT(*) FROM {table};"
                cursor.execute(query)
                maxid_after_values.append(cursor.fetchone()[0])
                    
                logger.info("Maximum ID after cleaning up for %s: %s" % (table, maxid_after_values))
        
        # back up the database table to csv table 
        for table in db_tables:
            try: 
                with DatabaseConnection(db_config) as (conn, cursor):
                    query = f'''COPY {table} TO 'D:/repos/pirnamoonsym/data_table/{table}.csv' CSV HEADER;'''
                    cursor.execute(query)
                    logger.info(f"Back up csv file for database table {table} successfull")
            except psycopg2.Error as e:
                logger.error(f"Error: {e}")
        
        # end of the update execution
        print("Finish updating. Please check the log file.")
        logger.info(f"Database update executed at {datetime.now()}")
    
    except Exception as e:
        if logger:
            logger.error("An error occurred: %s" % e)
        else:
            print("An error occurred: %s" % e)
    finally:
        # Restore the original working directory
        os.chdir(original_folder)
              

# File path of where the output file is
output_file_path = f'D:\\repos\\pirnamoonsym\\updating_report\\db_update_report_{datetime.now().strftime("%Y%m%d %H-%M-%S")}.log'



# variable to store original stdout
original_stdout = sys.stdout

def main():
    try:
        # Open the file in write mode and redirect stdout to the file
        with open(output_file_path, 'w') as f:
            sys.stdout = f

            # Set up a schedule to run the job every 1 day
            schedule.every(1).day.at("19:30").do(db_update)
            
            print("Script is running")  # Add this line for debugging

            while True:
                schedule.run_pending()
                time.sleep(1)

    finally:
        # restore stdout to its original state
        sys.stdout = original_stdout

if __name__ == "__main__":
    main()
    
    
