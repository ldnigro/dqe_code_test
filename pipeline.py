# Workarrorund import modules in VSCODE
import importlib
importlib.invalidate_caches()
import contextlib
import sys
import os
sys.path.append("./spark_test")

# --- END Workarround -----------------

from pyspark.sql.types import StringType
from pyspark.sql import * 
from pyspark.sql import DataFrame

import config as cfg
import file_check as fchk
import quality_check as qchk
import input
import datetime
import data_base as data
import output as out
import logging
import time

# Reload imports
importlib.reload(cfg)
importlib.reload(fchk)
importlib.reload(qchk)
importlib.reload(input)
importlib.reload(data)
importlib.reload(out)

def main(argv):

    logging.basicConfig(filename='pipe_line.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')

    conf_file=""

    if len(argv)==1 or argv[1]=="-h":
        print('\nUsage: pipeline.py <config-file>\n')
        sys.exit(2)
    
    else:
        conf_file=argv[1]
        
    with contextlib.redirect_stdout(None):
        pass

    
    ## Main Loop
    while True:
    
        # Read Configuration
        config = cfg.Config(conf_file)
        input_signal = config.get_signal()
        input_path = config.get_input_path()

        if input_signal != 'RUN':
            break

        # Data Base
        conn = data.connect(config.get_dbpath(), config.get_dbname())
        data.create_support_database(conn)
        
        # Process only once a day
        actual_date = datetime.date.today().strftime("%Y-%m-%d")
        prev_date = data.get_last_date(conn)
        while prev_date!=None and (prev_date == actual_date):
              time.sleep(600) # 10 min check
              actual_date = datetime.date.today().strftime("%Y-%m-%d")
        
        if prev_date==None:
              data.insert_process_date(conn,actual_date)
        else:
              data.update_process_date(conn,prev_date, actual_date)
              prev_date = actual_date

        logging.info("Start process " + actual_date + "." )

        # Init Spark session
        #spark_session.sparkContext.addPyFile(os.path.abspath("./spark_test") + "/quality_check.py")
        spark_session = data.init_spark()
        spark_session.sparkContext.addPyFile(os.path.abspath(".") + "/quality_check.py")

        # Create spark areas dataframe
        df_areas = input.read_areas(spark_session, config.get_area_path(),config.get_area_filename())

        # Files to process
        file_list = fchk.get_files_from_location(input_path, config.get_extension())
        
        for file in file_list:

            # Perform file check
            status, error =  fchk.check_file_constraints(input_path, file ,config.get_prefix(),config.get_extension())
    
            if status==False:
              logging.error("Error [" + error + "] processing: " + file )
              continue

            # File is ok to procdess 
            if status and data.can_process(conn, file):
        
                logging.info("Start process: " + file )

                start_time = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
        
                # Insert start process time
                data.insert_start_processed(conn, start_time, file)
        
                # Read input file
                df = input.read(input_path, file, spark_session)
       
                # Clean
                df = qchk.clean(df)
       
                # Validate location
                df = qchk.validate_location(df, df_areas)
        
                # Write bad & metadata
                df_bad = out.write_bad(spark_session, config, df, file)

                # Write clean
                out.write_clean(spark_session, config, df, df_bad, file)

                # Escribir fin de proceso
                end_time = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
                data.update_end_processed(spark_session,end_time, file)

                logging.info("End process: " + file )
            
            else:
                logging.error("Error [ processed ] : " + file )
    
        # Free resources 
        conn.close()
        data.stop_spark(spark_session)
        logging.info("Finish process " + actual_date + "." )

    logging.info("Signal to finish process..." )
    
    try:
        logging.info("Close Database connection..." )
        conn.close()  
    except:
        pass
    try:
        logging.info("Close Spark connection..." )
        data.stop_spark(spark_session)
    except:
        pass


if __name__ == "__main__":

    main( sys.argv )