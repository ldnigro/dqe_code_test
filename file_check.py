"""
    File check module
    Check file constraints

    * check_file_constraints(location_path: str, filename: str, prefix: str, ext: str) -> Tuple[bool, str]:

    * get_files_from_location(location: str, ext="csv") -> list:
    * get_file_date(filename: str,prefix : str) -> str:    
    * is_valid_date(filedate: str) -> bool:
    * can_process(spark: SparkSession, filename: str) -> bool:

"""

import os
import datetime
from typing import Tuple
from pyspark.sql import SparkSession

def get_files_from_location(location: str, ext="csv") -> list:
    """
        Get a list of files from given path and extension

        Params:
        * location: path where the files are
        * ext: file extensins\n
        return: list of filenames or None if error \n
        ex: get_files_from_location('/path/', '.csv')
    """
    try:
        file_list = [x for x in os.listdir(location) if x.lower().endswith("." + ext.lower())]
        return file_list    
    except:
        return None

def get_file_date(filename: str,prefix : str) -> str:
    """
        Get file date from a filename

        Params:
        * filename: filename without path
        * prefix: filename prefix\n

        * return: String with filename or '' on error\n
        ex: get_files_date('data_file_20210527182730.csv', 'data_file_')
    """
    try:
        return filename[len(prefix):filename.index(".")]
    except:
        return ""

# Check valid date
def is_valid_date(filedate: str) -> bool:
    """
        Check if a string is a valid date

        Params:
        * filename: filename without path
        * prefix: filename prefix
        
        Return: True valid or False invalid date

        ex: get_files_date('20210527182730'')
    """
    res = True
    try:
        y = int(filedate[0:4])
        m = int(filedate[4:6])
        d = int(filedate[6:8])
        h = int(filedate[8:10])
        min = int(filedate[10:12])
        sec = int(filedate[12:14])
        datetime.datetime(y,m,d,h,min,sec)
    except:
        res = False    
    finally:
        return res

def check_file_constraints(location_path: str, filename: str, prefix: str, ext: str) -> Tuple[bool, str]:
    """
        Check file name constraints
        extension, prefix, valid date, if it is a file,\n
        empty / size, if it was processed
        
        Params:
        * location_path: path
        * filename: filename without path
        * prefix: filename prefix
        * ext: filename extension
        
        Return: tuple (True, 'pass') or (Fale, 'error')\n  

        ex: get_files_date('/path/','data_file_20210527182730.csv','data_file_','csv')
    """

    status = False
    msg = "pass"
    file = location_path + filename
    
    try:        
        #Check extension
        if (not filename.endswith("."+ext)):
            raise(Exception("%s bad file extension."%(filename)))

        # Check if prefix is correct
        if (not filename.startswith(prefix)):   
            raise(Exception("%s bad prefix."%(filename)))    

        # Check if correct date
        if (not is_valid_date(get_file_date(filename,prefix))):
            raise(Exception("%s bad date."%(filename)))    

        # Check if it is a file
        if (not os.path.isfile(file)):
            raise(Exception("%s is not a file." %(filename)))
        
        # Check size 
        if (os.path.getsize(location_path + filename)==0):
            raise(Exception("%s size is 0." %(filename)))
        
        # Check if is Empty (only header)
        with  open(file,"r") as fread:
            out = fread.readlines(1000)
            if(len(out)<2):
                raise(Exception("%s is empty file."%(filename)))

        # Pass
        status = True

    except (Exception) as error:
        msg = error.args[0]
    finally:
        return status, msg
