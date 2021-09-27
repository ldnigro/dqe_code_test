"""
    Read information from input
    * read(filepath, filename, spark_session) -> DataFrame : Read original files
    * read_areas(spark_session: SparkSession, areas_path: str, areas_file: str) -> DataFrame: from .xlsx 
"""

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession 
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, StructField, LongType, TimestampType, StructType
import pandas as pd

def equivalent_type(f):
    """
        Convert types to spark equivalent
    """
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    """
        Create StructField with name and typo
    """
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(spark_session, pandas_df):
    """
        Covert pandas to spark with a list of columns names and types
    """
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark_session.createDataFrame(pandas_df, p_schema)


def read(filepath, filename, spark_session) -> DataFrame:
    """
        Read file from source

        Params
        * filepath
        * filename
        * spark_session

        Return: Spark DataFrame
    
    """
    pdas = pd.read_csv(filepath + filename,sep=",", quotechar='"', low_memory=False, dtype='str')
    pdas['row_number']=range(0,len(pdas))
    pdas['row_number'] = pdas['row_number'].astype('str')

    del pdas["approx_cost(for two people)"] 
    del pdas["menu_item"] 
    del pdas["listed_in(type)"]
    del pdas["listed_in(city)"]
    del pdas["online_order"]
    del pdas["book_table"]
    
    return  pandas_to_spark(spark_session, pdas)

def read_areas(spark_session: SparkSession, areas_path: str, areas_file: str) -> DataFrame:
    """
        Read Areas file, only area column
        
        Paramas
        * spark_session: spark_session
        * areas_path: path for the file
        * areas_file: filename
        
        Return pandas dataframe
    """
    pddf=pd.read_excel(areas_path + areas_file,usecols="A")
    pddf.rename(columns = {'Area':'area_master'}, inplace = True)
    spdf = spark_session.createDataFrame(pddf)
    return spdf
