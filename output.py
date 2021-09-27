"""
    Output Module
    
    * write_bad: records to a .bad file and errors to a .meta file
    * write_clean: write clean records to a .out file

"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.session import SparkSession
from config  import Config
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import pandas as pd
import csv

def write_bad(spark_session: SparkSession, config: Config, dataframe: DataFrame, filename: str) -> DataFrame:
    """
        Writes bad file

        Params
        * spark_session
        * config
        * dataframe
        * filename

        Return DataFrame
    """
    data=[]
    area = False
    nules = False
    vot = False

    file = config.get_output_path() + filename + ".bad"
    pds = []
    # Bad Area Records
    df_area_bad = dataframe.filter(col('area_master').isNull())
    if df_area_bad.count() > 0:
        df_area_bad.drop("area_master")
        pds.append(df_area_bad.toPandas())
        area = True
    else:
        df_area_bad.drop("area_master")

    # Null Records
    df_out_bad = dataframe.filter(
        col("name").isNull() |
        col("name").isin(['']) | 
        col('location').isNull() |
        col('location').isin(['']) | 
        col('phone').isNull()  |
        col('phone').isin([''])|
        col('contact_number_1').isNull()|
        col('contact_number_1').isin(['']))
    
    if df_out_bad.count() > 0:
        pds.append(df_out_bad.toPandas())
        nules = True
    
    df_votes_bad = dataframe.filter(col("votes").isin(["-1"]))
    if df_votes_bad.count() > 0:
        pds.append(df_votes_bad.toPandas())
        vot = True
    
    if area or nules or vot:
        pds_df = pd.concat(pds)
        del pds_df['area_master']
        pds_df = pds_df.drop_duplicates()
        
        pds_df = pds_df[['row_number','url','address','name','rate','votes','contact_number_1','contact_number_2',\
            'location','rest_type','dish_liked','cuisines','reviews_list']]

        pds_df['row_number'] = pds_df['row_number'].astype('int')
        pds_df.sort_values('row_number',ascending=True, inplace=True )

        pds_df.to_csv(file,sep=",", header=True, index=False, quoting=csv.QUOTE_ALL)
        
        # Metadata
        file = config.get_output_path() + filename + ".meta"
    
        # Get Metadata
    
        #  number of null records
        data = []
        if nules:
            numbers = list(df_out_bad.select('row_number').toPandas()['row_number'])
            numbers.sort(key=int)
            data.append(("null","[" + ",".join(str(x) for x in numbers) + "]"))
    
        # number of bad area records
        if area:
            numbers = list(df_area_bad.select('row_number').toPandas()['row_number'])
            numbers.sort(key=int)
            data.append(("bad_area","[" + ",".join(str(x) for x in numbers) + "]" ))
    
        # Number of bad votes redords
        if vot:
            numbers = list(df_votes_bad.select('row_number').toPandas()['row_number'])
            numbers.sort(key=int)
            data.append( ("not_int_votes","[" + ",".join(str(x) for x in numbers) + "]" ))
        
        if len(data)>0:
            pd_bad= pd.DataFrame(data, columns=["type_of_issue","row_num_list"])
            pd_bad.to_csv(file , sep=",", index=False)

        return spark_session.createDataFrame(pds_df)

def write_clean(spark_session: SparkSession, config: Config, dataframe: DataFrame, df_bad: DataFrame, filename: str):
    """
        Writes clean records

        Params
        * spark_session
        * config
        * dataframe
        * filename

    """
    
    file = config.get_output_path() + filename + ".out"
    
    df_out_clean = dataframe.join(df_bad,"row_number",how="leftanti")
    df_out_clean = df_out_clean.sort(df_out_clean.row_number.asc())
    
    pds_df = df_out_clean.toPandas()
    pds_df = pds_df[['row_number','url','address','name','rate','votes','contact_number_1','contact_number_2',\
        'location','rest_type','dish_liked','cuisines','reviews_list']]

    pds_df['row_number'] = pds_df['row_number'].astype('int')
    pds_df.sort_values('row_number',ascending=True, inplace=True)
    pds_df.to_csv(file,sep=",", header=True, index=False, quoting=csv.QUOTE_ALL)
