"""
    Quality check module
    Clean / validate : address, name, location, phone, votes, reviews_list

    * clean(df:DataFrame) -> DataFrame : clean dataframe

    * validate_location(location: DataFrame, areas: DataFrame) -> DataFrame:
    * clean_descriptive(text: str) -> str: clean special / junk chars from text
    * clean_phone(numbers: str) -> str: clean phone numbers    
    * validate_votes(votes: str) -> int: validate if votes is numeric
"""
import re
import pandas as pd
from unicodedata import normalize
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, StringType, udf, split
from pyspark.sql.session import SparkSession


def clean(df: DataFrame) -> DataFrame:
    """
        Apply cleanning 

        Params
        * df: DataFrame
    
        Return cleaned df
    """    
    clean_d = udf(lambda z: clean_descriptive(z), StringType()) 

    df = df.withColumn("address", clean_d(col("address")))
    df = df.withColumn("reviews_list", clean_d(col("reviews_list")))
    df = df.withColumn("name", clean_d(col("name")))

    # Apply phone transformations
    clean_p = udf(lambda z: clean_phone(z), StringType()) 
    df = df.withColumn("temp_phone", clean_p(col('phone')))

    split_col = split(df['temp_phone'], ' ')

    df = df.withColumn("contact_number_1", split_col.getItem(0))
    df = df.withColumn("contact_number_2", split_col.getItem(1))
    
    # Clean votes
    clean_n = udf(lambda z: validate_votes(z), StringType()) 
    df = df.withColumn("votes", clean_n(col('votes')))
    
    return df
    
def validate_location(loca_area: DataFrame, areas: DataFrame) -> DataFrame:
    """
        Return a new dataframe with area and area from master table

        Params
        * location: source dataframe
        * areas: master data dataframe\n

        Return: new dataframe to validate nulls at area_master column
    """        
    return loca_area.join(areas, loca_area.location == areas.area_master, how="left")
    

def clean_descriptive(text: str) -> str:
    """
        Remove especial/junk characters from the text.
        As POSIX says, ( ) are special chars too
        
        Params
        * text: text to clean
        
        Return: cleaned text
    """
    # Normalize to ascii
    s1 = text.replace("ñ", "n").replace("Ñ", "N")
    s1 = re.sub("\\\\n", " ",s1)
    s2 = normalize("NFKD", s1).encode("ascii","ignore").decode("ascii")
    
    # Replace special chars including ( )
    s2=re.sub("[^(\w|\d|\s)]|\?|x(\d){2}(A|E|I|O|U)","", s2)  
    return re.sub("(A|E|I|O|U){2,20}|(A|E|I|O|U)x(\d){2}(A|E|I|O|U)","",s2)

def clean_phone(numbers: str) -> str:
    """
        Clean and divide phone number
        remove + or spaces for the first\n
        remove firt char if 0 from the country code\n
        split if possible in 2 or more contact numbers\n
        Ref. E. 164 formatting.

        Params
        * numbers: to clean
        
        Return: white space separated list of numbers
    """
    if not numbers:
         return ","

    bad_code = False
    
    #num_tokens = numbers.split(" ")
    num_tokens  = re.split(r'\s|\n|\r',numbers)
    num_tokens = [x for x in num_tokens if x!=""]
    result = []
    counter = 0
   
    for i in num_tokens:
        if len(i)==0:
            continue
        if counter%2==0:
            code = "".join([x for x in i if x!="+"])
            if code == "":
                bad_code = True
                continue
            elif code.startswith("0"):
                code=code[1:] 
                if len(code)==0:
                    bad_code = True
                else:    
                    bad_code = False
            else:
                bad_code = False
        else:
            aux = code+i
            if aux.isnumeric() and bad_code==False:
                result.append(aux)
            code = ""
        counter+=1

    res = ""

    for i in result:
        "".join([x for x in i if x.isnumeric()])

    if len(result)==0:
        res = " "
    elif len(result)==1:
        res = result[0] + " "
    else:
        res = result[0] + " " + result[1]
    return res

def validate_votes(votes: str) -> str:
    """
        Validate that votes are int
        if not, return -1
    """
    try:
        return str(int(votes)) if votes.isnumeric() else str(-1)
    except:
        return str(-1)
