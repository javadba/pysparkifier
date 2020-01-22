from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql import Row, Column, DataFrame as SDataFrame
from pyspark.sql.types import *
import pandas as pd
import os, sys
from collections import namedtuple as ntup

__all__ = 'p setupSpark getSpark describeWithNulls toPandas sparkSql fromCsvSpark cols'.split()

def p(msg,o=None): omsg = ": %s" %repr(o) if o is not None else ""; print("%s%s\n" %(msg, omsg))

def setupSpark():
    os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
    pd.set_option('display.max_rows', 20)
    pd.set_option('display.max_colwidth', -1)
    pd.set_option('display.max_columns', None)
    pd.set_option('expand_frame_repr', False)
    spark = SparkSession.builder.appName("pysparkifier").master("local").getOrCreate()
    return spark
    
# # Start the Apache Spark server
#
def getSpark():
    spark = spark if 'spark' in globals() else setupSpark()
    return spark

def describeWithNulls(df, doPrint=True):
    # df = pd.DataFrame({ 'a': [1,2,3], 'b': ['a','b','c'], 'c': [99.5,11.2, 433.1], 'd':[123,'abc',None]})
    desc = df.describe()  # Returns a DataFrame with stats in the row index
    combo = pd.concat([df.isna().sum(),desc.T],axis=1).set_axis(['Nulls']+list(desc.index),axis=1,inplace=False)
    if doPrint:
        p(combo.head(100))
    return combo    

# Read a Parquet File into Spark DataFRame and also create Pandas Dataframe
#
def toPandas(path, tname, sql=None, count=False):
    df = getSpark().read.parquet(path)
    if count: p(tname + ": count="+str(df.count()))
    df.createOrReplaceTempView(tname)
    if sql is not None:
        df = getSpark().sql(sql)
        df.createOrReplaceTempView(tname)
    pdf = df.toPandas()
    describeWithNulls(pdf)
    return df,pdf

#  Run a Spark SQL and return pandas and spark dataframes
#
def sparkSql(sql, tname=None, count=True, describe=False):
    sdf = getSpark().sql(sql)
    if count and tname: p(tname + ": count="+str(sdf.count()))
    if tname:    sdf.createOrReplaceTempView(tname)
    pdf = sdf.toPandas()
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
      print(pdf)
    if describe:
      describeWithNulls(pdf)
    return sdf,pdf

#  Read a CSV and create pandas and spark dataframes
#
def fromCsvSpark(path, tname, header=True, count=True, sql=None):
    p('Reading from %s..' %path)
    df = getSpark().read.csv(path,header=header)
    if count: p(tname + ": count="+str(df.count()))
    df.createOrReplaceTempView(tname)
    if sql is not None:
        df = getSpark().sql(sql)
        df.createOrReplaceTempView(tname)
    pdf = df.toPandas()
    describeWithNulls(pdf)
    return df,pdf

# Return selected columns of pandas DF
#
def cols(df, cnames):
    cnames = cnames if isinstance(cnames,list) else [cnames]
    return df.loc[:,cnames]

# Tokenization Spark UDF using comma delimiter
#
def tokenize(txt):
    def stripx(x): return x.strip()
    if txt.find(',') < 0: return txt
    else:
        toks = list(map(stripx, txt.split(',')))
        # return [toks[0], toks]
        return toks

def _keep_me_private():
    print("should NOT be exposed when doing `from pysparkifier import *`")
    
if __name__ == '__main__':
    sampleCode="""
    from pyspark.sql.types import *
    from pyspark.sql.functions import udf
    tokenize_udf = udf(tokenize, ArrayType(StructType([StructField("tok", StringType(), False)])))
    spark.udf.register("tokenize",tokenize_udf)
    """