from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark_sc=SparkSession.Builder().appName('Ex01').master('local[*]').getOrCreate()