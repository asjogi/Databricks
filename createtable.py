from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark_sc=SparkSession.Builder().appName('Ex01').master('local[*]').getOrCreate()
#Read data from Databricks location
read_data=spark_sc.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/5000_Sales_Records.csv')
#Dropping columns 
read_filter = read_data.drop('Region', 'Country', 'Order Priority', 'Ship Date', 'Total Profit', 'Total Cost', 'Unit Cost')
#Renaming the columns and removing spaces
df_1 = read_filter.withColumnRenamed('Order Id','order_id') \
.withColumnRenamed('Item Type','item_type') \
.withColumnRenamed('Sales Channel','sales_channel') \
.withColumnRenamed('Order Date','order_date') \
.withColumnRenamed('Units Sold','units_sold') \
.withColumnRenamed('Unit Price','unit_price') \
.withColumnRenamed('Total Revenue','total_revenue')
#Altering data types for few columns
df_2 = df_1.withColumn('units_sold',col('units_sold').cast('float')) \
.withColumn('unit_price',col('unit_price').cast('float')) \
.withColumn('total_revenue',col('total_revenue').cast('float')) \
.withColumn('order_date',to_date(col('order_date'), 'M/d/yyyy'))
#Repartition and saving to table
df_2.repartition(3).write.mode('overwrite').saveAsTable('Table_Sales')