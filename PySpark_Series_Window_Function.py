# Databricks notebook source
data = [("Alice", 10), ("Bob", 10), ("Bob", 20),  

        ("Alice", 30), ("Bob", 20), ("Alice", 50),  

        ("Bob", 40), ("Bob", 50)] 

columns = ["Name", "Value"] 

 # Create DataFrame 

df = spark.createDataFrame(data, columns) 

# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,rank,row number and dense rank
from pyspark.sql import functions as F 
from pyspark.sql.window import Window 


# COMMAND ----------

#window specifiction
windowSpec = Window.partitionBy("Name").orderBy(F.desc("Value")) 

# COMMAND ----------

df_with_ranking = df.withColumn("row_number", F.row_number().over(windowSpec))\
                    .withColumn("rank", F.rank().over(windowSpec))\
                    .withColumn("dense_rank", F.dense_rank().over(windowSpec))

  

df_with_ranking.show() 

# COMMAND ----------

# DBTITLE 1,RANK
df.show()

# COMMAND ----------

#import modules
from pyspark.sql.window import Window
from pyspark.sql import funtions as F

# COMMAND ----------

#define window specification
windowSpec1=Window.partitionBy("Name").orderBy("Value")

# COMMAND ----------

# DBTITLE 1,RANK
df_with_rank=df.withColumn("rank",F.rank().over(windowSpec1))
df_with_rank.show()

# COMMAND ----------

# DBTITLE 1,ROW NUMBER
df_with_row_number=df.withColumn("rank",F.row_number().over(windowSpec1))
df_with_row_number.show()

# COMMAND ----------

# DBTITLE 1,DESNSE RANK
df_with_row_number=df.withColumn("rank",F.dense_rank().over(windowSpec1))
df_with_row_number.show()

# COMMAND ----------

# DBTITLE 1,LAG
data = [("Alice", 10), ("Bob", 10), ("Bob", 20),  

        ("Alice", 30), ("Bob", 20), ("Alice", 50),  

        ("Bob", 40), ("Bob", 50)] 

columns = ["Name", "Value"] 

 

# Create DataFrame 

df = spark.createDataFrame(data, columns) 

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import functions as F 

# COMMAND ----------

windowSpec = Window.partitionBy("Name").orderBy(F.desc("Value")) 

# COMMAND ----------

# Apply lag function to get the previous row's 'Value' 

df_with_lag = df.withColumn("lag_1", F.lag("Value", 1).over(windowSpec)) 

df_with_lag.show() 

# COMMAND ----------

# Apply lag function to get the previous row's 'Value' 

df_with_lag = df.withColumn("lag_1", F.lag("Value", 2).over(windowSpec)) 

df_with_lag.show() 

# COMMAND ----------

# DBTITLE 1,LEAD
data = [("Alice", 10), ("Bob", 10), ("Bob", 20),  

        ("Alice", 30), ("Bob", 20), ("Alice", 50),  

        ("Bob", 40), ("Bob", 50)] 

columns = ["Name", "Value"] 

 

# Create DataFrame 

df = spark.createDataFrame(data, columns) 

# COMMAND ----------

df.display()

# COMMAND ----------

#We will specifiy the window
windowSpec = Window.partitionBy("Name").orderBy(F.desc("Value")) 


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Apply lag function to get the previous row's 'Value' 

df_with_lead = df.withColumn("lead_1", F.lead("Value", 1).over(windowSpec)) 

df_with_lead.show() 

# COMMAND ----------

# Apply lag function to get the previous row's 'Value' 

df_with_lead = df.withColumn("lead_1", F.lead("Value", 2).over(windowSpec)) 

df_with_lead.show() 

# COMMAND ----------

data = [("Alice", 10), ("Bob", 10), ("Bob", 20),  

        ("Alice", 30), ("Bob", 20), ("Alice", 50),  

        ("Bob", 40), ("Bob", 50)] 

columns = ["Name", "Value"] 

 

# Create DataFrame 

df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql import functions as F 
from pyspark.sql.window import Window 

# COMMAND ----------

windowSpec = Window.partitionBy("Name").orderBy("Value")

# COMMAND ----------

# Apply first function to get the first 'Value' in the partition 

df_with_first = df.withColumn("first_value", F.first("Value").over(windowSpec)) 

df_with_first.show() 

# COMMAND ----------

# Apply first function to get the first 'Value' in the partition 

df_with_first = df.withColumn("first_value", F.first("Value").over(windowSpec)) 

df_with_first.show() 

# COMMAND ----------

# Apply last function to get the last 'Value' in the partition 

df_with_last = df.withColumn("last_value", F.last("Value").over(windowSpec)) 

df_with_last.show() 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample data
data = [("Alice", 10), ("Bob", 10), ("Bob", 20),  
        ("Alice", 30), ("Bob", 20), ("Alice", 50),  
        ("Bob", 40), ("Bob", 50)] 

columns = ["Name", "Value"] 

# Create DataFrame 
df = spark.createDataFrame(data, columns)

# Define window specification
#windowSpec = Window.partitionBy("Name").orderBy("Value").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
windowSpec = Window.partitionBy("Name").orderBy("Value")

# Apply last function to get the last 'Value' in the partition 
df_with_last = df.withColumn("last_value", F.last("Value").over(windowSpec)) 

df_with_last.show()


# COMMAND ----------

# DBTITLE 1,Dataframe Creation
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

# Sample data
data = [
    ("A", 1),
    ("A", 2),
    ("A", 3),
    ("A", 4),
    ("B", 1),
    ("B", 2),
    ("B", 3)
]
df = spark.createDataFrame(data, ["group", "value"])

# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,rowsBetween(-1, 1)
# Define a window partitioned by group and ordered by value
windowSpec = Window.partitionBy("group").orderBy("value").rowsBetween(-1, 1)

# Add rolling sum over the window frame
df_with_sum = df.withColumn("rolling_sum", sum("value").over(windowSpec))

df_with_sum.show()


# COMMAND ----------

# DBTITLE 1,rowsBetween(Window.unboundedPreceding, Window.currentRow)
# Define window: partition by group, order by value, cumulative till current row
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("group").orderBy("value") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add cumulative sum column
df_with_cumsum = df.withColumn("cumulative_sum", sum("value").over(windowSpec))

df_with_cumsum.show()


# COMMAND ----------

# DBTITLE 1,rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
# Define window: partition by group, order by value, cumulative till current row
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("group").orderBy("value") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Add cumulative sum column
df_with_cumsum = df.withColumn("cumulative_sum", sum("value").over(windowSpec))

df_with_cumsum.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_spec = Window.partitionBy("group_column").orderBy("order_column") \
    .rangeBetween(start, end)


# COMMAND ----------

data = [
    ("A", 1),
    ("A", 2),
    ("A", 3),
    ("A", 4),
    ("B", 1),
    ("B", 3),
    ("B", 5)
]
df = spark.createDataFrame(data, ["group", "value"])


# COMMAND ----------

data = [
    ("A", 1),
    ("A", 2),
    ("A", 3),
    ("A", 4),
    ("B", 1),
    ("B", 2),
    ("B", 3)
]
df1 = spark.createDataFrame(data, ["group", "value"])


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

window_spec = Window.partitionBy("group").orderBy("value") \
    .rangeBetween(-1, 1)

df.withColumn("range_sum", sum("value").over(window_spec)).show()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

window_spec = Window.partitionBy("group").orderBy("value") \
    .rangeBetween(-1, 1)

df1.withColumn("range_sum", sum("value").over(window_spec)).show()


# COMMAND ----------

data = [
    ("A", "2024-01-01", 10),
    ("A", "2024-01-02", 20),
    ("A", "2024-01-04", 30),
    ("A", "2024-01-06", 40),
]


# COMMAND ----------

from pyspark.sql.functions import col, sum, to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp
df = spark.createDataFrame(data, ["group", "date", "sales"]) \
         .withColumn("date", to_date("date"))

# 1-day before to 1-day after
window_spec = Window.partitionBy("group").orderBy(unix_timestamp("date")) \
    .rangeBetween(-86400, 86400)  # 1 day before and after

df.withColumn("rolling_sum", sum("sales").over(window_spec)).show()



# COMMAND ----------

# DBTITLE 1,Example2
data = [
    ("Alice", 1000),
    ("Bob", 1100),
    ("Carol", 1200),
    ("David", 2000),
    ("Eve", 2100),
]


# COMMAND ----------

df = spark.createDataFrame(data, ["name", "salary"])

# Window: within ±100 of current salary
window_spec = Window.orderBy(col("salary").cast("long")).rangeBetween(-100, 100)

df.withColumn("peer_band_salary", sum("salary").over(window_spec)).show()


# COMMAND ----------

# DBTITLE 1,Example3
data = [
    ("2024-01-01", 100),
    ("2024-01-02", 101),
    ("2024-01-03", 103),
    ("2024-01-04", 108),
    ("2024-01-05", 110),
]


# COMMAND ----------

df = spark.createDataFrame(data, ["date", "price"])

window_spec = Window.orderBy("price").rangeBetween(-5, 5)  # price range

df.withColumn("price_window_avg", sum("price").over(window_spec)).show()

#Use case: average stock price in a price band.

# COMMAND ----------

# DBTITLE 1,Numeric Example
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

data = [("A", 1), ("A", 2), ("A", 3), ("A", 4), ("B", 1), ("B", 3), ("B", 5)]
df = spark.createDataFrame(data, ["group", "value"])


# COMMAND ----------

window_spec = Window.partitionBy("group").orderBy("value").rangeBetween(-1, 1)
df.withColumn("range_sum", sum("value").over(window_spec)).show()

# COMMAND ----------

# DBTITLE 1,TimeStamp Example
from pyspark.sql.functions import col, sum, to_timestamp, unix_timestamp
# Sample data
data = [
    ("A", "2024-06-01 10:00:00", 10),
    ("A", "2024-06-02 10:00:00", 20),
    ("A", "2024-06-03 10:00:00", 30),
    ("A", "2024-06-04 10:00:00", 40),
    ("B", "2024-06-02 10:00:00", 20),
    ("B", "2024-06-03 10:00:00", 30),
    ("B", "2024-06-04 10:00:00", 40)
]

df = spark.createDataFrame(data, ["group", "ts", "value"]) \
    .withColumn("ts", to_timestamp("ts")) \
    .withColumn("ts_long", unix_timestamp("ts") * 1000)  # convert to milliseconds
df.show()


# COMMAND ----------

# One day in milliseconds
ONE_DAY_MS = 86400000

from pyspark.sql.window import Window

window_spec = Window.partitionBy("group") \
    .orderBy("ts_long") \
    .rangeBetween(-ONE_DAY_MS, ONE_DAY_MS)

df.withColumn("range_sum", sum("value").over(window_spec)).show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, to_timestamp, unix_timestamp

# Sample data ​

data = [
    ("A", "2024-06-01 10:00:00", 10),
    ("A", "2024-06-02 10:00:00", 20),
    ("A", "2024-06-03 10:00:00", 30),
    ("A", "2024-06-04 10:00:00", 40),
    ("B", "2024-06-02 10:00:00", 20),
    ("B", "2024-06-03 10:00:00", 30),
    ("B", "2024-06-04 10:00:00", 40)]

# COMMAND ----------

df = spark.createDataFrame(data, ["group", "ts", "value"]) \
.withColumn("ts", to_timestamp("ts")) \
.withColumn("ts_long", unix_timestamp("ts") * 1000)  # convert to milliseconds ​

df.show()

# COMMAND ----------

# One day in milliseconds​
ONE_DAY_MS=86400000​
from pyspark.sql.window import Window​
window_spec = Window.partitionBy("group") \​
.orderBy("ts_long") \​
.rangeBetween(-ONE_DAY_MS, ONE_DAY_MS)​


df.withColumn("range_sum", sum("value").over(window_spec)).select("group", "ts", "value","range_sum").show()

# COMMAND ----------

# One day in milliseconds
ONE_DAY_MS = 86400000

from pyspark.sql.window import Window
from pyspark.sql.functions import sum

# Define the window specification
window_spec = Window.partitionBy("group") \
    .orderBy("ts_long") \
    .rangeBetween(-ONE_DAY_MS, ONE_DAY_MS)

# Apply the window function
df.withColumn("range_sum", sum("value").over(window_spec)) \
  .select("group", "ts", "value", "range_sum") \
  .show()


# COMMAND ----------

data = [("A", 1), ("A", 2), ("A", 3), ("A", 4), ("B", 1), ("B", 3), ("B", 5)]

df = spark.createDataFrame(data, ["group", "value"])
df.show()


# COMMAND ----------

window_spec = Window.partitionBy("group").orderBy("value").rangeBetween(-1, 1)

df.withColumn("range_sum", sum("value").over(window_spec)).show()

# COMMAND ----------

from pyspark.sql.functions import first, last
from pyspark.sql.window import Window

# Sample DataFrame
data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 300),
    ("B", "2024-01-01", 150),
    ("B", "2024-01-02", 250),
]

df = spark.createDataFrame(data, ["group", "date", "value"])

# Convert date column to proper DateType (optional but good practice)
from pyspark.sql.functions import to_date
df = df.withColumn("date", to_date("date"))


# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.partitionBy("group").orderBy("date")


# COMMAND ----------

from pyspark.sql.functions import first, last

df_with_first_last = df.withColumn("first_value", first("value").over(window_spec)) \
                       .withColumn("last_value", last("value").over(window_spec))

df_with_first_last.show()


# COMMAND ----------

from pyspark.sql.functions import first, last
from pyspark.sql.window import Window

# Sample Data
data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 300),
    ("B", "2024-01-01", 150),
    ("B", "2024-01-02", 250),
]

df = spark.createDataFrame(data, ["group", "date", "value"])

# Convert date column to DateType
from pyspark.sql.functions import to_date
df = df.withColumn("date", to_date("date"))

# Define window with full partition
window_spec = Window.partitionBy("group").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Apply first() and last() over full partition
from pyspark.sql.functions import first, last

df_with_first_last = df.withColumn("first_value", first("value").over(window_spec)) \
                       .withColumn("last_value", last("value").over(window_spec))

df_with_first_last.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, avg, max, min, to_date

# Create DataFrame
data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 300),
    ("B", "2024-01-01", 150),
    ("B", "2024-01-02", 250),
]

df = spark.createDataFrame(data, ["group", "date", "value"])
df = df.withColumn("date", to_date("date"))

# Define window
window_spec = Window.partitionBy("group").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Apply window functions
from pyspark.sql.functions import sum, avg, max, min

df_with_aggregates = df.withColumn("running_sum", sum("value").over(window_spec)) \
    .withColumn("running_avg", avg("value").over(window_spec)) \
    .withColumn("running_max", max("value").over(window_spec)) \
    .withColumn("running_min", min("value").over(window_spec))

df_with_aggregates.show()


# COMMAND ----------

# DBTITLE 1,ntile,cust_dist,percent_rank
from pyspark.sql.functions import ntile, cume_dist, percent_rank
from pyspark.sql.window import Window

# Sample DataFrame
data = [
    ("Alice", 95),
    ("Bob", 85),
    ("Cathy", 80),
    ("David", 70),
    ("Emma", 60)
]

df = spark.createDataFrame(data, ["name", "score"])

# Define window specification (sorted by score)
window_spec = Window.orderBy("score")

# Apply ntile, cume_dist, and percent_rank
df_with_metrics = df.withColumn("tile_3", ntile(3).over(window_spec)) \
                    .withColumn("cume_dist", cume_dist().over(window_spec)) \
                    .withColumn("percent_rank", percent_rank().over(window_spec))

# Show result
df_with_metrics.show()
