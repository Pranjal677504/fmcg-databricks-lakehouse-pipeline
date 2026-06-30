# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "/Workspace/consolidated pipeline/setup/utilities"

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg", "Catalog")
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("data_source", "customers", "Data source")
data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)


# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
base_path = f's3://sports--project/{data_source}/*.csv'
print(base_path)


# COMMAND ----------

df = spark.read.format("csv").load(base_path)
display(df)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*","_metadata.file_name", "_metadata.file_size")
)
display(df.limit(10))

# COMMAND ----------

#print check data type

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")
    

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_duplicate = df_bronze.groupBy("customer_id").count().filter(F.col("count" )> 1)
display(df_duplicate)

# COMMAND ----------

print('Rows before duplicates removed: ', df_bronze.count())

df_silver = df_bronze.dropDuplicates(["customer_id"])

print('Rows after duplicates removed: ', df_silver.count())



# COMMAND ----------

#check namers having space in starting

display(
    df_silver.filter(F.col("customer_name").startswith(" "))
)

# COMMAND ----------

#removing space in starting of name

df_silver = df_silver.withColumn("customer_name", F.trim(F.col("customer_name")))
#check null values

display(
    df_silver.filter(F.col("customer_name").isNull())
)    

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)

# COMMAND ----------

from pyspark.sql.functions import ltrim

df = df.withColumn("customer_name", ltrim("customer_name"))

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)

# COMMAND ----------

from pyspark.sql.functions import trim

df_bronze = df.withColumn("customer_name", trim("customer_name")) \
       .withColumn("city", trim("city"))

# COMMAND ----------

df_bronze.show(10)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# 1. Correctly read from and write to df_bronze
# 2. Use regex to strip out any flavor of leading whitespace
df_bronze = df_bronze.withColumn("customer_name", regexp_replace("customer_name", r"^[ \t\u00a0]+", "")) \
                     .withColumn("city", regexp_replace("city", r"^[ \t\u00a0]+", ""))

# COMMAND ----------

df_bronze.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.regexp_replace(F.col("customer_name"), r"(?U)^\s+|\s+$", "")
).withColumn(
    "city",
    F.regexp_replace(F.col("city"), r"(?U)^\s+|\s+$", "")
)

# COMMAND ----------

# Check if ANY leading/trailing whitespace remains
from pyspark.sql.functions import length, ltrim, rtrim

df_silver.filter(
    length(F.col("customer_name")) != length(F.trim(F.col("customer_name")))
).show()


import pyspark.sql.functions as F
df_silver.filter(F.col("customer_name").rlike(r"^\s")) \
         .select("customer_name").limit(5).show(truncate=False)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# names correction

from pyspark.sql.functions import when, col

city_mapping={
    'Bengaluruu': 'Bengaluru',
    'Bengalore' : 'Bengaluru',

    'Hyderabadd' : 'Hyderabad',
    'Hyderbad' : 'Hyderabad',

    'NewDelhi' : 'New Delhi',
    'NewDheli' : 'New Delhi',
    'NewDelhee' : 'New Delhi'
}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver =(
    df_silver
    .replace(city_mapping, subset=['city'])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)           

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# title problem

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap("customer_name"))
)

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane ']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

#city correction

customer_city_fix = {
    #sprintx nutrition
    789403: "New Delhi",

    #Zenathlete Foods
    789420: "Bengaluru",

    #Primefuel Nutrition
    789521: "Hyderabad",

    #Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k,v) for k,v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)


# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left"  )
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")  #replace null city with fixed city
    )
    .drop("fixed_city")
)

display(df_silver)



# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane ']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    
    #Build final customer column : "Customername - city"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )

    #static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))


)
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")
        



# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Processing
# MAGIC
# MAGIC

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

#take require columns only

df_gold= df_silver.select("customer_id", "customer_name", "city", "read_timestamp", "customer", "market", "platform", "channel")





# COMMAND ----------

df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sp_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sp_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"

)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

spark.sql("DESCRIBE DETAIL fmcg.gold.dim_customers").show(truncate=False)