# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "/Workspace/consolidated pipeline/setup/utilities"

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
                                  
base_path = f's3://sports--project/{data_source}'
landing_path = f"{base_path}/landing"
processed_path = f"{base_path}/processed"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ",processed_path)

#define tables

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp",F.
current_timestamp()).select("*","_metadata.file_name","_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("append")\
    .saveAsTable(bronze_table)

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
files

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------


df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.show(2)

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

# total null values in order_qty
df_orders.select(
    count(when(col("order_qty").isNull(), 1)).alias("null_count"),
    count("*").alias("total_rows")
).show()

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# Flag nulls and fill with 0
df_orders_silver = df_orders.withColumn(
    "order_qty",
    when(col("order_qty").isNull(), lit(0)).otherwise(col("order_qty"))
).withColumn(
    "has_null_qty",
    when(col("order_qty") == 0, lit(True)).otherwise(lit(False))
)

# Verify
df_orders_silver.groupBy("has_null_qty").count().show()

# COMMAND ----------

silver_table = "fmcg.silver.orders"

df_orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(silver_table)

print(f"  {silver_table}")

# COMMAND ----------

# 1. Keep only rows where order quantity is present
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

#2. Clean customer_id -> keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
      .otherwise("999999")
      .cast("string")
)      

# COMMAND ----------

# 3. Remove weekday name from date text
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("order_placement_date"), "MMMM dd, yyyy")
    )
)

# COMMAND ----------

import pyspark.sql.functions as F

# Load directly from Bronze — no S3, no file movement!
df_orders = spark.read.table("fmcg.bronze.orders")

print("Rows:", df_orders.count())
df_orders.show(2)

# COMMAND ----------

# Step 1 — filter nulls
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# Step 2 — fix invalid customer_id
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
    .otherwise("999999")
    .cast("string")
)

# Step 3 — remove weekday from date
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# Step 4 — parse date formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("order_placement_date"), "MMMM dd, yyyy")
    )
)

# Step 5 — drop duplicates
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# Step 6 — product_id to string
df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

df_orders.show(5)

# COMMAND ----------

display(df_orders.limit(20))

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")

display(df_products.limit(5))


# COMMAND ----------

df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])

display(df_joined.limit(10))


# COMMAND ----------

catalog = "fmcg"
silver_schema = "silver"
data_source = "orders"

silver_table = f"{catalog}.{silver_schema}.{data_source}"
print(silver_table)
# fmcg.silver.orders

# COMMAND ----------

from pyspark.sql.functions import lit

# Add missing column to match silver table schema
df_joined = df_joined.withColumn("has_null_qty", lit(False))

print(df_joined.columns)  # verify column exists

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce

# Force parse the date column properly
df_joined = df_joined.withColumn(
    "order_placement_date",
    coalesce(
        to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        to_date(F.col("order_placement_date"), "yyyy-MM-dd"),
        to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        to_date(F.col("order_placement_date"), "MMMM dd, yyyy")
    )
)

# Verify
df_joined.select("order_placement_date").show(5)
# Should show dates, not strings

# COMMAND ----------

from delta.tables import DeltaTable

# Drop stale table 
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

df_joined.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(silver_table)

print(" Silver table written cleanly!")
spark.table(silver_table).printSchema()
spark.table(silver_table).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

df_gold.show(2)

# COMMAND ----------

from delta.tables import DeltaTable

# Redefine since kernel restarted
catalog = "fmcg"
gold_schema = "gold"
data_source = "orders"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

if not spark.catalog.tableExists(gold_table):
    print("Creating new Gold table...")
    df_gold.write.format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    merge_condition = """
        source.date = gold.date
        AND source.order_id = gold.order_id
        AND source.product_code = gold.product_code
        AND source.customer_code = gold.customer_code
    """
    gold_delta.alias("gold").merge(
        df_gold.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with Parent Company

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

#change all date to first day of month

df_monthly = (
    df_child
    .withColumn("month_start", F.trunc("date","MM"))

    .groupBy("month_start", "product_code", "customer_code") 
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
        )
    
)


display(df_monthly.limit(10))

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

from delta.tables import DeltaTable

# Rename month_start → date to match parent gold schema
df_monthly_renamed = df_monthly.withColumnRenamed("month_start", "date")

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

merge_condition = """
    parent_gold.date = child_gold.date
    AND parent_gold.product_code = child_gold.product_code
    AND parent_gold.customer_code = child_gold.customer_code
"""

gold_parent_delta.alias("parent_gold").merge(
    df_monthly_renamed.alias("child_gold"),
    merge_condition
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



# COMMAND ----------

