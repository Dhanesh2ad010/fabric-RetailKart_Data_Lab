# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "325f957c-64fb-4c53-b231-fa31b084f050",
# META       "default_lakehouse_name": "lh_retailkart",
# META       "default_lakehouse_workspace_id": "ebb37344-5499-4bce-91db-afa9ff20741e",
# META       "known_lakehouses": [
# META         {
# META           "id": "325f957c-64fb-4c53-b231-fa31b084f050"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd, random, os
random.seed(7)

rows = []
for i in range(1, 201):
    rows.append({
        "order_id": i,
        "customer_id": random.randint(1, 30),
        "product_id": random.randint(1, 5),
        "city": None if i % 25 == 0 else random.choice(["Chennai", "Bengaluru", "Hyderabad"]),
        "amount": None if i % 40 == 0 else round(random.uniform(100, 5000), 2),
        "order_date": str(pd.Timestamp("2026-11-01") + pd.Timedelta(days=random.randint(0, 7)))[:10],
        "updated_at": "2026-11-08 10:00:00",
    })
df = pd.DataFrame(rows)

dups = df.head(10).copy()                      # orders 1-10 arrive twice
dups["updated_at"] = "2026-11-08 11:00:00"     # the later copy is the newer one
df = pd.concat([df, dups])

os.makedirs("/lakehouse/default/Files/landing", exist_ok=True)
df.to_csv("/lakehouse/default/Files/landing/orders_raw.csv", index=False)
print(len(df))   # 210

products = spark.createDataFrame(
    [(1, "Phone", "Electronics"), (2, "Laptop", "Electronics"), (3, "Shirt", "Apparel"),
     (4, "Shoes", "Apparel"), (5, "Mixer", "Home")],
    ["product_id", "product_name", "category"])
products.write.mode("overwrite").format("delta").saveAsTable("dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

r = spark.table("raw_orders")
print(r.count(), r.filter("city IS NULL").count(), r.filter("amount IS NULL").count())
# expect 210, 8, 5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("df_orders_clean").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

raw = spark.table("raw_orders")
print("raw:", raw.count(), "distinct orders:", raw.select("order_id").distinct().count())  # 210, 200

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
dedup = raw.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
print("after dedupe:", dedup.count())   # 200

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clean = dedup.fillna({"city": "Unknown"}).filter(F.col("amount").isNotNull())
print("clean:", clean.count())   # 195

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prod = spark.table("dim_product")
wide = clean.join(prod, "product_id", "left")
wide.write.mode("overwrite").format("delta").saveAsTable("silver_orders")
display(wide.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(wide.groupBy("city", "category")
            .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"))
            .orderBy("city", "category"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT category, COUNT(*) AS orders, ROUND(SUM(amount),2) AS revenue
# MAGIC FROM silver_orders GROUP BY category ORDER BY revenue DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

t = spark.table("silver_orders")
ts_type = t.schema["updated_at"].dataType
print("before:", t.filter("order_id = 7").select("amount", "updated_at").collect())

late = (t.filter("order_id = 7")
         .withColumn("amount", F.col("amount") + 500)
         .withColumn("updated_at", F.lit("2026-11-09 09:00:00").cast(ts_type)))

(DeltaTable.forName(spark, "silver_orders").alias("t")
   .merge(late.alias("s"), "t.order_id = s.order_id")
   .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
   .whenNotMatchedInsertAll().execute())

print("after:", spark.table("silver_orders").filter("order_id = 7").select("amount", "updated_at").collect())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stale = (spark.table("silver_orders").filter("order_id = 7")
          .withColumn("amount", F.lit(1.0))
          .withColumn("updated_at", F.lit("2026-11-01 00:00:00").cast(ts_type)))

(DeltaTable.forName(spark, "silver_orders").alias("t")
   .merge(stale.alias("s"), "t.order_id = s.order_id")
   .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
   .whenNotMatchedInsertAll().execute())

print(spark.table("silver_orders").filter("order_id = 7").select("amount").collect())  # unchanged

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

x = (spark.table("silver_orders").select("order_id", "amount").limit(3)
       .withColumn("product_id", F.lit(99))
       .join(prod, "product_id", "left")
       .fillna({"product_name": "Unknown", "category": "Unknown"}))
display(x)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
