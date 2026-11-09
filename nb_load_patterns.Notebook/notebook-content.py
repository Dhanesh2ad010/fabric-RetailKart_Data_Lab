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
from pyspark.sql import functions as F

src = (spark.table("sales_orders")
       .withColumn("status", F.lit("NEW"))
       .withColumn("modified_at", F.to_timestamp("order_date")))
src.write.mode("overwrite").format("delta").saveAsTable("src_orders")
print(spark.table("src_orders").count())   # 1000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("src_orders").write.mode("overwrite").format("delta").saveAsTable("fact_orders")

(spark.table("fact_orders")
      .agg(F.max("modified_at").alias("last_watermark"))
      .write.mode("overwrite").format("delta").saveAsTable("etl_watermark"))

display(spark.table("etl_watermark"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

now = F.current_timestamp()

updated = (spark.table("src_orders").limit(20)
           .withColumn("amount", F.col("amount") + 100)
           .withColumn("status", F.lit("UPDATED"))
           .withColumn("modified_at", now))

new = (spark.range(1001, 1051).toDF("order_id")
       .withColumn("customer", F.concat(F.lit("Customer "), F.col("order_id")))
       .withColumn("city", F.when(F.col("order_id") % 3 == 0, "Chennai")
                            .when(F.col("order_id") % 3 == 1, "Bengaluru")
                            .otherwise("Hyderabad"))
       .withColumn("amount", (F.rand() * 4900 + 100).cast("double"))
       .withColumn("order_date", F.current_date())
       .withColumn("status", F.lit("NEW"))
       .withColumn("modified_at", now))

changes = updated.unionByName(new)

(DeltaTable.forName(spark, "src_orders").alias("t")
   .merge(changes.alias("s"), "t.order_id = s.order_id")
   .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_wm = spark.table("etl_watermark").collect()[0]["last_watermark"]
delta_rows = spark.table("src_orders").filter(F.col("modified_at") > F.lit(last_wm))
print("rows to load:", delta_rows.count())          # expect 70

(DeltaTable.forName(spark, "fact_orders").alias("t")
   .merge(delta_rows.alias("s"), "t.order_id = s.order_id")
   .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

(spark.table("fact_orders")
      .agg(F.max("modified_at").alias("last_watermark"))
      .write.mode("overwrite").format("delta").saveAsTable("etl_watermark"))

print("target rows:", spark.table("fact_orders").count())   # expect 1050
print(spark.table("fact_orders").filter("status = 'UPDATED'").count())  # expect 20

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

orders = spark.table("fact_orders")

dim_city = (orders.select("city").distinct()
            .withColumn("city_key", F.row_number().over(Window.orderBy("city")))
            .select("city_key", "city"))
dim_city.write.mode("overwrite").format("delta").saveAsTable("gold_dim_city")

dim_date = (orders.select("order_date").distinct()
            .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("order_date"))
            .withColumn("month", F.month("order_date"))
            .withColumn("month_name", F.date_format("order_date", "MMMM"))
            .withColumn("day_of_week", F.date_format("order_date", "EEEE"))
            .select("date_key", "order_date", "year", "month", "month_name", "day_of_week"))
dim_date.write.mode("overwrite").format("delta").saveAsTable("gold_dim_date")

fact = (orders.join(dim_city, "city")
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .select("order_id", "city_key", "date_key", "customer", "amount", "status"))
fact.write.mode("overwrite").format("delta").saveAsTable("gold_fact_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
  SELECT c.city, d.month_name, COUNT(*) AS orders, ROUND(SUM(f.amount),2) AS revenue
  FROM gold_fact_orders f
  JOIN gold_dim_city c ON f.city_key = c.city_key
  JOIN gold_dim_date d ON f.date_key = d.date_key
  GROUP BY c.city, d.month_name, d.month
  ORDER BY d.month, c.city
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

stream = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
          .withColumn("order_id", F.col("value"))
          .withColumn("city", F.element_at(
              F.array(F.lit("Chennai"), F.lit("Bengaluru"), F.lit("Hyderabad")),
              (F.col("value") % 3 + 1).cast("int")))
          .withColumn("amount", F.round(F.rand() * 5000 + 100, 2))
          .select("order_id", "city", "amount", "timestamp"))

q = (stream.writeStream.format("delta")
     .option("checkpointLocation", "Files/checkpoints/live_orders")
     .outputMode("append")
     .toTable("live_orders"))

time.sleep(30)
print(spark.table("live_orders").count())   # roughly 150
q.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("live_orders").limit(5))
display(spark.sql("SELECT city, COUNT(*) FROM live_orders GROUP BY city"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
