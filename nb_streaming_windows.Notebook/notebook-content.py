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
import time
from pyspark.sql import functions as F

src = (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
       .withColumn("city", F.element_at(
           F.array(F.lit("Chennai"), F.lit("Bengaluru"), F.lit("Hyderabad")),
           (F.col("value") % 3 + 1).cast("int")))
       .withColumn("amount", F.round(F.rand() * 5000 + 100, 2)))

tumbling = (src.groupBy(F.window("timestamp", "10 seconds"), "city")
               .agg(F.count("*").alias("orders"),
                    F.round(F.sum("amount"), 2).alias("revenue")))

q1 = (tumbling.writeStream.format("memory").queryName("tumbling_orders")
      .outputMode("complete").start())

time.sleep(35)
display(spark.sql("""
  SELECT window.start AS w_start, window.end AS w_end, city, orders, revenue
  FROM tumbling_orders ORDER BY w_start, city"""))
q1.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

hopping = (src.withWatermark("timestamp", "10 seconds")
              .groupBy(F.window("timestamp", "20 seconds", "10 seconds"), "city")
              .agg(F.count("*").alias("orders"),
                   F.round(F.sum("amount"), 2).alias("revenue")))

q2 = (hopping.writeStream.format("memory").queryName("hopping_orders")
      .outputMode("append").start())

time.sleep(60)
display(spark.sql("""
  SELECT window.start AS w_start, window.end AS w_end, city, orders
  FROM hopping_orders ORDER BY w_start, city"""))
q2.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
