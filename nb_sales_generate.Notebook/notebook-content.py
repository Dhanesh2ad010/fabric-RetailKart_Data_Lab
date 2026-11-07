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
# META     },
# META     "environment": {
# META       "environmentId": "6b715026-17fa-af85-40ca-83e75f2b854e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
print(spark.version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from faker import Faker
import random

fake = Faker("en_IN")#vdgdgd
rows = [
    (i, fake.name(), random.choice(["Chennai","Bengaluru","Hyderabad"]),
     round(random.uniform(100, 5000), 2), fake.date_between("-30d", "today"))
    for i in range(1, 1001)
]
df = spark.createDataFrame(rows, ["order_id","customer","city","amount","order_date"])
df.write.mode("overwrite").format("delta").saveAsTable("sales_orders")
display(spark.sql("SELECT city, COUNT(*) orders, ROUND(SUM(amount),2) revenue FROM sales_orders GROUP BY city"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = "abfss://ebb37344-5499-4bce-91db-afa9ff20741e@onelake.dfs.fabric.microsoft.com/325f957c-64fb-4c53-b231-fa31b084f050/Tables/dbo/sales_orders"
df2 = spark.read.format("delta").load(path)
print(df2.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("checking git version control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("checking")#fdgfdeee

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
