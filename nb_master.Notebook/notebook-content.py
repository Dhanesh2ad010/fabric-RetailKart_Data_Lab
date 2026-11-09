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
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
for c in ["Chennai", "Bengaluru", "Hyderabad"]:
    n = notebookutils.notebook.run("nb_city_sales", 120, {"city": c})
    print(c, "->", n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
