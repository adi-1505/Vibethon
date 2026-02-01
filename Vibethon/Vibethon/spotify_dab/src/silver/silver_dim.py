# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # DimUser

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation","abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimUser/checkpoint")\
                          .load("abfss://bronze@sptfyazurestracc.dfs.core.windows.net/DimUser")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

display(df_user)

# COMMAND ----------

import os
import sys
new_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(new_path)
from utils.transformation import reuse

# COMMAND ----------

del_col = reuse()
df_user = del_col.dropcol(df_user,['_rescued_data'])
display(df_user)

# COMMAND ----------

df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

df_user.writeStream.format('delta')\
                   .outputMode('append')\
                   .option("checkpointLocation",'abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimUser/checkpoint')\
                   .trigger(once=True)\
                   .option("path","abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimUser/data")\
                   .toTable("apotify_cata.silver.DimUser")
     

# COMMAND ----------

# MAGIC %md
# MAGIC # DimArtist

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format","parquet")\
                        .option("cloudFiles.schemaLocation","abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimArtist/checkpoint")\
                        .option("schemaEvolutionMode", "addNewColumn")\
                        .load("abfss://bronze@sptfyazurestracc.dfs.core.windows.net/DimArtist")

# COMMAND ----------

del_col = reuse()
df_art = del_col.dropcol(df_art,['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])

# COMMAND ----------

display(df_art)

# COMMAND ----------

df_art.writeStream.format('delta')\
                   .outputMode('append')\
                   .option('checkpointLocation','abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimArtist/checkpoint')\
                   .trigger(once=True)\
                   .option('path','abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimArtist/data')\
                   .toTable("apotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format","parquet")\
                        .option("cloudFiles.schemaLocation","abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimTrack/checkpoint")\
                        .option("schemaEvolutionMode", "addNewColumn")\
                        .load("abfss://bronze@sptfyazurestracc.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_track = df_track.withColumn("Duration_Flag", when(col('duration_sec')<150,"Low")\
                                               .when(col('duration_sec')<300,'Medium')\
                                                 .otherwise('High'))
display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))

# COMMAND ----------

df_track = del_col.dropcol(df_track,['_rescued_data'])


# COMMAND ----------

display(df_track)

# COMMAND ----------

df_track.writeStream.format('delta')\
                    .outputMode('append')\
                    .option("checkpointLocation","abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimTrack/checkpoint")\
                    .trigger(once=True)\
                    .option('path','abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimTrack/data')\
                    .toTable("apotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC # DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                         .option("cloudFiles.schemaLocation","abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimDate/checkpoint")\
                         .option("schemaEvolutionMode", "addNewColumn")\
                          .load("abfss://bronze@sptfyazurestracc.dfs.core.windows.net/DimDate")

# COMMAND ----------

display(df_date)

# COMMAND ----------

df_date = reuse().dropcol(df_date, ['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format('delta')\
                   .outputMode('append')\
                   .option('checkpointLocation','abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimDate/checkpoint')\
                   .trigger(once=True)\
                   .option('path','abfss://silver@sptfyazurestracc.dfs.core.windows.net/DimDate/data')\
                   .toTable("apotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC # FactStream

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
                          .option("cloudFiles.format","parquet")\
                          .option("cloudFiles.schemaLocation","abfss://silver@sptfyazurestracc.dfs.core.windows.net/FactStream/checkpoint")\
                          .option("schemaEvolutionMode","addNewColumns")\
                          .load("abfss://bronze@sptfyazurestracc.dfs.core.windows.net/FactStream")
                            

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reuse().dropcol(df_fact,['_rescued_data'])

df_fact.writeStream.format('delta')\
                   .outputMode('append')\
                    .option('checkpointLocation','abfss://silver@sptfyazurestracc.dfs.core.windows.net/FactStream/checkpoint')\
                   .trigger(once=True)\
                   .option('path','abfss://silver@sptfyazurestracc.dfs.core.windows.net/FactStream/data')\
                   .toTable("apotify_cata.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apotify_cata.gold.dimtrack
# MAGIC where track_id in (46,5)

# COMMAND ----------

