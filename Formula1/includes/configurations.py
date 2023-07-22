# Databricks notebook source

# Create Reusable Variables to use with %run throughout the project with other notebooks

# COMMAND ----------

# create vars for mounted foler pathes in ADLS, with these created, we can use them in the ingestion notebooks where we set a path and execute them by using %run "../includes/configurations"
# enter the varibale as an f string:  f"{stage_folder_path}/cicuits.csv"

# read in files from stage area
stage_folder_path = '/mnt/dlformula1jk/stage'

# write transformed data to clean
clean_folder_path = '/mnt/dlformula1jk/clean'

# write transformed/joined tables for presentation layer
final_folder_path = '/mnt/dlformula1jk/final'

# COMMAND ----------

# if using the abs protocol instead of mounts:
# stage_folder_path = 'abfss:///dlformula1jk@stage.dfs.core.windows.net'
# clean_folder_path = 'abfss:///dlformula1jk@clean.dfs.core.windows.net'
# final_folder_path = 'abfss:///dlformula1jk@final.dfs.core.windows.net'

# COMMAND ----------


