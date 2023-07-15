# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenent_id and client_secret from key vault.
# MAGIC 2. Call file system untility mount to mount the storage.
# MAGIC 3. Explore other file system utilities related to mount.

# COMMAND ----------

# 1., 2. create variables to store client id and tenant id
client_id = dbutils.secrets.get(scope='formula1-scope', key='f1-client-is')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='f1-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='f1-client-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC Replace
# MAGIC
# MAGIC "secret-scope" with the Databricks secret scope name. \
# MAGIC "service-credential-key" with the name of the key containing the client secret. \
# MAGIC "storage-account" with the name of the Azure storage account. \
# MAGIC "application-id" with the Application (client) ID for the Azure Active Directory application. \
# MAGIC "directory-id" with the Directory (tenant) ID for the Azure Active Directory application.
# MAGIC

# COMMAND ----------

# set spark config - see https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# Mount - Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@dlformula1jk.dfs.core.windows.net/",
  mount_point = "/mnt/dlformula1jk/demo",
  extra_configs = configs)

# COMMAND ----------

# use file system sematics now to list files in the demo container
display(dbutils.fs.ls("/mnt/dlformula1jk/demo"))

# COMMAND ----------

# read the circuits file with spark dataframe api using file system sematics
display(spark.read.csv("/mnt/dlformula1jk/demo/circuits.csv"))

# COMMAND ----------

# See all the mounts in a workspace
display(dbutils.fs.mounts())

# COMMAND ----------

# to unmount - not going to run but just for demonstration purposes
# dbutils.fs.unmount('/mnt/dlformula1jk/demo')

# COMMAND ----------


