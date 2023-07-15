# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Containers for Project

# COMMAND ----------

# create a function to mount containers with names as arguements
def mount_adls(storage_acct_name, container_name):
    # get keys from vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='f1-client-is')
    tenant_id = dbutils.secrets.get(scope='formula1-scope', key='f1-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='f1-client-secret')

    # set spark config - see https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # mount the container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_acct_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_acct_name}/{container_name}",
        extra_configs = configs)
    
    # show mounts on every run
    display(dbutils.fs.mounts())
  
     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Staging Container

# COMMAND ----------

# pass in the storage account name and the container name
mount_adls('dlformula1jk', 'stage')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Clean Container

# COMMAND ----------

mount_adls('dlformula1jk', 'clean')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Final Container

# COMMAND ----------

mount_adls('dlformula1jk', 'final')

# COMMAND ----------

# to unmount - not going to run but just for demonstration purposes
# dbutils.fs.unmount('/mnt/dlformula1jk/demo')
