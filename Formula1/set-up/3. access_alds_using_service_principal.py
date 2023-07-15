# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using Service Principal
# MAGIC 1. Register the Azure AD Application/Service Principal
# MAGIC 2. Generate secret/password for the application
# MAGIC 3. Set Spark Config with applicant id, directory/tenant id and secret
# MAGIC 4. Assign role storage blob container to the data lake.

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

# 3. set spark config 
# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.dlformula1jk.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlformula1jk.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlformula1jk.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dlformula1jk.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlformula1jk.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformula1jk.dfs.core.windows.net"))

# COMMAND ----------

# read the circuits file with spark dataframe api
display(spark.read.csv("abfss://demo@dlformula1jk.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


