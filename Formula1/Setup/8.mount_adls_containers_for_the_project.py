# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Mount Azure Data Lake Containers for the Project
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 2. Get Spark Config with App/Cleint Id, Directory/Tenant_id and secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls (storage_account_name, container_name):
    #Get secrets from vault
    print('Retrieving secrets from the vault...', end='')
    client_id = dbutils.secrets.get('course-scope','dl-service-principal-client-id')
    client_secret = dbutils.secrets.get('course-scope','dl-service-principal-client-secret')
    tenant_id = dbutils.secrets.get('course-scope','dl-service-principal-tenant-id')
    print('done!')
    
    #Set spark configuration
    print('Creating Spark configuration...',end='')
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'}
    print('done!')
     
    #Check if the mount already exists and unmounts it if found
    if any(mount.mountPoint == f'/mnt/{storage_account_name}/{container_name}' for mount in dbutils.fs.mounts()):
        print(f'{storage_account_name}/{container_name} already mounted, unmounting...',end='')
        dbutils.fs.unmount(f'/mnt/{storage_account_name}/{container_name}')
        print('done!')

    #Mount the storage account container
    print(f'Mounting {storage_account_name}/{container_name}...',end='')
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    print('done!')
    print('Finished, displaying all mounts:')
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('dlcoursestorage', 'demo')
mount_adls('dlcoursestorage', 'raw')
mount_adls('dlcoursestorage', 'presentation')
mount_adls('dlcoursestorage', 'processed')
