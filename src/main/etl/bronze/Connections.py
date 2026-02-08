from datetime import datetime,timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType
class Connections:
    audit_table = 'adx_etl.ETL_JOB_AUDIT'
    def __init__(self,read_cred_obj,params,spark,dbutils):
        # JDBC connection properties
        print("BEGIN: ***** Setting Connection *****")
        self.params = params
        self.spark = spark
        self.jdbc_hostname = self.params.env + "adxanalyticssqlserver.database.windows.net"
        self.jdbc_port = 1433
        self.database_name = self.params.database_name
        self.table_name = self.params.source_table
        self.username = read_cred_obj.sql_server_user
        self.password = read_cred_obj.sql_server_pass
        self.sp_client_id = read_cred_obj.sp_client_id
        self.sp_client_secret = read_cred_obj.sp_client_secret
        self.sp_directoryid = read_cred_obj.sp_directoryid
        

        print("Buidling jdbc url Connections")
        # JDBC URL
        self.jdbc_url = f"jdbc:sqlserver://{self.jdbc_hostname}:{self.jdbc_port};databaseName={self.database_name}"

        # Connection properties dictionary
        self.connection_properties = {
            "user": self.username,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        #Setting Spark Config
        self.storage_account = 'devadxstorageaccountgen2'
        self.container_name = 'adx'
        self.project_name = 'analytics'
        self.layer = 'bronze'

        self.spark.conf.set(f"fs.azure.account.auth.type.{self.storage_account}.dfs.core.windows.net", "OAuth")
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{self.storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{self.storage_account}.dfs.core.windows.net", self.sp_client_id)   # Service Principal ID
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{self.storage_account}.dfs.core.windows.net", self.sp_client_secret)  # Service Principal Secret
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{self.storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{self.sp_directoryid}/oauth2/token") # Directory ID

    def get_source_df(self,source_table=None):
        if source_table:
            read_table = source_table
        else:
            read_table = self.table_name
        print(f"BEGIN: *****Reading Table {read_table}*****")
        df = self.spark.read.jdbc(url=self.jdbc_url, table=read_table, properties=self.connection_properties)
        print(f"COMPLETED: *****Reading Table {read_table}*****")
        return df
    
    def write_to_adls_delta(self,transformed_df,params,dbutils):
        source_table = params.source_table
        schema,table_name = source_table.split('.')
        #ADLS Gen2 Details  
        #target_path = f'abfss://{container_name}@{self.storage_account}.dfs.core.windows.net/'
        print("BEGIN: Writing into DELTA Table")
        if params.batch_load == 'batch':
            as_of_dt = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        elif params.batch_load == 'microbatch':
            as_of_dt = (datetime.now().date()).strftime("%Y-%m-%d")
        #Adding as_of_dt
        transformed_df = transformed_df.withColumn("as_of_dt",lit(as_of_dt))
        #Writing into Delta Table with partition by as_of_dt
        transformed_df.write.partitionBy("as_of_dt").format("delta").mode("overwrite").save(
        f"abfss://{self.container_name}@{self.storage_account}.dfs.core.windows.net/{self.project_name}/{self.layer}/{schema}/{table_name}"
        )
        print("COMPLETED: Writing into DELTA Table")
    
    def write_to_audit_delta(self,app_name,source_table,batch_load,run_status,error_message):
        print("BEGIN: Writing into AUDIT Table")
        #source_table = self.params.source_table
        schema,table_name = self.audit_table.split('.')
        if batch_load == 'batch':
            as_of_dt = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        elif batch_load == 'microbatch':
            as_of_dt = (datetime.now().date()).strftime("%Y-%m-%d")  
        
        audit_path = f"abfss://{self.container_name}@{self.storage_account}.dfs.core.windows.net/{self.project_name}/{schema}/{table_name}/"
        #ADLS Gen2 Details 
        print(f"Audit Table Path: {audit_path}")
        # Create audit dataframe
            # ✅ Explicit schema (fixes your error)
        audit_schema = StructType([
            StructField("app_name", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("batch_load", StringType(), True),
            StructField("run_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("load_time", TimestampType(), True)
        ])

        audit_df = self.spark.createDataFrame(
            [(app_name,source_table,batch_load, run_status, error_message, datetime.now())],schema=audit_schema
        )

        audit_df.show(10)
        print(f"Completed : Audit Table Path: {audit_path}")
        print("Writing into Audit Table")
        # Append to audit delta table
        #audit_df.write.format("delta").mode("append").partitionBy("as_of_dt").insertInto(audit_path)
        audit_df.write.format("delta") \
        .mode("append") \
        .save(audit_path)
        print("COMPLETED: Writing into AUDIT Table")

    def ensure_audit_table(self):
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS adx_etl")
        



   