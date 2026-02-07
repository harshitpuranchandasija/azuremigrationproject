from pyspark.sql import SparkSession
class Connections:
    def __init__(self,read_cred_obj,params,spark):
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

        print("Buidling jdbc url Connections")
        # JDBC URL
        self.jdbc_url = f"jdbc:sqlserver://{self.jdbc_hostname}:{self.jdbc_port};databaseName={self.database_name}"

        # Connection properties dictionary
        self.connection_properties = {
            "user": self.username,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

    def get_source_df(self,source_table=None):
        if source_table:
            read_table = source_table
        else:
            read_table = self.table_name
        print(f"BEGIN: *****Reading Table {read_table}*****")
        df = self.spark.read.jdbc(url=self.jdbc_url, table=read_table, properties=self.connection_properties)
        print(f"COMPLETED: *****Reading Table {read_table}*****")
        return df

   